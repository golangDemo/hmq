package topics

import (
	"errors"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/logger"
	"go.uber.org/zap"
	"reflect"
	"time"
	"unsafe"

	"github.com/go-redis/redis/v7"
)

var (
	client *redis.Client
	log    = logger.Get().Named("topics")
)

func init() {
	Register("redis", NewMemProvider())
	InitRedis()
}

func InitRedis() {
	client = redis.NewClient(&redis.Options{
		Addr:     "192.168.18.136:6379",
		Password: "123456", // no password set
		DB:       0,        // use default DB

	})

	pong, err := client.Ping().Result()
	log.Debug("init redis client:", zap.String("pong", pong), zap.Error(err))
}

type redisTopics struct {
}

func (rt *redisTopics) Subscribe(topic []byte, qos byte, subscriber interface{}) (byte, error) {
	if qos > QosExactlyOnce {
		qos = QosExactlyOnce
	}
	topicKey, err := TopicKey(topic)
	if err != nil {
		return qos, err
	}
	return qos, client.SAdd(topicKey, subscriber).Err()
}
func (rt *redisTopics) Unsubscribe(topic []byte, subscriber interface{}) error {
	topicKey, err := TopicKey(topic)
	if err != nil {
		return err
	}
	return client.SRem(topicKey, subscriber).Err()
}
func (rt *redisTopics) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	topicKey, err := TopicKey(topic)
	if err != nil {
		return err
	}
	results, ok := client.SMembers(topicKey).Result()
	for _, sub := range results {
		*subs = append(*subs, sub)
	}
	return ok
}

func TopicKey(topic interface{}) (string, error) {
	return appendString("topics:", topic)
}
func RetainKey(topic interface{}) (string, error) {
	return appendString("retain:", topic)
}

func appendString(prefix string, key interface{}) (string, error) {
	keyV := reflect.ValueOf(key)
	switch keyV.Kind() {
	case reflect.String:
		return prefix + keyV.String(), nil
	case reflect.Slice:
		byteStr := key.([]byte)
		return prefix + *(*string)(unsafe.Pointer(&byteStr)), nil
	default:
		return "", errors.New("appString only accept string or []byte ")
	}
}

func (rt *redisTopics) Retain(msg *packets.PublishPacket) error {
	retainTopic, err := RetainKey(msg.TopicName)
	if err != nil {
		return err
	}
	log.Debug("retain key", zap.String("key", retainTopic))
	// TODO change msg.String to json data
	return client.Set(retainTopic, msg.String(), 100*time.Second).Err()
}
func (rt *redisTopics) Retained(topic []byte, msgs *[]*packets.PublishPacket) error {
	retainTopic, err := RetainKey(topic)
	if err != nil {
		return err
	}
	log.Debug("retain key", zap.String("key", retainTopic))
	msgVal, err := client.Get(retainTopic).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	// TODO convert msg Val to publish.Packet
	log.Debug("get retained msg", zap.String("msgVal", msgVal))
	pubMsg := packets.PublishPacket{TopicName: string(topic)}
	*msgs = append(*msgs, &pubMsg)
	return nil
}
func (rt *redisTopics) Close() error {
	return nil
}
