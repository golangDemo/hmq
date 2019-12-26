package topics

import (
	"encoding/json"
	"errors"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/logger"
	"go.uber.org/zap"
	"io/ioutil"
	"reflect"
	"time"
	"unsafe"

	"github.com/go-redis/redis/v7"
)

var (
	client *redis.Client
	log    = logger.Get().Named("topics")
	rc     RedisConfig
)

type RedisConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

func init() {
	Register("redis", NewRedisProvider())
	rc, err := LoadConfig("../../../conf/redis.config")
	if err != nil {
		log.Debug("load redis config error")
	}
	InitRedis(rc)
}

func NewRedisProvider() *redisTopics {
	return &redisTopics{}
}

func LoadConfig(filename string) (*RedisConfig, error) {

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		// log.Error("Read config file error: ", zap.Error(err))
		return nil, err
	}
	// log.Info(string(content))

	var config RedisConfig
	err = json.Unmarshal(content, &config)
	if err != nil {
		// log.Error("Unmarshal config file error: ", zap.Error(err))
		return nil, err
	}

	return &config, nil
}

func InitRedis(rc *RedisConfig) {
	client = redis.NewClient(&redis.Options{
		Addr:     rc.Addr,
		Password: rc.Password, // no password set
		DB:       rc.DB,       // use default DB

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
