package topics

import (
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/logger"
	"go.uber.org/zap"
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
	topicKey := byteRedisKey{key: topic}
	return qos, client.SAdd(topicKey.TopicKey(), subscriber).Err()
}
func (rt *redisTopics) Unsubscribe(topic []byte, subscriber interface{}) error {
	topicKey := byteRedisKey{key: topic}
	return client.SRem(topicKey.TopicKey(), subscriber).Err()
}
func (rt *redisTopics) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	topicKey := byteRedisKey{key: topic}
	results, ok := client.SMembers(topicKey.TopicKey()).Result()
	for _, sub := range results {
		*subs = append(*subs, sub)
	}
	return ok
}

type redisKey interface {
	TopicKey(prefix string, key interface{}) string
	RetainKey(prefix string, key interface{}) string
}

type stringRedisKey struct {
	key string
}

func (sr *stringRedisKey) TopicKey() string {
	return keyString("topics:", sr.key)
}
func (sr *stringRedisKey) RetainKey() string {
	return keyString("retain:", sr.key)
}

func keyString(prefix string, key string) string {
	return prefix + key
}

type byteRedisKey struct {
	key []byte
}

func (sr *byteRedisKey) TopicKey() string {
	return keyByte("topics:", sr.key)
}
func (sr *byteRedisKey) RetainKey() string {
	return keyByte("retain:", sr.key)
}

func keyByte(prefix string, key []byte) string {
	return prefix + *(*string)(unsafe.Pointer(&key))
}

func (rt *redisTopics) Retain(msg *packets.PublishPacket) error {
	topicKey := stringRedisKey{key: msg.TopicName}
	retainTopic := topicKey.RetainKey()
	log.Debug("retain key", zap.String("key", retainTopic))
	// TODO change msg.String to json data
	return client.Set(retainTopic, msg.String(), 100*time.Second).Err()
}
func (rt *redisTopics) Retained(topic []byte, msgs *[]*packets.PublishPacket) error {
	topicKey := byteRedisKey{key: topic}
	retainTopic := topicKey.RetainKey()
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
