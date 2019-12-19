package topics

import (
	"fmt"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/go-redis/redis/v7"
)

type redisTopics struct {
}

func (rt *redisTopics) Subscribe(topic []byte, qos byte, subscriber interface{}) (byte, error) {
	return []byte{}, nil
}
func (rt *redisTopics) Unsubscribe(topic []byte, subscriber interface{}) error {
	return nil
}
func (rt *redisTopics) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	return nil
}
func (rt *redisTopics) Retain(msg *packets.PublishPacket) error {
	return nil
}
func (rt *redisTopics) Retained(topic []byte, msgs *[]*packets.PublishPacket) error {
	return nil
}
func (rt *redisTopics) Close() error {
	return nil
}

func ExampleNewClient() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "123456", // no password set
		DB:       0,        // use default DB

	})

	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
	// Output: PONG <nil>

}

func ExampleClient() {
	err := client.Set("key", "value", 0).Err()
	if err != nil {
		panic(err)

	}

	val, err := client.Get("key").Result()
	if err != nil {
		panic(err)

	}
	fmt.Println("key", val)

	val2, err := client.Get("key2").Result()
	if err == redis.Nil {
		fmt.Println("key2 does not exist")

	} else if err != nil {
		panic(err)

	} else {
		fmt.Println("key2", val2)

	}
	// Output: key value
	// key2 does not exist

}
