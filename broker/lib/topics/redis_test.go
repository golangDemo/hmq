package topics

import (
	"github.com/eclipse/paho.mqtt.golang/packets"
	"testing"
)

func TestExampleClient(t *testing.T) {
	InitRedis()
	t.Logf("LOG: example is ok")
}

func TestTopicKey(t *testing.T) {
	tk, err := TopicKey([]byte("test"))
	if err != nil {
		t.Errorf("topic key caculate error")
	}
	expectedKey := "topics:test"
	if tk != expectedKey {
		t.Errorf("redis topic should be %s, but %s \n", expectedKey, tk)
	}
	t.Logf("LOG: redis key %s \n", tk)
}

func TestRedisTopics_Subscribe(t *testing.T) {
	InitRedis()
	rt := new(redisTopics)
	qos, subOk := rt.Subscribe([]byte("test"), byte(1), "zhangchao")
	if subOk != nil {
		t.Errorf("sub message error %d", subOk)
	}
	t.Logf("LOG: subscribe qos %d\n", qos)
}

func TestRedisTopics_Unsubscribe(t *testing.T) {
	InitRedis()
	rt := new(redisTopics)
	unSubOK := rt.Unsubscribe([]byte("test"), "zhangchao")
	if unSubOK != nil {
		t.Errorf("unsub topic error %d", unSubOK)
	}
	t.Logf("LOG: unsubscribe topic %s", "test")
}

func TestRedisTopics_Subscribers(t *testing.T) {
	InitRedis()
	var subs []interface{}
	var qoss []byte
	rt := new(redisTopics)
	err := rt.Subscribers([]byte("test"), 1, &subs, &qoss)
	if err != nil {
		t.Errorf("get subscribers error %d", err)
	}
	for _, sub := range subs {
		t.Logf("LOG: sub %s\n", sub)
	}

}

func TestRedisTopics_Retain(t *testing.T) {
	InitRedis()
	payload := []byte("test payload")
	msg := packets.PublishPacket{TopicName: "retain/test", Payload: payload}
	rt := new(redisTopics)
	ok := rt.Retain(&msg)
	if ok != nil {
		t.Errorf("retain msg error")
	}
	t.Logf("retained msg %s\n", payload)
}

func TestRedisTopics_Retained(t *testing.T) {
	InitRedis()
	rt := new(redisTopics)
	topicName := []byte("retain/test")
	var msgS []*packets.PublishPacket
	ok := rt.Retained(topicName, &msgS)
	if ok != nil {
		t.Errorf("get retained msg error %d", ok)
	}
	for _, msg := range msgS {
		t.Logf("retained msg %s\n", msg.TopicName)
	}
}
