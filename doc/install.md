## HMQ启动过程

### 服务列表


#### 依赖服务

##### kafka
```sh
brew install kafka
sudo mkdir -p /usr/local/var/run/zookeeper/data
sudo chmod 777 /usr/local/var/run/zookeeper/data
zkServer start

mkdir -p /usr/local/var/lib/kafka-logs
sudo chmod 777 /usr/local/var/lib/kafka-logs
/usr/local/Cellar/kafka/2.3.1/libexec/bin/kafka-server-start.sh /usr/local/etc/kafka/server.properties

```

#### hmq MQTT主服务
```sh
git clone git@github.com:golangDemo/hmq.git

go build main.go

./main -c conf/hmq.conf
```

#### auth Http mock server
```sh
git clone git@github.com:golangDemo/auth.git

go build main.go

./main
```

#### hmq cluster服务
```sh
git clone git@github.com:golangDemo/auth.git

go build main.go

./main
```
