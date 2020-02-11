# 压测

- 用Kafka官方自带的脚本，对Kafka进行压测
- Kafka压测时，可查看到哪个地方出现了瓶颈（CPU，内存，网络IO）
  - 一般都是网络IO达到瓶颈
  - kafka-consumer-perf-test.sh
  - kafka-producer-perf-test.sh



# Kafka Producer压力测试

- record-size
  - 是一条信息有多大
  - 单位是字节
- num-records
  - 是总共发送多少条信息
- throughput
  - 是每秒多少条信息

```bash
[ttshe@hadoop102 kafka]$ bin/kafka-producer-perf-test.sh  --topic test --record-size 100 --num-records 100000 --throughput 1000 --producer-props bootstrap.servers=hadoop102:9092,hadoop103:9092,hadoop104:9092
```

- 结果

```bash
[2019-10-05 06:58:30,137] WARN Error while fetching metadata with correlation id 3 : {test=LEADER_NOT_AVAILABLE} (org.apache.kafka.clients.NetworkClient)
5002 records sent, 1000.0 records/sec (0.10 MB/sec), 2.3 ms avg latency, 220.0 max latency.
5007 records sent, 1001.2 records/sec (0.10 MB/sec), 0.7 ms avg latency, 9.0 max latency.
...
5002 records sent, 1000.2 records/sec (0.10 MB/sec), 0.6 ms avg latency, 8.0 max latency.
5001 records sent, 1000.2 records/sec (0.10 MB/sec), 0.5 ms avg latency, 6.0 max latency.

100000 records sent, 999.970001 records/sec (0.10 MB/sec), 0.73 ms avg latency, 220.00 ms max latency, 0 ms 50th, 2 ms 95th, 4 ms 99th, 32 ms 99.9th.
```

- 本例中一共写入10w条消息，每秒向Kafka写入了0.10MB的数据，平均是1000条消息/秒，每次写入的平均延迟为0.7ms，最大的延迟为220ms
- 测试与生产环境的数据一致，测试是否符合生产条件



# Kafka Consumer压力测试

- Consumer的测试，如果这四个指标（IO，CPU，内存，网络）都不能改变，考虑增加分区数来提升性能

```bash
[ttshe@hadoop102 kafka]$ bin/kafka-consumer-perf-test.sh --zookeeper hadoop102:2181 --topic test --fetch-size 10000 --messages 10000000 --threads 1
```

- 消费的速度要大于生产的速度，否则会有堆积
- 参数说明：
  - --zookeeper 指定zookeeper的链接信息
  - --topic 指定topic的名称
  - --fetch-size 指定每次fetch的数据的大小
  - --messages 总共要消费的消息个数
  - --threads 线程数个数
- 结果
  - 开始测试时间
  - 测试结束数据
  - 最大吞吐率
    - 9.5367MB/s
  - 平均每秒消费
    - 7.8042MB/s
  - 最大每秒消费
    - 100000条
  - 平均每秒消费
    - 81833.0606条

```bash
start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec
2019-10-05 07:06:44:901, 2019-10-05 07:06:46:123, 9.5367, 7.8042, 100000, 81833.0606
```



# 机器数量计算

- Kafka机器数量=`2*（峰值生产速度*副本数/100）+1`
- 先要预估一天大概产生多少数据，然后用Kafka自带的生产压测
  - 只测试Kafka的写入速度，保证数据不积压
- 计算出峰值生产速度。再根据设定的副本数，就能预估出需要部署Kafka的数量
- 比如采用压力测试测出写入的速度是10M/s一台，峰值的业务数据的速度是50M/s。副本数为2
  - Kafka机器数量=`2*（50*2/100）+ 1`=3台
    - 每天2亿条数据，3台满足

