# flume，take出小文件怎么处理?

- 可以根据时间30min一次，或者128M一次落盘
- 有三个参数，rollInterval，rollSize，rollCount



# flume分布

- 使用的版本是1.7
- 12台物理机上部署了3台
- 对于日志文件，保留30天
- 32G内存，16线程CPU，磁盘4T



# flume 配置

- flume从日志文件读取数据使用source是tairDir模式，保证了数据不丢失，断点续传的功能，需要配置checkPoint
- 而1.6版本是没有该功能的
- 为了保证数据不丢失，使用的channel类型是file
- sink配置的是KafkaSink
  - 后期优化直接从channel发送给kafka

- 由于flume采用了事务，在channel的两端使用了事务机制，保证了put和take都不会丢失event，数据采集的准确性比较好
- sink发送kafka的不同主题，采用了选择器selector，同时配置了multiplexing
  - 依据event中的header映射
  - 实现是自定义flume拦截器机制



# 关于性能

ganglia 监控flume的性能，查看put和take机制的尝试次数和成功次数，如果put尝试次数大于成功，可以修改flume-env的默认1G配置文件，改为4G











