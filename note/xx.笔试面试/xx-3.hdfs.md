问题：HDFS读写流程，shuffle机制，Hadoop优化。Yarn调度器，yarn任务提交流程，集群的搭建过程

- hadoop作为集群文件的存储系统，注意namenode和resourceManager的HA配置（使用ZK）
- 注意把hdfs的namenode的edits和fsimage文件配置在不同的目录下，不同的目录所在不同的磁盘（性能会有提升）
- 注意namenode和datanode之间的通信，设置合理的心跳时间，有2个参数在hdfs-site.xml中，可以提升他们之间的通信流畅程度
- hdfs的分层存储，多个磁盘，使用不同的存储策略，如一份存储在固态上，其他存储在机械硬盘上等

