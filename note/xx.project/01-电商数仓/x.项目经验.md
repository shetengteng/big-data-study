# Hadoop宕机

- 如果MR造成系统宕机
  - 控制Yarn同时运行的任务数
  - 控制每个任务申请的最大内存
  - 调整参数：yarn.scheduler.maximum-allocation-mb
    - 单个任务可申请的最多物理内存量，默认是8192MB

- 如果写入文件过量造成NameNode宕机
  - 调高Kafka的存储大小
  - 控制从Kafka到HDFS的写入速度
  - 高峰期的时候用Kafka进行缓存
  - 高峰期过去数据同步会自动跟上



# Zookeeper

- Zookeeper群起脚本失效，远程ssh失效



## Linux环境变量

- 修改/etc/profile文件
  - 用来设置系统环境参数
    - 如$PATH
  - 这里面的环境变量是对系统内所有用户生效
  - 使用bash命令，需要source  /etc/profile一下

- 修改~/.bashrc文件
  - 针对==某一个特定的用户==，环境变量的设置只对该用户自己有效
  - 使用bash命令，==只要以该用户身份运行命令行就会读取该文件==

- 把/etc/profile里面的环境变量追加到~/.bashrc目录

```bash
[ttshe@hadoop102 ~]$ cat /etc/profile >> ~/.bashrc
[ttshe@hadoop103 ~]$ cat /etc/profile >> ~/.bashrc
[ttshe@hadoop104 ~]$ cat /etc/profile >> ~/.bashrc
```

​        

# 数仓调试

- ZK容易出现问题，要查看ZK的状态
  - myid配置的问题
- Kafka也容易出现问题，查看是否可以正常生产和消费消息
  - brokerid配置问题
- 第一级别flume
  - 开发时，日志不要放在黑洞里面
