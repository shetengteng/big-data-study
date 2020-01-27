# 面试题

待完善



- Spark内置模块包括哪些？请分别简述其功能

  - 与内核有关，参考内核调优

  

- Spark的特点有哪些？简述

  - 快，将弹性，分布式说明一下

  

- Spark中核心的进程有哪些？分别说明其主要功能

  - CoarseGrainedExecutorBackEnd 粗粒度执行后台进程
  - Master  Standalone模式下
  - Worker Standalone模式下
  - SparkSubmit
  - Driver 不是，是线程
  - Client 有，但很快消失
  - 内核调优中详细说明

  

- Spark有几种运行模式，如何指定在不同的模式下运行

  - Local
  - Standalone
    - 在spark-submit时，在master上参数填写spark://ip:port
  - Yarn
  - Mesos

- 如何提交一个Spark任务？主要参数有哪些

  - spark-submit
    - --master
    - --class
    - --executor-memory
    - --executor-core

- 画出在Standalone-Client模式下提交任务的流程图

  - 一般用于测试
  - 

- 画出在Yarn-Cluster模式下提交任务的流程图

- 

- 简述你所理解的不同运行模式之间的区别

  - 资源调度和任务调度的关系
  - local
    - 资源调度和任务调度在本机
  - 独立模式
    - spark有自己独立的集群
    - master和worker形成资源调度
    - driver和executor形成任务调度
    - master和driver做交互，worker和executor做交互
  - yarn
    - resourceManager与ApplicationMaster交互
    - executor和nodeManager的container交互

  



- 编写WordCount(读取一个本地文件)，并打包到集群运行,说明需要添加的主要参数
  - 
- RDD的属性
  - 分区，依赖，首选位置
- RDD的特点
  - 
- 如何创建一个RDD，有几种方式，举例说明
  - 内存中创建，通过集合创建
  - 通过外部存储创建
  - 通过转换算子得到RDD
  - new 得到，如读取mysql数据
- 创建一个RDD，使其一个分区的数据转变为一个String
  - 例如(Array("a","b","c","d"),2)=>("ab","cd")

```scala
var rdd = sc.makeRDD(Array("a","b","c","d"),2)
rdd.mapPartitions(items=>{
    var tmp = ""
    for(item <- items){
        tmp += item
        println(tmp)
    }
    Iterator(tmp)
}).collect
```

- map与mapPartitions的区别

  - map每个数据操作一遍
  - mapPartitions每个分区执行一遍

  

- coalesce与repartition两个算子的作用以及区别与联系

  - coalesce默认没有shuffle，可以指定
  - repartition有shuffle过程，等价于coalesce指定shuflle的调用

- 使用zip算子时需要注意的是什么（即哪些情况不能使用）

  - 2个RDD的分区数量要一致
  - 分区内数量要一致，否则报错

  

- reduceByKey跟groupByKey之间的区别

  - 与效率有关

  - reduceByKey有预聚合

  - groupByKey没有预聚合

    

- reduceByKey跟aggregateByKey之间的区别与联系

  - reduceByKey是aggregateByKey实现的子集
  - aggregateByKey，粒度更细
  - combineByKey粒度比aggregateByKey更细

  

- combineByKey的参数作用，说明其参数调用时机

  - 第一个参数是分区内第一个数据需要比较
  - 第二个参数是分区内数据操作
  - 第三个参数是分区间数据操作

  

- 使用RDD实现Join的多种方式

  - 广播变量
  - join，内连接
  - cogroup，外连接

  

- aggregateByKey与aggregate之间的区别与联系

  - 分区间要额外操作

  

- 创建一个RDD，自定义一种分区规则并实现？spark中是否可以按照Value分区

  - 自定义实现分区器
  - 不可用安装value进行分区，只给了key，没有给value

  

- 读取文件，实现WordCount功能。（使用不同的算子实现，至少3种方式）



- 说说你对RDD血缘关系的理解

  - 使用了装饰者设计模式
  - DAG，依据血缘生成一个有向无环图
  - 使用DAG基于宽窄依赖划分阶段
  - 如果血缘关系太长使用cache和checkPoint进行缓存

  

- Spark是如何进行任务切分的，请说明其中涉及到的相关概念

  - 宽窄依赖
  - 看源码，有映像

- RDD的cache和checkPoint的区别和联系

  - 

- Spark读取HDFS文件默认的切片机制

  - 取文件总和，1.1倍

  

- 说说你对广播变量的理解

  - 

- 自定义一个累加器，实现计数功能

  - 可以共享变量，数据采集功能