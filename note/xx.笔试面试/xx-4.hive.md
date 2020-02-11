hive的架构，动态分区与静态分区，四个by，窗口函数，时间系统函数，hive的优化

## 架构

- metastore，存储在mysql中
- 解释器，优化器，执行器
- 本质是将sql转换成mr程序执行
- hive的数据存在hdfs上，元数据信息存储在mysql上，需要给mysql配置主从，防止mysql宕机，避免元数据损坏
- hive底层将HQL抽象成语法树，然后转换成查询块->转换成执行逻辑最后优化成MR计划，选择最优的MR程序进行执行
- 一般是多少个select 就有多少个job，一般是job串联

