package com.stt.recommend

// 将ES 和 MongoDB的配置封装成样例类
/**
  *
  * @param httpHosts      http主机列表，逗号分隔 9200
  * @param transportHosts transport 主机列表 9300 集群之间用于数据传递
  * @param index          需要操作的索引
  * @param clustername    集群名，默认为elasticsearch
  */
case class ESConfig(
                     httpHosts: String,
                     transportHosts: String,
                     index: String,
                     clustername: String
                   )