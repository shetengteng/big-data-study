package com.stt.spark.rdd

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object Ch10_CustomerAccumulator {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("ch10"))

    val rdd: RDD[String] = sc.makeRDD(Array("error1","ss","error3","ee"))

    // 使用采集器（累加器）
    val blackList: MyBlackListAccumulator = new MyBlackListAccumulator

    // 注册累加器
    sc.register(blackList)

    rdd.map(item =>{
      blackList.add(item)
      item
    }).collect

    println(blackList.value)

    sc.stop()
  }
}

// 判断运行情况，采集有问题的信息
// 使用HashSet的原因，是错误数据相同的只要记录一个
class MyBlackListAccumulator extends AccumulatorV2[String,util.HashSet[String]]{

  var blackList = new util.HashSet[String]()

  override def isZero: Boolean = blackList.isEmpty

  override def copy(): AccumulatorV2[String, util.HashSet[String]] = new MyBlackListAccumulator()

  override def reset(): Unit = blackList.clear()

  override def add(v: String): Unit = {
    if(v.contains("error")){
      blackList.add(v)
    }
  }

  override def merge(other: AccumulatorV2[String, util.HashSet[String]]): Unit = blackList.addAll(other.value)
  
  override def value: util.HashSet[String] = blackList
}