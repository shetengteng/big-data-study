package com.stt.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Ch08_Add {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("ch08"))
    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4))

    // 使用采集器（累加器）
    val sum: LongAccumulator = sc.longAccumulator("sum")

    rdd.map(item =>{
      sum.add(item)
    }).collect

    println(sum.value)
    sc.stop()
  }
}