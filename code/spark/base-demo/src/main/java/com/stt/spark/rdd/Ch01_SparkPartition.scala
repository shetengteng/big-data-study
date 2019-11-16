package com.stt.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Ch01_SparkPartition {
  def main(args: Array[String]): Unit = {

    // 分区个数1
    // val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("ch1_spark_partition"))
    // 分区个数4
    // val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("ch1_spark_partition"))
    // 分区个数12 按照当前cpu核数而来
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ch1_spark_partition"))

    print(sc.makeRDD(Array(1,2,3,4,5)).partitions.length)

    sc.stop()
  }
}