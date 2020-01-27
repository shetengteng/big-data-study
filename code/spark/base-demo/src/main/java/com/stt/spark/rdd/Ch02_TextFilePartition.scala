package com.stt.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Ch02_TextFilePartition {
  def main(args: Array[String]): Unit = {

    // 分区个数1
    // val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("ch1_spark_partition"))
    // 分区个数4
    // val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("ch1_spark_partition"))
    // 分区个数12 按照当前cpu核数而来
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ch1_spark_partition"))

    val rdd: RDD[String] = sc.textFile("data/spark/Ch01_partition")
    print(rdd.partitions.length)
    rdd.saveAsTextFile("data/spark/Ch01_output")
    sc.stop()
  }
}