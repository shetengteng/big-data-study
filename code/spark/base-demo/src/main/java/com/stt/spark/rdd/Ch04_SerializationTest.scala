package com.stt.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Ch04_SerializationTest {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("serial").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("aa","bb","cc"))

    var s = new Search("a")
    val rdd2: RDD[String] = s.getMatch2(rdd)

    rdd2.collect.foreach(println)

    sc.stop()
  }
}

class Search(query:String) extends Serializable{

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD 写法1
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD 写法2
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(query))
  }
}