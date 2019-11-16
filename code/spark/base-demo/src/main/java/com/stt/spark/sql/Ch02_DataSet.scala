package com.stt.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Ch02_DataSet {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ch02")
    // 构建SparkSQL上下文，SparkSession构造方法私有，使用builder构建
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 添加隐式转换
    import sparkSession.implicits._

    val df: DataFrame = sparkSession.read.json("data/spark/sql/Ch01/person.json")

    // 将DataFrame转换为DataSet
    val ds: Dataset[Person] = df.as[Person]
    ds.show()

    sparkSession.stop()
  }
}

case class Person(id:BigInt,name:String,age:BigInt)
