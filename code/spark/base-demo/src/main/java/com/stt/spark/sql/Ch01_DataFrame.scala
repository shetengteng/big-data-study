package com.stt.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Ch01_DataFrame {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ch01")
    // 构建SparkSQL上下文，SparkSession构造方法私有，使用builder构建
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 添加隐式转换
    import sparkSession.implicits._

    val rdd: RDD[(Int, String, Int)] = sparkSession.sparkContext.makeRDD(Array((1,"zhangsan",22),(2,"lisi",23)))



    // 将rdd转换为DataFrame，执行列结构
    val df: DataFrame = rdd.toDF("id","name","age")

    df.show()

    // 将DataFrame转换为DataSet
    val ds: Dataset[User] = df.as[User]
    ds.show()

    sparkSession.stop()
  }
}

case class User(id:Int,name:String,age:Long)
