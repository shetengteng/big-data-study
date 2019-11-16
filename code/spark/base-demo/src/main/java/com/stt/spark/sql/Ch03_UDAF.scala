package com.stt.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object Ch03_UDAF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ch02")
    // 构建SparkSQL上下文，SparkSession构造方法私有，使用builder构建
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 添加隐式转换
    import sparkSession.implicits._

    val df: DataFrame = sparkSession.read.json("data/spark/sql/Ch01/person.json")

    val avgUDAF = new AgeAvgUDAF()
    sparkSession.udf.register("ageAvg",avgUDAF)

    // 将DataFrame转换为DataSet
    val ds: Dataset[Person2] = df.as[Person2]
    ds.createTempView("user")

    sparkSession.sql("select * from user").show()
    sparkSession.sql("select ageAvg(age) from user").show()

    sparkSession.stop()
  }
}

case class Person2(id:BigInt,name:String,age:BigInt)

// 统计年龄的平均值
// 弱类型，适用于DataFrame
class AgeAvgUDAF extends UserDefinedAggregateFunction{

  // 输入的数据的结构
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)
  }

  // 中间的逻辑处理的数据的结构
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }

  // 处理完成的结果的类型
  override def dataType: DataType = {
    DoubleType
  }

  // 表示当前函数是否稳定（一致性）
  override def deterministic: Boolean = true

  // 数据处理时，中间缓存数据初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 对应的 new StructType().add("sum",LongType).add("count",LongType) 结构赋初始值
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 通过每次的输入数据，更新缓存数据，分区内更新
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // 将多个节点的缓存数据合并操作，分区间操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}