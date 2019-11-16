package com.stt.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object Ch04_UDAF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ch04")
    // 构建SparkSQL上下文，SparkSession构造方法私有，使用builder构建
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 添加隐式转换
    import sparkSession.implicits._

    val df: DataFrame = sparkSession.read.json("data/spark/sql/Ch01/person.json")
    // 强类型的UDAF函数，必须要使用强类型的DataSet
    val avgUDAF: TypedColumn[Person3, Double] = new AgeAvgUDAF2().toColumn.name("ageAvg")

    // 将DataFrame转换为DataSet
    val ds: Dataset[Person3] = df.as[Person3]
    ds.createTempView("user")

    ds.select(avgUDAF).show()

    sparkSession.stop()
  }
}

case class Person3(id:Long,name:String,age:Long)
case class BufferAvg(var sum:Long,var count:Long)

// 统计年龄的平均值
// 强类型，适用于DataSet
class AgeAvgUDAF2 extends Aggregator[Person3,BufferAvg,Double]{
  // 中间数据初始值
  override def zero: BufferAvg = BufferAvg(0L,0L)

  // 分区内聚合
  override def reduce(b: BufferAvg, a: Person3): BufferAvg = {
    b.sum += a.age
    b.count += 1
    b
  }

  // 分区间聚合
  override def merge(b1: BufferAvg, b2: BufferAvg): BufferAvg = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // 完成计算
  override def finish(reduction: BufferAvg): Double = {
    reduction.sum.toDouble / reduction.count
  }

  // 节点间传递需要序列化，编码器，对象使用product，常规类型用scalaXXX
  override def bufferEncoder: Encoder[BufferAvg] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}