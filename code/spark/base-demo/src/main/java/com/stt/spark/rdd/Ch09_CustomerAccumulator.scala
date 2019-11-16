package com.stt.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object Ch09_CustomerAccumulator {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("ch08"))

    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4))

    // 使用采集器（累加器）
    val sum: MyAccumulator = new MyAccumulator

    // 注册累加器
    sc.register(sum,"sum")

    rdd.map(item =>{
      sum.add(item)
    }).collect

    println(sum.value)

    sc.stop()
  }
}

// 自定义累加器
class MyAccumulator extends AccumulatorV2[Int,Int]{

  var sum = 0

  // 是否初始状态
  override def isZero: Boolean = sum == 0

  // 执行器执行时，需要拷贝累加器对象
  override def copy(): AccumulatorV2[Int, Int] = {
     val self = new MyAccumulator()
      self.synchronized{
        self.sum = 0
      }
      self
  }

  // 重置数据
  override def reset(): Unit = sum = 0

  // 累加数据
  override def add(v: Int): Unit = sum += v

  // 合并计算的结果数据
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    sum += other.value
  }

  // 累加器的结果
  override def value: Int = sum
}
