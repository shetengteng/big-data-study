package com.stt.spark.rdd

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Ch05_SelfPartitioner {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("serial").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(("aac",1),("bb",2),("cc",3)))
//    (7,(bb,2))
//    (11,(cc,3))
//    (3,(aac,1))
    rdd.mapPartitionsWithIndex((index,items)=>{items.map(item=>(index,item))}).foreach(println)

    val rdd2 = rdd.partitionBy(new CustomerPartitioner(2))
//    (1,(aac,1))
//    (0,(bb,2))
//    (1,(cc,3))
    rdd2.mapPartitionsWithIndex((index,items)=>{items.map(item=>(index,item))}).foreach(println)

    sc.stop()
  }
}

class CustomerPartitioner(numPartition: Int) extends Partitioner{
  override def numPartitions: Int = numPartition

  override def getPartition(key: Any): Int = {
    // 取得最后一个字符进行分区
    key.toString.last.toInt%numPartitions
  }
}
