package com.stt.flink.source

import org.apache.flink.streaming.api.scala._

/**
  * 定义一个数据样例类，传感器实体
  *
  * @param id
  * @param timestamp
  * @param temperature
  */
case class SensorEntity(id:String, timestamp: Long, temperature: Double)

/**
  * 从集合中读取数据
  */
object Ch04_FromArray {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从自定义的集合中读取数据
    val sensorStream: DataStream[SensorEntity] = env.fromCollection(List(
      SensorEntity("s01", 1547718199, 35.80018327300259),
      SensorEntity("s02", 1547718201, 15.402984393403084),
      SensorEntity("s03", 1547718202, 6.720945201171228),
      SensorEntity("s04", 1547718205, 38.101067604893444)
    ))

    sensorStream.print("test01").setParallelism(2)

    // 从已有的元素中获取
    env.fromElements(1,3.0,"fromElement").print()

    env.execute("Ch04_FromArray")

  }
}
