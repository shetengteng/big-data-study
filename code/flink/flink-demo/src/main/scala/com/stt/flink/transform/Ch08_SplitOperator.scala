package com.stt.flink.transform

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
object Ch08_SplitOperator {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从自定义的集合中读取数据
    val sensorStream: DataStream[SensorEntity] = env.fromCollection(List(
      SensorEntity("s01", 1547718199, 35.80018327300259),
      SensorEntity("s02", 1547718201, 15.402984393403084),
      SensorEntity("s03", 1547718202, 6.720945201171228),
      SensorEntity("s04", 1547718205, 38.101067604893444)
    ))

    val splitStream: SplitStream[SensorEntity] = sensorStream.split(
      item => {
        if (item.temperature > 30) List("high1","high2") else List("low") // 可以打上多个标记
      }
    )
    val highStream: DataStream[SensorEntity] = splitStream.select("high2")
    val lowStream: DataStream[SensorEntity] = splitStream.select("low")

    highStream.print("high").setParallelism(1)
    lowStream.print("low").setParallelism(1)

    env.execute("Ch08_SplitOperator")
  }
}