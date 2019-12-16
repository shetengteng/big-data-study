package com.stt.flink.transform

import org.apache.flink.streaming.api.scala._


/**
  * 从集合中读取数据
  */
object Ch09_ConnectedOperator {

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
        if (item.temperature > 30) Seq("high") else Seq("low") // 可以打上多个标记
      }
    )
    val highStream: DataStream[SensorEntity] = splitStream.select("high")
    val lowStream: DataStream[SensorEntity] = splitStream.select("low")

    val warning: DataStream[(String, Double, String)] = highStream.map(item => (item.id, item.temperature, "warn"))
    // 进行connected操作
    val connected: ConnectedStreams[(String, Double, String), SensorEntity] = warning.connect(lowStream)

    val value: DataStream[Product with Serializable] = connected.map(
      warningData => (warningData._1, warningData._2, warningData._3),
      lowData => (lowData.id, lowData.temperature, "low", lowData.timestamp)
    )

    value.print()

    env.execute("Ch09_ConnectedOperator")
  }
}