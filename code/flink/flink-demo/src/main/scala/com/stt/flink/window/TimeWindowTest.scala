package com.stt.flink.window

import com.stt.flink.source.SensorEntity
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TimeWindowTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[String] = env.socketTextStream("hadoop102",8888)

    val sensorStream: DataStream[SensorEntity] = dataStream
      .map(item => {
        val fields: Array[String] = item.split(",")
        SensorEntity(fields(0), fields(1).trim.toLong, fields(2).trim.toDouble)
      })


    // 统计10s内的最小温度
    val minTemperatureWindowStream: DataStream[(String, Double)] = sensorStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10)) // 开时间窗口
      .reduce((s1, s2) => (s1._1, s1._2.min(s1._2))) // reduce进行聚合操作

    minTemperatureWindowStream.print("timeWindow")

    sensorStream.print("input Data")

    env.execute("TimeWindowTest")
  }
}