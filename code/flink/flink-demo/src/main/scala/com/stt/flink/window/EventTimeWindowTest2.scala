package com.stt.flink.window

import com.stt.flink.source.SensorEntity
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimeWindowTest2 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    // 指定eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置watermark生成的时间间隔
    env.getConfig.setAutoWatermarkInterval(300L)

    val dataStream: DataStream[String] = env.socketTextStream("hadoop102", 8888)

    val sensorStream: DataStream[SensorEntity] = dataStream
      .map(item => {
        val fields: Array[String] = item.split(",")
        SensorEntity(fields(0), fields(1).trim.toLong, fields(2).trim.toDouble)
      })
//           .assignTimestampsAndWatermarks(new MyAssigner())
      .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorEntity](Time.seconds(1)) {
        override def extractTimestamp(t: SensorEntity): Long = t.timestamp * 1000
      }
    )

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

  /**
    * 周期性生成一个waterMark
    */
  class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorEntity] {

    val bound = 1 * 1000 // 单位ms
    var maxTs = Long.MinValue

    // 定义一个规则生成watermark
    override def getCurrentWatermark: Watermark = {
      new Watermark(maxTs - bound)
    }

    // 按照当前最大的时间戳
    override def extractTimestamp(t: SensorEntity, l: Long): Long = {
      maxTs = maxTs.max(t.timestamp * 1000)
      t.timestamp * 1000
    }
  }

}
