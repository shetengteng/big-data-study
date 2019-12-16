package com.stt.flink.sink

import com.stt.flink.source.SensorEntity
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从自定义的集合中读取数据
    val sensorStream: DataStream[SensorEntity] = env.fromCollection(List(
      SensorEntity("s01", 1547718199, 35.80018327300259),
      SensorEntity("s02", 1547718201, 15.402984393403084),
      SensorEntity("s03", 1547718202, 6.720945201171228),
      SensorEntity("s04", 1547718205, 38.101067604893444)
    ))

    val dataStream = sensorStream.map(data=>data.toString)

    // 向kafka发送消息
    dataStream.addSink(new FlinkKafkaProducer011[String](
      "hadoop102:9092",
      "sensorTest",
      new SimpleStringSchema())
    )

    env.execute("KafkaSinkTest")
  }
}