package com.stt.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.common.serialization.StringDeserializer


object Ch06_FromKafka {

  val topic = "sensor"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaStream: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String](
        topic,
        new SimpleStringSchema(),
        new Properties() {
          {
            setProperty("bootstrap.servers", "hadoop102:9092")
            setProperty("group.id", "consumer-group")
            setProperty("key.deserializer", classOf[StringDeserializer].toString)
            setProperty("value.deserializer", classOf[StringDeserializer].toString)
            setProperty("auto.offset.reset", "latest")
          }
        })
    )

    kafkaStream.map(item =>{
        val fields = item.split(",")
        SensorEntity(fields(0).trim,fields(1).trim.toLong,fields(2).trim.toDouble)
      }
    ).print("kafka").setParallelism(1)

    env.execute("Ch06_FromKafka")
  }
}
