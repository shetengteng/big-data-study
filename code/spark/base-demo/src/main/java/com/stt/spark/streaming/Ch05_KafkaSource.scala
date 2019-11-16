package com.stt.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object Ch05_KafkaSource {

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setMaster("local[*]").setAppName("Ch04_ReceiverSource")

    // 5s读取一次
    val context: StreamingContext = new StreamingContext(conf,Seconds(5))

    // kafka数据源
    val dStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](
      context,
      Map(
//        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092", // 0.8版本必须连接zookeeper
        "zookeeper.connect" -> "hadoop102:2181",
        ConsumerConfig.GROUP_ID_CONFIG -> "spark",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"

      ),
      Map(
        "spark-topic" -> 3
      ),
      StorageLevel.MEMORY_ONLY
    )

    dStream.map(t=>(t._2,1)).reduceByKey(_ + _).print()

    // 开启接收器
    context.start()

    // main的是driver，需要一直启动，等待接收器执行
    context.awaitTermination()
  }

}