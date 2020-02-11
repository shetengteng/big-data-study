package com.stt.spark.dw.realtime

import com.stt.spark.dw.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StartUpLogAppTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StartUpLogAppTest").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstant.TOPIC_STARTUP,ssc)

    val valueDStream: DStream[String] = kafkaDStream.map(record =>{
      record.value()
    })
    // 执行foreachRDD 后的 collect行动算子开始真实运行
    valueDStream.foreachRDD(rdd=>{
      println(rdd.collect().mkString("\n"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
