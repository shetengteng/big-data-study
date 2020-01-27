package com.stt.flink.T01

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object KafkaProducerForTestHotItems {

  def main(args: Array[String]): Unit = {

   var producer = new KafkaProducer[String,String](new util.HashMap[String,Object](){{
     put("bootstrap.servers", "hadoop102:9092")
     put("acks","all")
     put("retries","1")
     put("batch.size","16384")
     put("linger.ms","1")
     put("buffer.memory","33554432")
     put("key.serializer", classOf[StringSerializer])
     put("value.serializer", classOf[StringSerializer])
   }})

    // 写法1
//    val datas: util.List[String] = FileUtils.readLines(new File(this.getClass.getClassLoader.getResource("UserBehavior.csv").getPath))
//
//    import scala.collection.JavaConversions._
//
//    for(data <- datas){
//      producer.send(new ProducerRecord[String,String]("hotItems",data))
//    }

    // 写法2
    scala.io.Source.fromFile(this.getClass.getClassLoader.getResource("UserBehavior.csv").toURI).getLines().foreach(
      data => {
        producer.send(new ProducerRecord[String,String]("hotItems",data))
      }
    )

    producer.close()
  }

}