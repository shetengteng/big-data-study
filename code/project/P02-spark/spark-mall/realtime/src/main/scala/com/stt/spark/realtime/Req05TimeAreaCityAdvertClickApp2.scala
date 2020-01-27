package com.stt.spark.realtime

import java.text.SimpleDateFormat
import java.util.Date

import com.stt.spark.mall.common.{MallKafkaUtil, RedisUtil}
import com.stt.spark.mall.model.KafkaMsg
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object Req05TimeAreaCityAdvertClickApp2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Req05-2").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "ads_log"

    // 获取kafka的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MallKafkaUtil.getKafkaStream(topic, ssc)

    val msgDStream: DStream[KafkaMsg] = kafkaDStream.map(record => {
      val line = record.value()
      val datas = line.split(" ")
      KafkaMsg(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    //    - 获取kafka中传递的数据
    //    - 将数据转换结构：（Message）--> (ts:area:city:advert, 1L)
    val msgMapDStream: DStream[(String, Long)] = msgDStream.map(msg => {
      val date: Date = new Date(msg.ts.toLong)
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      var keyField = sdf.format(date) + ":" + msg.area + ":" + msg.city + ":" + msg.adId
      (keyField, 1L)
    })
    //    - 将转换后的数据进行聚合统计(ts:area:city:advert, 1L)--> (ts:area:city:advert, sum)
    //    - 将结果保存起来
    //    - 方式2：使用有状态RDD，将数据保存到CP，同时更新Redis
    ssc.sparkContext.setCheckpointDir("cp")

    val sumClickDStream: DStream[(String, Long)] = msgMapDStream.updateStateByKey {
      // 第一个参数是相同key 的seq -->[(keyField, 1L),..]
      // 第二个参数是checkpoint
      case (seq, checkpoint) => {
        val sum = checkpoint.getOrElse(0L) + seq.sum
        // 返回值会放回checkpoint中
        Option(sum)
      }
    }
    // 此时获得的是最终的sum结果
    sumClickDStream.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {
        var client = RedisUtil.getJedisClient
        for ((key, sum) <- datas) {
          client.hset("data:area:city:ads", key, sum.toString)
        }
        client.close()
      })
    })

    // 接收器启动
    ssc.start()
    // 接收器要阻塞，Driver要一直启动
    ssc.awaitTermination()
  }
}