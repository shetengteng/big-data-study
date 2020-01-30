package com.stt.realtime

import java.text.SimpleDateFormat
import java.util.Date

import com.stt.spark.mall.common.{MallKafkaUtil, RedisUtil}
import com.stt.spark.mall.model.KafkaMsg
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.JsonMethods

// 最近1小时广告流量的趋势
object Req07TimeAreaAdvertClickTrendApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Req07").setMaster("local[*]")
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

    // 将一定时间范围内的数据当成一个整体进行统计
    val windowDStream: DStream[KafkaMsg] = msgDStream.window(Seconds(60),Seconds(10))
    // 对时间进行转换
    val windowMapDStream: DStream[(String, Long)] = windowDStream.map(msg => {
      // ts是ms单位
      val ts = msg.ts.toLong
      // 获取时间戳的转换，将尾部余数s去除-->重点
      var tsStr = ts / 10000 + "0000"
      (tsStr + ":" + msg.adId, 1L)
    })
    // 汇总
    val windowSumClickDStream: DStream[(String, Long)] = windowMapDStream.reduceByKey(_+_)
    // 转换格式
    val adToSumDStream: DStream[(String, (String, Long))] = windowSumClickDStream.map {
      case (ts_ad, sum) => {
        val keys = ts_ad.split(":")
        // (ad,(ts,sum))
        val date: Date = new Date(keys(0).toLong)
        val sdf = new SimpleDateFormat("mm:ss")
        (keys(1), (sdf.format(date), sum))
      }
    }
    //
    // 将指定的数据的广告点击次数进行统计
    // 将统计结果按照时间进行排序
    val resultDStream: DStream[(String, List[(String, Long)])] = adToSumDStream.groupByKey().mapValues(datas => {
      datas.toList.sortWith {
        case (left, right) => {
          left._1 < right._1
        }
      }
    })
    // 将结果保存到redis中 key:ads_sumclick_trend value: ad List[(date,sum)]
    // 保存数据时需要考虑到格式List，趋势是按照时间排序的
    resultDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(datas =>{
        var client = RedisUtil.getJedisClient
        datas.foreach{
          case (ad,ts_sum_list) =>{
              import org.json4s.JsonDSL._
              var ads_click_json = JsonMethods.compact(JsonMethods.render(ts_sum_list))
              client.hset("ads:sumclick:trend",ad,ads_click_json)
          }
        }

        client.close()

//        等价于
//        for((key,list) <- datas){
//            ...
//        }

      })
    })
    // 接收器启动
    ssc.start()
    // 接收器要阻塞，Driver要一直启动
    ssc.awaitTermination()
  }
}