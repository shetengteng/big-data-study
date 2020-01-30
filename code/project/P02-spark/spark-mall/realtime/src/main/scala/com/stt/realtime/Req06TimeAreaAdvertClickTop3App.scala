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

// 每天热门地区广告top3
object Req06TimeAreaAdvertClickTop3App {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Req06").setMaster("local[*]")
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

    // *************************
    //获取需求5的数据：（ 'date:area:city:advert', sum ）
    //将数据进行转换：（ date:area:city:advert, sum ）-->（ date:area:advert, sum1 ）, （ date:area:advert, sum2 ）, （ date:area:advert, sum3 ）
    // 将数据转换结构：（Message）--> (ts:area:advert, 1L)
    val sumClickMapDStream: DStream[(String, Long)] = sumClickDStream.map {
      case (fields, sum) => {
        val fieldList = fields.split(":")
        (fieldList(0) + ":" + fieldList(1) + ":" + fieldList(3), sum)
      }
    }
    //将转换后的数据进行聚合：（ date:area:advert, sumTotal ）
    val sumTotalClickDStream: DStream[(String, Long)] = sumClickMapDStream.reduceByKey(_ + _)
    //将聚合后的数据进行结构转换：（ date:area:advert, sumTotal ）-> （ (date,area）(advert, sumTotal )）
    val sumTotalMapDStream: DStream[((String, String), (String, Long))] = sumTotalClickDStream.map {
      case (date_area_ad, sum) => {
        val fields: Array[String] = date_area_ad.split(":")
        ((fields(0), fields(1)), (fields(2), sum))
      }
    }

    //将转换结构后的数据进行分组排序，获取前三的数据
    val top3ResultDStream: DStream[((String, String), List[(String, Long)])] =
      sumTotalMapDStream.groupByKey().mapValues(datas => {
        datas.toList.sortWith {
          case (left, right) => {
            left._2 > right._2
          }
        }.take(3)
      })

    //将结果保存到Redis中（json的转换）
    top3ResultDStream.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {
        val client = RedisUtil.getJedisClient
        datas.foreach {
          case ((date, area), adSumList) => {
            var key = "top3_ads_per_day:" + date
            // 将list转换为map
            val adSumMap: Map[String, Long] = adSumList.toMap
            // 隐式转换
            import org.json4s.JsonDSL._

            var ads_click_json = JsonMethods.compact(JsonMethods.render(adSumMap))
            client.hset(key, area, ads_click_json)
          }
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