package com.stt.spark.realtime

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.stt.spark.mall.common.{MallKafkaUtil, RedisUtil}
import com.stt.spark.mall.model.KafkaMsg
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object Req04BlackListApp3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Req04-3").setMaster("local[*]")
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

    // TODO Driver执行一次
    val transformDStream: DStream[KafkaMsg] = msgDStream.transform(rdd => {
      // TODO Driver 周期执行
      //    - 获取用户点击广告的数据
      //    - 判断当前数据中是否含有黑名单数据，如果存在进行过滤
      // 从redis中获取黑名单
      val client: Jedis = RedisUtil.getJedisClient
      val blackUserSet = client.smembers("blackList")
      client.close()
      // 使用广播变量,每个Executor中一份
      val broadCastBlackList: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(blackUserSet)
      rdd.filter(msg => {
        // TODO Executor 执行
        !broadCastBlackList.value.contains(msg.userId)
      })
    })

    // 优化方案
    transformDStream.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {
        val client1: Jedis = RedisUtil.getJedisClient
        datas.foreach(msg => {
          // 在redis中聚合用户点击广告的次数：hash -> (  date:userid:adid,sumClick )
          // 对该hash中的该key的value+1
          // 将时间戳转换为日期
          println((msg.userId, msg.adId))
          val date: Date = new Date(msg.ts.toLong)
          val sdf = new SimpleDateFormat("yyyy-MM-dd")
          var keyField = sdf.format(date) + ":" + msg.userId + ":" + msg.adId
          client1.hincrBy("date:user:advert:clickcount", keyField, 1)
          //    - 获取聚合后的点击次数进行阈值（100）判断
          val sum = client1.hget("date:user:advert:clickcount", keyField).toLong
          if (sum >= 100) {
            //    - 如果点击次数超过阈值，那么会将用户加入redis的黑名单中：set
            client1.sadd("blackList", msg.userId)
          }
        })
        client1.close()
      })
    })

    // 接收器启动
    ssc.start()
    // 接收器要阻塞，Driver要一直启动
    ssc.awaitTermination()
  }
}