package com.stt.spark.dw.realtime

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import com.alibaba.fastjson.JSON
import com.stt.spark.dw.realtime.bean.StartUpLog
import com.stt.spark.dw.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


object StartUpLogApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StartUpLogApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstant.TOPIC_STARTUP,ssc)

    // 对kafka中的json数据进行转换为对象
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      val log: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
      // 将ts时间转换为日常时间存储到ES中，由于ES中不能对字段进行函数操作，因此需要存储冗余转换数据
      val time =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(log.ts), ZoneId.systemDefault())
      log.logHour = time.getHour.toString
      log.logHourMinute = time.getHour + ":" + time.getMinute
      log.logDate = time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
      log
    })


    // 方式1
    // 依据 redis中已有的当日访问用户进行过滤
    val startUpLogFilterDStream: DStream[StartUpLog] = startUpLogDStream.filter(log => {
      val client = RedisUtil.getJedisClient
      val flag = !client.sismember("dau:" + log.logDate, log.mid)
      client.close()
      flag
    })

    // 方式2
    // 进行优化，减少连接redis次数
    // 分析，需要查看应用场景，如果mid过多，set内元素过多，那么会造成每次redis查询结果传输负担重，造成redis阻塞
    // 此时还是使用第一种方式
    // transform由Driver固定周期执行一次
//    val startUpLogFilterDStream: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
//      println(s"filter before ${rdd.count()}")
//      val client = RedisUtil.getJedisClient
//      val time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
//      val midSet = client.smembers("dau:" + time).toArray
//      client.close()
//      // 如果使用广播变量,每个Executor中一份，使用kryo序列化
//      // 不使用广播变量，需要对set进行转换为array，使用java序列化，每个Executor读取到midSet之后从Driver中获取
//      val filterRDD: RDD[StartUpLog] = rdd.filter(log => {
//        !midSet.contains(log.mid)
//      })
//      println(s"filter after ${filterRDD.count()}")
//      filterRDD
//    })

    // 将当日访问数据写入到redis
    startUpLogFilterDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(datas =>{
        val client: Jedis = RedisUtil.getJedisClient

        // 需要使用listBuffer缓存保存数据
        val listBuffer = new ListBuffer[StartUpLog]
        datas.foreach(log => {
            val key = "dau:"+ log.logDate
            client.sadd(key,log.mid)
            listBuffer += log
        })
        println(s"filter after ${listBuffer.size}")
        client.close()
        if(listBuffer.size != 0){
          // 将DAU数据保存到ES中
          MyEsUtil.executeIndexBulk(GmallConstant.ES_INDEX_DAU,listBuffer.toList,null)
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
