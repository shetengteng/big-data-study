package com.stt.spark.dw.realtime

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.JSON
import com.stt.spark.dw.realtime.bean.OrderInfo
import com.stt.spark.dw.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstant.TOPIC_ORDER, ssc)

    // 对kafkaDStream中的json转换为bean对象
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      val orderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      val time: LocalDateTime =
        LocalDateTime.parse(orderInfo.createTime,
          DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      orderInfo.createDate = time.toLocalDate.toString
      orderInfo.createHour = time.getHour.toString
      orderInfo.createHourMinute = time.getMinute.toString

      // 在业务上需要对特殊字段进行脱敏处理，如收件人，手机号等
      // splitAt 表示在某个位置切分为元组
      orderInfo.consignee = orderInfo.consignee.splitAt(1)._1+"**"
      orderInfo.consigneeTel = orderInfo.consigneeTel.splitAt(3)._1+"********"
      orderInfo
    })

    // 保存到ES中
    orderInfoDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(datas =>{
//        println(datas.toList.mkString("\n"))
        // 注意 datas.size后，datas重置为空 datas是一个迭代器类型
        val list = datas.toList
        if(list.size!=0){
          MyEsUtil.executeIndexBulk(GmallConstant.ES_INDEX_ORDER,list,null)
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
