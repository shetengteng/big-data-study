package com.stt.project.ch04

import java.util.Properties

import com.stt.project.ch03.VipIncrementAnalysis2._
import com.stt.project.ch03.VipIncrementAnalysis3.initJDBC
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, _}

import util.control.Breaks._

/**
  * 每隔30S，统计近60S的用户行为数据，当出现进入定单页（eventKey=enterOrderPage）>=3次，
  * 但是没有成功完成订单时(去业务数据库的实时备份表vip_users查询用户是否为VIP)，即将用户uid持久化到Mysql中
  */
object UnPaymentAnalysis {

  val prop = new Properties()
  prop.load(getClass.getClassLoader.getResourceAsStream("config.properties"))

  initJDBC()

  def main(args: Array[String]): Unit = {

    val ssc = createStreamContext()

    // 启动流式计算
    ssc.start()
    ssc.awaitTermination()
  }

  def createStreamContext(): StreamingContext = {
    val conf = new SparkConf()
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 优雅关闭配置
      .set("spark.streaming.backpressure.enabled", "true") // 背压配置
      .setAppName(getClass.getSimpleName)

    val ssc = new StreamingContext(conf, Seconds(getProcessInterval))

    val msgDStream: InputDStream[ConsumerRecord[String, String]] = getDStreamFromKafka(ssc)
    // 业务处理
    unPaymentAnalysisHandler(msgDStream)
    ssc
  }

  def getDStreamFromKafka(ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> prop.getProperty("brokers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> getClass.getSimpleName,
      "auto.offset.reset" -> "latest", // latest从最新的开始读取 smallest从最早的读取
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val fromOffsets: Map[TopicPartition, Long] =
      readOffsetFromDB().map(r => (new TopicPartition(r._1, r._2), r._3)).toMap

    // 消息处理匿名函数
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

    // 使用kafka的Direct模式，拉取的方式
    // 注意需要声明类型
    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets)
    )
  }

  def unPaymentAnalysisHandler(msgDStream: InputDStream[ConsumerRecord[String, String]]) = {

    // 进行过滤处理，小于3个的以及存在于数据库的
    val filterUnnormalOrderUser = (event:((String,String),Int))=>{

      if(event._2 >=3){
        // 查询数据库
        val result: List[Int] = DB.readOnly {
          implicit session => {
            sql"""
                select id
                from vip_user
                where uid = ${event._1._1}
              """.map(r => r.get[Int](1)).list().apply()
          }
        }
        // 如果结果为空，代表用户还不是vip，所以需要做后续运营
        if(result.isEmpty){
          true
        }else{
          false
        }
      }else{
        false
      }
    }

    // 定义偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    // 使用transform 算子的原因是，不会对分区进行修改，此时rdd的分区和kafka的分区保持一致
    msgDStream
      .transform(rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      })
      .map(m => m.value()) // 将value取出
      .filter(completeOrderData)
      .map(conversionUIDAndOne) // 数据转换，返回((uid,phone),1)格式的数据
      .reduceByKeyAndWindow((a:Int,b:Int) => a+b,Seconds(getProcessInterval*4), Seconds(getProcessInterval*2)) // 窗口大小60s，滑动距离30s
      .filter(filterUnnormalOrderUser)
      .map{ case ((uid: String, phone: String), sum: Int) => (uid, phone,sum)} // 转换格式
      .foreachRDD(rdd => {

        // 将所有的rdd的结果汇总到driver，当数据量小的时候
        val results: Array[(String, String,Int)] = rdd.collect()

          // 开始事务
          DB.localTx {
            implicit session => {
              for (o <- results) breakable {
                // 对分区数据进行更新
                sql"replace into unpayment_record(uid,phone) values (${o._1},${o._2})"
                  .executeUpdate().apply()
                println(o)
              }

              // 保存offset
              for (offset <- offsetRanges) {
                //println(offset.topic, offset.partition, offset.fromOffset, offset.untilOffset)
                sql"update unpayment_topic_offset set offset=${offset.untilOffset} where topic=${offset.topic} and part_id=${offset.partition}"
                  .update().apply()
              }
              // 更新offset 如果更新失败了，消息重新计算，那么需要做幂等去重
              msgDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
            }
          }
      })

    //对msg进行过滤处理
    def completeOrderData(msg: String): Boolean = {
      val fields = msg.split("\t")
      // 切分后长度要是17
      if (fields.length == 17) {
        return "enterOrderPage".equals(fields(15))
      }
      return false
    }

    // 数据转换，返回((uid,phone),1)格式的数据
    def conversionUIDAndOne(msg: String): ((String, String), Int) = {
      val fields = msg.split("\t")
      val uid = fields(0)
      val phone = fields(9)
      ((uid, phone), 1)
    }
  }
}