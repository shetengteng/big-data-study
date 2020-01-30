package com.stt.project.ch03

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ipip.ipdb.City
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB, _}

/**
  * 按地区分组统计每日新增VIP数量
  */
object VipIncrementAnalysis {

  // 从properties文件中获取各种参数
  val prop = new Properties()
  prop.load(getClass.getClassLoader.getResourceAsStream("config.properties"))

  // 解析ip地址
  val ipdb = new City(getClass.getClassLoader.getResource("ipipfree.ipdb").getPath)

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      // 不同的应用，使用的checkpoint的路径可能不同，需要从外部传入
      println("Usage:Please input checkpoint Path")
      System.exit(1)
    }

    initJDBC()

    val checkpointPath = args(0)

    // 使用getOrCreate方式可以实现从Driver端失败后恢复
    val ssc = StreamingContext.getOrCreate(checkpointPath, () => createStreamContext(checkpointPath))

    // 启动流式计算
    ssc.start()
    ssc.awaitTermination()
  }

  // 统计地区vip的新增数量
  def createStreamContext(checkpointPath: String): StreamingContext = {
    val conf = new SparkConf()
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 优雅关闭配置
      .set("spark.streaming.backpressure.enabled", "true") // 背压配置
      .setAppName(getClass.getSimpleName)

    val ssc = new StreamingContext(conf, Seconds(getProcessInterval))

    // 开启检查点
    ssc.checkpoint(checkpointPath)
    val msgDStream: InputDStream[(String, String)] = getDStreamFromKafka(ssc)
    // 业务处理
    vipIncrementByCountry(msgDStream)

    ssc
  }

  def vipIncrementByCountry(msgDStream: InputDStream[(String, String)]) = {

    // 定义偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    // 使用transform 算子的原因是，不会对分区进行修改，此时rdd的分区和kafka的分区保持一致
    msgDStream
      .transform {
        rdd => {
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }
      .filter(completeOrderData)
      .map(conversionToDateCountryOne) // 数据转换，返回((2019-04-03,北京),1)格式的数据
      .updateStateByKey(updateFunc) // 从checkpoint中取出数据，计算后放入
      .filter(eventTimeLessThan2Days) // 只保留最近2天的状态，而不只保存1天的状态是考虑跨天的情况
      .foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        // 开启事务
        DB.localTx {
          implicit session => {
            // 对分区数据进行更新
            partition.foreach {
              case ((eventTime: String, province: String), sum: Int) => {
                println((eventTime, province, sum))
                sql"replace into vip_increment_analysis(province,cnt,dt) values (${province},${sum.toLong},${eventTime})"
                  .executeUpdate().apply()
              }
            }

            // 保存offset
            for (offset <- offsetRanges) {
//              println(offset.topic, offset.partition, offset.fromOffset, offset.untilOffset)
              sql"update topic_offset set offset=${offset.untilOffset} where topic=${offset.topic} and part_id=${offset.partition}"
                .update().apply()
            }
          }
        }
      })
    })

    /**
      * 对msg进行过滤处理
      *
      * @param msg (key,value)
      * @return
      */
    def completeOrderData(msg: (String, String)): Boolean = {
      val fields = msg._2.split("\t")
      // 切分后长度要是17
      if (fields.length == 17) {
        // 业务字段eventType要求是completeOrder
        return "completeOrder".equals(fields(15))
      }
      return false
    }

    /**
      * 数据转换，返回((2019-04-03,北京),1)格式的数据
      *
      * @param msg
      * @return
      */
    def conversionToDateCountryOne(msg: (String, String)): ((String, String), Int) = {
      val fields = msg._2.split("\t")
      val ip = fields(8)
      val info = ipdb.findInfo(ip, "CN")
      val regionName = if (info != null) info.getRegionName else "未知"

      val eventTime = fields(16).toLong
      val eventDay: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(eventTime * 1000))

      ((eventDay, regionName), 1)
    }

    /**
      * 只保留最近2天的状态，而不只保存1天的状态是考虑跨天的情况
      *
      * @param msg
      * @return
      */
    def eventTimeLessThan2Days(msg: ((String, String), Int)): Boolean = {
      val eventTime: Long = new SimpleDateFormat("yyyy-MM-dd").parse(msg._1._1).getTime
      val now: Long = System.currentTimeMillis()
      if (now - eventTime >= 24 * 2 * 3600) {
        //        return false
      }
      return true
    }

    /**
      * 从检查点获取数据返还给检查点
      *
      * @param datas
      * @param buffer
      * @return
      */
    def updateFunc(datas: Seq[Int], buffer: Option[Int]) = {
      val lastResult = buffer.getOrElse(0)
      val currentResult = datas.sum // 对获取的数据元组的value进行求和
      Some(lastResult + currentResult)
    }


  }

  def initJDBC() = {
    // jdbc相关参数
    val driver = prop.getProperty("jdbcDriver")
    val jdbcUrl = prop.getProperty("jdbcUrl")
    val jdbcUser = prop.getProperty("jdbcUser")
    val jdbcPassword = prop.getProperty("jdbcPassword")

    // 设置jdbc
    Class.forName(driver)
    // 设置连接池
    ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)
  }

  def getDStreamFromKafka(ssc: StreamingContext): InputDStream[(String, String)] = {

    // kafka相关参数
    val brokers = prop.getProperty("brokers")
    // 由于从数据库中读取，从配置读取的信息暂时没有用
    val topics = prop.getProperty("topic").split(",").toSet

    val kafkaParams = Map(
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "largest" // largest从最新的开始读取 smallest从最早的读取
    )
    // 从数据库获取 topic partition offset 信息
    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly {
      implicit session =>
        sql"select topic,part_id,offset from topic_offset".map {
          r => (TopicAndPartition(r.string(1), r.int(2)), r.long(3))
        }.list.apply().toMap
    }

    // 消息处理匿名函数
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

    // 使用kafka的Direct模式，拉取的方式
    // 注意需要声明类型
    val msgDStream: InputDStream[(String, String)] = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    // 设置checkpoint的周期推荐是scc周期的5到10倍
    msgDStream.checkpoint(Seconds(getProcessInterval * 10))
    msgDStream
  }

  // 单位间隔时间
  def getProcessInterval(): Long = {
    prop.getProperty("processingInterval").toLong
  }
}