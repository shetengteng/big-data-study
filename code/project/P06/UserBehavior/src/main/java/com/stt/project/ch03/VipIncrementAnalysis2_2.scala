package com.stt.project.ch03

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import kafka.message.MessageAndMetadata
import net.ipip.ipdb.City
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB, _}

/**
  * 按地区分组统计每日新增VIP数量，不使用累加器
  */
object VipIncrementAnalysis2_2 {

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
    val msgDStream: InputDStream[ConsumerRecord[String, String]] = getDStreamFromKafka(ssc)
    // 业务处理
    vipIncrementByCountry(msgDStream)

    ssc
  }

  def getDStreamFromKafka(ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {

    // kafka相关参数
    val brokers = prop.getProperty("brokers")
    // 由于从数据库中读取，从配置读取的信息暂时没有用
    val topics = prop.getProperty("topic").split(",").toSet


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
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

  /**
    * 从数据库获取 topic partition offset 信息
    *
    * @return
    */
  def readOffsetFromDB(): List[(String, Int, Long)] = {
    DB.readOnly {
      implicit session =>
        sql"select topic,part_id,offset from unpayment_topic_offset".map {
          r => (r.string(1), r.int(2), r.long(3))
        }.list.apply()
    }
  }

  def vipIncrementByCountry(msgDStream: InputDStream[ConsumerRecord[String, String]]) = {

    // 定义偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    // 使用transform 算子的原因是，不会对分区进行修改，此时rdd的分区和kafka的分区保持一致
    msgDStream.transform(rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      })
      .map(m => m.value()) // 将value取出
      .checkpoint(Seconds(getProcessInterval * 10)) // 设置checkpoint的周期推荐是scc周期的5到10倍
      .filter(completeOrderData)
      .map(conversionToDateCountryOne) // 数据转换，返回((2019-04-03,北京),1)格式的数据
      .updateStateByKey(updateFunc) // 从checkpoint中取出数据，计算后放入
      .filter(eventTimeLessThan2Days) // 只保留最近2天的状态，而不只保存1天的状态是考虑跨天的情况
      .foreachRDD(rdd => {

        def offsetSortWith = (left: (String, Int, Long), right: (String, Int, Long)) => {
          if (left._1 == right._1) left._2 > right._2 else left._1 > right._1
        }

        // 判断offset是否变化，没有变化就不执行
        val offsetStr1: String = readOffsetFromDB().sortWith(offsetSortWith).toString
        val offsetStr2: String = offsetRanges.map(r => (r.topic, r.partition, r.untilOffset))
          .toList.sortWith(offsetSortWith).toString

        if (!offsetStr1.equals(offsetStr2)) {
          // 使用rdd的collect方式将executor的结果返回到driver端
          val buff = rdd.collect().map {
            case ((dt: String, province: String), sum: Int) => (dt, province, sum)
          }

        // 开始事务
        DB.localTx {
          implicit session => {
            for (o <- buff) {
              // 对分区数据进行更新
              sql"replace into vip_increment_analysis(province,cnt,dt) values (${o._2},${o._3},${o._1})"
                .executeUpdate().apply()
              println(o)
            }

            // 保存offset
            for (offset <- offsetRanges) {
              //println(offset.topic, offset.partition, offset.fromOffset, offset.untilOffset)
              sql"update topic_offset set offset=${offset.untilOffset} where topic=${offset.topic} and part_id=${offset.partition}"
                .update().apply()
            }
            // 更新offset 如果更新失败了，消息重新计算，那么需要做幂等去重
            msgDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
          }
        }
      }

    })

    /**
      * 对msg进行过滤处理
      *
      * @param msg (key,value)
      * @return
      */
    def completeOrderData(msg: String): Boolean = {
      val fields = msg.split("\t")
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
    def conversionToDateCountryOne(msg: String): ((String, String), Int) = {
      val fields = msg.split("\t")
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
        //  return false
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

  // 单位间隔时间
  def getProcessInterval(): Long = {
    prop.getProperty("processingInterval").toLong
  }
}
