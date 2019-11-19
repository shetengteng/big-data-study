package com.stt.spark.mall.offline

import com.stt.spark.mall.common.{CommonUtil, ConfigurationUtil}
import com.stt.spark.mall.model.UserVisitAction
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

// 需求3 页面跳转转化率
object Req03PageJumpRateApp {

  def main(args: Array[String]): Unit = {

    // 使用sparkSql获取数据
    val conf = new SparkConf().setMaster("local[*]").setAppName("Req03")

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    // 引入隐式转换
    import spark.implicits._

    spark.sql("use " + ConfigurationUtil.getValueByKey("hive.database"))

    var sql = new StringBuilder("select * from user_visit_action where 1 = 1")

    var startDate = ConfigurationUtil.getCondValue("startDate")
    var endDate = ConfigurationUtil.getCondValue("endDate")

    if (!CommonUtil.isEmpty(startDate)) {
      sql.append(" and action_time >='").append(startDate).append("'")
    }

    if (!CommonUtil.isEmpty(endDate)) {
      sql.append(" and action_time <='").append(endDate).append("'")
    }

    val actionDF: DataFrame = spark.sql(sql.toString)

    // 转换DataSet到RDD
    val actionRDD: RDD[UserVisitAction] = actionDF.as[UserVisitAction].rdd

    // **************************************************
    //    - 获取Hive中保存日志数据
    //    - 将日志数据根据session进行分组排序，获取同一个session中的页面跳转路径
    //    - 将页面跳转路径形成拉链效果  A--->B.B--->C
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(log => {
      log.session_id
    })

    // key是sessionId,value是pageFlow,count
    val zipPageIdCountRDD: RDD[(String, List[(String, Long)])] = sessionRDD.mapValues(datas => {
      // 按照时间进行排序
      val pageFlow: List[UserVisitAction] = datas.toList.sortWith {
        case (left, right) => {
          left.action_time < right.action_time
        }
      }
      // 只需要 pageId
      // 1 - 2 - 3 - 4
      val pageIdFlow1: List[Long] = pageFlow.map(log => {
        log.page_id
      })
      // 去除第一个，zip后形成拉链表
      // 2 - 3 - 4
      val pageIdFlow2: List[Long] = pageIdFlow1.tail

      // 形成拉链表List[(1,2),(2,3),(3,4)]
      // 注意使用scala原生的zip操作
      val zipPageIdFlow: List[(Long, Long)] = pageIdFlow1.zip(pageIdFlow2)

      // 对拉链表进行组合成key
      zipPageIdFlow.map {
        case (pId1, pId2) => (pId1 + "_" + pId2, 1L)
      }
    })

    // 统计拉链后的数据点击总次数（A） 进行扁平化
    // 直接写flatMap(_) 运行时会出错，无法推测是类型还是方法
    val flatZipToCountRDD: RDD[(String, Long)] = zipPageIdCountRDD.map(_._2).flatMap(x => x)

    // 获取条件参数进行过滤
    val targetIds: Array[String] = ConfigurationUtil.getCondValue("targetPageFlow").split(",")

    val targetPageFlow = targetIds.zip(targetIds.tail).map {
      case (id1, id2) => id1 + "_" + id2
    }
    // - 将符合条件的日志数据根据页面ID进行分组聚合（B）
    val filterZipToCountRDD: RDD[(String, Long)] = flatZipToCountRDD.filter {
      case (pageFlow, count) => {
        targetPageFlow.contains(pageFlow)
      }
    }
    // 分子 (1-2,10)
    val reduceZipToSumRDD: RDD[(String, Long)] = filterZipToCountRDD.reduceByKey(_ + _)

    // 过滤 分母中符合条件的数据
    val actionFilterRDD: RDD[UserVisitAction] = actionRDD.filter(log => {
      targetIds.contains(log.page_id.toString)
    })

    // 分母
    val pageActionRDD: RDD[(Long, Long)] = actionFilterRDD.map(log => (log.page_id, 1L)).reduceByKey(_ + _)

    // 将RDD转换为map
    val pageActionMap: Map[Long, Long] = pageActionRDD.collect().toMap

    // 将计算结果A / B,获取转换率
    reduceZipToSumRDD.foreach {
      case (pageFlow, sum) => {
        var pageId = pageFlow.split("_")(0).toLong
        var rate = (sum.toDouble / pageActionMap(pageId) * 100).toInt

        println((pageFlow, rate))
      }
    }

    // 将转换率通过JDBC保存到Mysql中
    spark.stop()
  }
}