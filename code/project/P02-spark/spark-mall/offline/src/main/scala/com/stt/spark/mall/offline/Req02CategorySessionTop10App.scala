package com.stt.spark.mall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.stt.spark.mall.common.{CommonUtil, ConfigurationUtil}
import com.stt.spark.mall.model.{CategoryTop10, UserVisitAction}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.{immutable, mutable}

// 需求2
object Req02CategorySessionTop10App {

  def main(args: Array[String]): Unit = {
    // 使用sparkSql获取数据
    val conf = new SparkConf().setMaster("local[*]").setAppName("Req02")

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

    println(actionRDD.count())

    // 对数据进行统计分析
    // 将品类聚合成不同属性的数据(category,sumClick),(category,sumOrder),(category,sumPay)
    // 通过join的方式组合在一起，效率低下，不推荐使用
    // 使用累加器聚合数据
    val categoryCountAccumulator = new CategoryCountAccumulator

    // 注册累加器
    spark.sparkContext.register(categoryCountAccumulator)

    actionRDD.foreach(
      actionLog => {
        if (actionLog.click_category_id != -1) {
          categoryCountAccumulator.add(actionLog.click_category_id + "_click")
        } else if (actionLog.order_category_ids != null) {
          actionLog.order_category_ids.split(",").foreach(id => {
            categoryCountAccumulator.add(id + "_order")
          })
        } else if (actionLog.pay_category_ids != null) {
          actionLog.pay_category_ids.split(",").foreach(id => {
            categoryCountAccumulator.add(id + "_pay")
          })
        }
      }
    )
    // 获取累加器的结果
    // (category_click, sumClick)(category_order, sumOrder)(category_pay, sumPay)
    val categorySumMap: mutable.HashMap[String, Long] = categoryCountAccumulator.value

    // 将聚合的数据融合在一起(category,(sumClick,sumOrder,sumPay))
    val statMap: Map[String, mutable.HashMap[String, Long]] =
      categorySumMap.groupBy {
        case (k, v) => {
          k.split("_")(0) // 结果Map的key是categoryId
        }
      }

    val taskId = UUID.randomUUID().toString

    val mapResultData: immutable.Iterable[CategoryTop10] = statMap.map {
      case (categoryId, map) => {
        CategoryTop10(
          taskId,
          categoryId,
          map.getOrElse(categoryId + "_click", 0L),
          map.getOrElse(categoryId + "_order", 0L),
          map.getOrElse(categoryId + "_pay", 0L)
        )
      }
    }


    // 进行排序
    val top10Data: List[CategoryTop10] = mapResultData.toList.sortWith {
      case (left, right) => {
        // 返回数据，不要使用卫语句，可能会有异常情况
        if (left.clickCount == right.clickCount) {
          if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            left.orderCount > right.orderCount
          }
        } else {
          left.clickCount > right.clickCount
        }
      }
    }.take(10)


    // TODO 如果在filter算子中使用top10Data,需要对top10Data进行广播变量传递
    val ids: Set[String] = top10Data.map(_.categoryId).toSet

    //    - 基于需求1获取热门品类的Top10的数据（需求1）
    //    - 将日志数据进行过滤，保留Top10品类的数据

    val filterRDD: RDD[UserVisitAction] = actionRDD.filter(log => {
      ids.contains(log.click_category_id.toString)
    })
    //    - 将日志数据进行结构转换->(  categoryId + sessionId, 1L )
    //    - 将转换结构后的数据进行聚合：(  categoryId + sessionId, 1L )-> (  categoryId + sessionId, sum)
    //    - 将聚合的数据进行机构转换：(  categoryId + sessionId, sum)->( categoryId, (sessionId, sum) )
    //    - 将转换结构后的数据进行分组：( categoryId, (sessionId, sum) )->( categoryId, List(sessionId, sum) )
    val groupRDD: RDD[(String, Iterable[(String, Long)])] = filterRDD
      .map(log => {
        (log.click_category_id + "_" + log.session_id, 1L)
      }).reduceByKey(_ + _)
      .map {
        case (categoryId_sessionId, sum) => {
          var ids = categoryId_sessionId.split("_")
          (ids(0), (ids(1), sum))
        }
      }.groupByKey()

    val resultData: RDD[(String, List[(String, Long)])] = groupRDD.mapValues(datas =>
      datas.toList.sortWith {
        case (left, right) => left._2 > right._2
      }.take(10))

    //    - 将分组后的数据进行排序（倒序），取前10条
    //    - 将结构通过JDBC保存到Mysql中


    // 连接数据库,将数据库插入表中
    val driverClass = ConfigurationUtil.getValueByKey("jdbc.driver.class")
    val url = ConfigurationUtil.getValueByKey("jdbc.url")
    val user = ConfigurationUtil.getValueByKey("jdbc.user")
    val password = ConfigurationUtil.getValueByKey("jdbc.password")

    // 该foreach是算子，在Executor中执行，statement 需要序列化
    // 此处优化可使用foreachPartition 进行数据连接处理
    resultData.foreach {
      case (categoryId, list) => {

        // 在此处statement 进行初始化
        var statement: PreparedStatement = null
        var connection: Connection = null
        try {
          // 加载驱动
          Class.forName(driverClass)

          val insertSql = "insert into category_top10_session_count values ( ?, ?, ?, ?)"

          connection = DriverManager.getConnection(url, user, password)
          statement = connection.prepareStatement(insertSql)

          list.foreach {
            case (sessionId, sum) => {
              statement.setObject(1, taskId)
              statement.setObject(2, categoryId)
              statement.setObject(3, sessionId)
              statement.setObject(4, sum)

              statement.executeUpdate()
            }
          }

        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          statement.close()
          connection.close()
        }
      }
    }

    spark.stop()
  }
}