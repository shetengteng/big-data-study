package com.stt.spark.mall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.stt.spark.mall.common.{CommonUtil, ConfigurationUtil}
import com.stt.spark.mall.model.{CategoryTop10, UserVisitAction}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 需求1 ： 获取点击、下单和支付数量排名前 10 的品类
  */
object Req1CategoryTop10Application {

    def main(args: Array[String]): Unit = {

        //4.1 从Hive中获取数据
        //    4.1.1 使用SparkSQL来获取数据

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10Application")

        // 构建SparkSql环境
        val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        // 引入隐式转换
        import spark.implicits._

        spark.sql("use " + ConfigurationUtil.getValueByKey("hive.database"))

        var sql = "select * from user_visit_action where 1 = 1 "

        val sqlBuilder = new StringBuilder(sql)

        val startDate = ConfigurationUtil.getCondValue("startDate")
        val endDate = ConfigurationUtil.getCondValue("endDate")

        if ( !CommonUtil.isEmpty(startDate) ) {
            sqlBuilder.append(" and action_time >= '").append(startDate).append("'")
        }
        if ( !CommonUtil.isEmpty(endDate) ) {
            sqlBuilder.append(" and action_time <= '").append(endDate).append("'")
        }

        val actionLogDF: DataFrame = spark.sql(sqlBuilder.toString)
        //    4.1.2将获取的数据转换为RDD
        val actionLogRDD: RDD[UserVisitAction] = actionLogDF.as[UserVisitAction].rdd

        //println(actionLogRDD.count())
        //    4.2 对数据进行统计分析
        //    4.2.1 将品类聚合成不同的属性数据（category, sumClick）,(category, sumOrder),(Category, sumPay)

        // 使用累加器聚合数据
        // 声明累加器
        val categoryCountAccumulator = new CategoryCountAccumulator
        // 注册累加器
        spark.sparkContext.register(categoryCountAccumulator)

        actionLogRDD.foreach(
            actionLog=>{
                if (actionLog.click_category_id != -1) {
                    // 10_click
                    categoryCountAccumulator.add(actionLog.click_category_id + "_click")
                } else if (actionLog.order_category_ids != null) {
                    val ids = actionLog.order_category_ids.split(",")
                    // 10_order
                    ids.foreach(id=>{
                        categoryCountAccumulator.add(id + "_order")
                    })
                } else if (actionLog.pay_category_ids != null) {
                    val ids = actionLog.pay_category_ids.split(",")
                    // 10_pay
                    ids.foreach(id=>{
                        categoryCountAccumulator.add(id + "_pay")
                    })
                }

            }
        )

        // (category_click, sumClick)(category_order, sumOrder)(category_pay, sumPay)
        val categorySumMap: mutable.HashMap[String, Long] = categoryCountAccumulator.value

        //4.2.2 将聚合的数据融合在一起（category, (sumClick, sumOrder, sumPay)）

        val statMap: Map[String, mutable.HashMap[String, Long]] = categorySumMap.groupBy {
            case (k, v) => {
                k.split("_")(0)
            }
        }

        val taskId = UUID.randomUUID().toString

        val listData: List[CategoryTop10] = statMap.map {
            case (categoryId, map) => {
                CategoryTop10(
                    taskId,
                    categoryId,
                    map.getOrElse(categoryId + "_click", 0L),
                    map.getOrElse(categoryId + "_order", 0L),
                    map.getOrElse(categoryId + "_pay", 0L)
                )
            }
        }.toList

        //4.2.3 将聚合的数据根据要求进行倒序排列，取前10条
        val top10Data: List[CategoryTop10] = listData.sortWith {
            case (left, right) => {

                if (left.clickCount < right.clickCount) {
                    false
                } else if (left.clickCount == right.clickCount) {
                    if (left.orderCount < right.orderCount) {
                        false
                    } else if (left.orderCount == right.orderCount) {
                        left.payCount > right.payCount
                    } else {
                        true
                    }
                } else {
                    true
                }
            }
        }.take(10)

        //4.3 将分析结果保存到MySQL中
        //    4.3.1 将统计结果使用JDBC存储到Mysql中

        val driverClass = ConfigurationUtil.getValueByKey("jdbc.driver.class")
        val url = ConfigurationUtil.getValueByKey("jdbc.url")
        var user = ConfigurationUtil.getValueByKey("jdbc.user")
        var password = ConfigurationUtil.getValueByKey("jdbc.password")

        Class.forName(driverClass)

        val connection: Connection = DriverManager.getConnection(url, user, password)

        val insertSQL = "insert into category_top10 values ( ?, ?, ?, ?, ? )"

        val pstat: PreparedStatement = connection.prepareStatement(insertSQL)

        /*
        for (data <- top10Data) {
            pstat.setObject(1, data.taskId)
            pstat.setObject(2, data.categoryId)
            pstat.setObject(3, data.clickCount)
            pstat.setObject(4, data.orderCount)
            pstat.setObject(5, data.payCount)

            pstat.executeUpdate()
        }
        */
        top10Data.foreach(data=>{
            pstat.setObject(1, data.taskId)
            pstat.setObject(2, data.categoryId)
            pstat.setObject(3, data.clickCount)
            pstat.setObject(4, data.orderCount)
            pstat.setObject(5, data.payCount)

            pstat.executeUpdate()
        })

        pstat.close()
        connection.close()

        // 释放资源
        spark.stop()
    }

}

/**
  * 品类总和累加器 (category_click, sumClick)(category_order, sumOrder)(category_pay, sumPay)
  */
class CategoryCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

    var map = new mutable.HashMap[String, Long]()

    // 是否为初始状态
    override def isZero: Boolean = {
        map.isEmpty
    }

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
         new CategoryCountAccumulator
    }

    // 重置累加器
    override def reset(): Unit = {
        map.clear()
    }

    // 增加数据
    override def add(v: String): Unit = {
        map(v) = map.getOrElse(v, 0L) + 1L
    }

    // 合并数据
    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
        map = map.foldLeft(other.value){
            case ( xmap, ( category, count ) ) => {
                xmap(category) = xmap.getOrElse(category, 0L) + count
                xmap
            }
        }
    }

    // 返回值
    override def value: mutable.HashMap[String, Long] = {
        map
    }
}