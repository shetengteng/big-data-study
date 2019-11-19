package com.stt.spark.mall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.stt.spark.mall.common.{CommonUtil, ConfigurationUtil}
import com.stt.spark.mall.model.{CategoryTop10, UserVisitAction}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

// 需求1 获取点击，下单，支付数量top10
object Req01CategoryTop10App {

  def main(args: Array[String]): Unit = {
    // 使用sparkSql获取数据
    val conf = new SparkConf().setMaster("local[*]").setAppName("Req01")

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    // 引入隐式转换
    import spark.implicits._

    spark.sql("use "+ConfigurationUtil.getValueByKey("hive.database"))

    var sql = new StringBuilder("select * from user_visit_action where 1 = 1")

    var startDate = ConfigurationUtil.getCondValue("startDate")
    var endDate = ConfigurationUtil.getCondValue("endDate")

    if(!CommonUtil.isEmpty(startDate)){
      sql.append(" and action_time >='").append(startDate).append("'")
    }

    if(!CommonUtil.isEmpty(endDate)){
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
        if(actionLog.click_category_id != -1){
          categoryCountAccumulator.add(actionLog.click_category_id+"_click")
        }else if(actionLog.order_category_ids != null){
          actionLog.order_category_ids.split(",").foreach(id=>{
            categoryCountAccumulator.add(id+"_order")
          })
        }else if(actionLog.pay_category_ids != null){
          actionLog.pay_category_ids.split(",").foreach(id=>{
            categoryCountAccumulator.add(id+"_pay")
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

    println(mapResultData.toList.size)

    // 进行排序
    val top10Data: List[CategoryTop10] = mapResultData.toList.sortWith {
      case (left, right) => {
        // 返回数据，不要使用卫语句，可能会有异常情况
        if (left.clickCount == right.clickCount) {
          if (left.orderCount == right.orderCount) {
              left.payCount > right.payCount
          }else{
            left.orderCount > right.orderCount
          }
        }else{
          left.clickCount > right.clickCount
        }
      }
    }.take(10)

    // 连接数据库,将数据库插入表中
    val driverClass = ConfigurationUtil.getValueByKey("jdbc.driver.class")
    val url = ConfigurationUtil.getValueByKey("jdbc.url")
    val user = ConfigurationUtil.getValueByKey("jdbc.user")
    val password = ConfigurationUtil.getValueByKey("jdbc.password")


    var statement: PreparedStatement  = null
    var connection: Connection = null
    try{
      // 加载驱动
      Class.forName(driverClass)

      val insertSql = "insert into category_top10 values ( ?, ?, ?, ?, ?)"

      connection = DriverManager.getConnection(url,user,password)
      statement= connection.prepareStatement(insertSql)

      for(data <- top10Data){
        statement.setObject(1,data.taskId)
        statement.setObject(2,data.categoryId)
        statement.setObject(3,data.clickCount)
        statement.setObject(4,data.orderCount)
        statement.setObject(5,data.payCount)
        statement.executeUpdate()
      }

    }catch {
      case e:Exception=> e.printStackTrace()
    }finally {
      statement.close()
      connection.close()
    }
    spark.stop()
  }
}

/**
  * 参数类型： IN , OUT
  * category 品类总和累加器(category_click,sumClick) (category_order,sumOrder),(category_pay,numPay)
  */
class CategoryCountAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]] {

  var map = new mutable.HashMap[String,Long]()

  override def isZero: Boolean = {
    map.isEmpty
  }

  // 复制累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
     new CategoryCountAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  // 累加器增加数据
  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v,0L) + 1L
  }

  // 多个累加器合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    // 使用左折叠将2个HashMap做合并
//    map = map.foldLeft(other.value)((otherMap,item)=>{
//      if(otherMap.contains(item._1)){
//        otherMap(item._1) += item._2
//      }else{
//        otherMap(item._1) = item._2
//      }
//      otherMap
//    })

    map = map.foldLeft(other.value){
      case (otherMap,(category,count)) => {
        otherMap(category) = otherMap.getOrElse(category,0L) + count
        otherMap
      }
    }
  }

  override def value: mutable.HashMap[String, Long] = {
    map
  }
}