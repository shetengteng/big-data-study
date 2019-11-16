package com.stt.spark.rdd

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
    通过RDD访问MySQL数据
  */
object Ch06_JDBC {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Application")

        // 构建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://linux1:3306/company"
        val userName = "root"
        val passWd = "123456"

        val sql = "select * from staff where id >= ? and id <= ?"

        // 创建MySQLRDD
        val jdbcrdd = new JdbcRDD(
            sc,
            () => {
                Class.forName(driver)
                DriverManager.getConnection(url, userName, passWd)
            },
            sql,
            1,
            10,
            1,
            result => {
                (result.getInt(1), result.getString(2), result.getInt(3))
            }
        )
        jdbcrdd.foreach(t=>{
            println(t._1 + "-" + t._2 + "-" + t._3)
        })

        // spark中数据库数据的插入需要自己实现jdbc功能
        sc.makeRDD(Array(1,2,3,4,5)).foreach(x=>{

        })

        // 释放资源
        sc.stop()
    }
}
