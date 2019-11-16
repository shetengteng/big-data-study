package com.stt.spark.rdd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
    通过RDD访问Hbase数据
  */
object Ch07_Hbase {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Application")

        // 构建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val configuration: Configuration = HBaseConfiguration.create()
        //configuration.set(TableInputFormat.INPUT_TABLE, "student")

        // 构建RDD 读取HBase
//        val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
//            configuration,
//            classOf[TableInputFormat],
//            classOf[ImmutableBytesWritable],
//            classOf[Result])
//        hbaseRDD.foreach(t=>{
//            t._2.rawCells().foreach(cell=>{
//                println(Bytes.toString(CellUtil.cloneValue(cell)))
//            })
//        })

        // 插入Hbase数据
        val rdd = sc.makeRDD(Array(("1002", "zhangsan")))

        val hbaseRDD: RDD[(ImmutableBytesWritable, Put)] = rdd.map(t => {
            val rowkey = t._1
            val put = new Put(Bytes.toBytes(rowkey))
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(t._2))

            (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
        })

        val jobConf = new JobConf(configuration)
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "student")

        hbaseRDD.saveAsHadoopDataset(jobConf)

        // 释放资源
        sc.stop()
    }
}
