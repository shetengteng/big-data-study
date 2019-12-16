package com.stt.flink.sink

import java.util

import com.stt.flink.source.SensorEntity
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object ESSinkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从自定义的集合中读取数据
    val sensorStream: DataStream[SensorEntity] = env.fromCollection(List(
      SensorEntity("s01", 1547718199, 35.80018327300259),
      SensorEntity("s02", 1547718201, 15.402984393403084),
      SensorEntity("s03", 1547718202, 6.720945201171228),
      SensorEntity("s04", 1547718205, 38.101067604893444)
    ))

    sensorStream.addSink(esSinkBuilder.build()).setParallelism(1)

    env.execute("ESSinkTest")
  }

  val esSinkBuilder = new ElasticsearchSink.Builder[SensorEntity](
    new util.ArrayList[HttpHost](){{
      add(new HttpHost("hadoop102",9200))
    }},
    new ElasticsearchSinkFunction[SensorEntity] {
      override def process(t: SensorEntity,
                           ctx: RuntimeContext,
                           requestIndexer: RequestIndexer): Unit = {
        println("data:"+t)

        requestIndexer.add(
          Requests.indexRequest()
            .index("sensor")
            .`type`("sensorData")
            .source(new util.HashMap[String,String](){{
              put("id",t.id)
              put("temperature",t.temperature.toString)
              put("timestamp",t.timestamp.toString)
            }})
        )

        println("success")
      }
    }
  )
}