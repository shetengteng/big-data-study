package com.stt.flink.T04_LoginMonitor

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable


object LoginFailByCEP {

  val n : Int = 2

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[LoginEvent] = env
      .readTextFile(this.getClass.getClassLoader.getResource("LoginLog.csv").getPath)
      .map(data => {
        val fields = data.split(",")
        LoginEvent(fields(0).trim.toLong, fields(1).trim, fields(2).trim, fields(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
          override def extractTimestamp(element: LoginEvent): Long = {
            element.timestamp * 1000L
          }
        }
      )

    val pattern: Pattern[LoginEvent, LoginEvent] =
      Pattern.begin[LoginEvent]("start-stage").where(_.eventType == "fail")
        .times(n).consecutive().within(Time.seconds(2))

    val keyedStream: KeyedStream[LoginEvent, Long] = dataStream.keyBy(_.userId)

    val warningDataStream: DataStream[Warning] = CEP.pattern(keyedStream,pattern).select(new FailedLoginProcess())

    warningDataStream.print("re")

    env.execute("LoginFail2")

  }

  class FailedLoginProcess() extends PatternSelectFunction[LoginEvent,Warning]{
    override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
      import scala.collection.JavaConverters._
      val events: mutable.Buffer[LoginEvent] =
        map.get("start-stage").asScala.sortWith(_.timestamp < _.timestamp)
      Warning(
        events.head.userId,
        events.head.timestamp,
        events.last.timestamp,
        "login failed over" + n + " times"
      )
    }
  }

}
