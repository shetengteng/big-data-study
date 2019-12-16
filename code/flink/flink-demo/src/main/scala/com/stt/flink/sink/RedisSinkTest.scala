package com.stt.flink.sink

import com.stt.flink.source.SensorEntity
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从自定义的集合中读取数据
    val sensorStream: DataStream[SensorEntity] = env.fromCollection(List(
      SensorEntity("s01", 1547718199, 35.80018327300259),
      SensorEntity("s02", 1547718201, 15.402984393403084),
      SensorEntity("s03", 1547718202, 6.720945201171228),
      SensorEntity("s04", 1547718205, 38.101067604893444)
    ))

    // 定义redis配置
    val config: FlinkJedisPoolConfig =
      new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()

    // 向kafka发送消息
    sensorStream.addSink(new RedisSink[SensorEntity](config,new MyRedisMapper))

    env.execute("RedisSinkTest")
  }

  class MyRedisMapper  extends RedisMapper[SensorEntity]{

    // 定义执行的命令
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET,"sensor_key")
    }

    // 定义value
    override def getValueFromData(t: SensorEntity): String = {
      t.temperature.toString
    }
    // 定义key
    override def getKeyFromData(t: SensorEntity): String = {
      t.id
    }
  }

}