package com.stt.flink.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.stt.flink.source.SensorEntity
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JDBCSinkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从自定义的集合中读取数据
    val sensorStream: DataStream[SensorEntity] = env.fromCollection(List(
      SensorEntity("s01", 1547718199, 35.80018327300259),
      SensorEntity("s02", 1547718201, 15.402984393403084),
      SensorEntity("s03", 1547718202, 6.720945201171228),
      SensorEntity("s04", 1547718205, 38.101067604893444)
    ))

    sensorStream.addSink(new MyJdbcSink())

    env.execute("JDBCSinkTest")
  }

  class MyJdbcSink() extends RichSinkFunction[SensorEntity]{
    // 定义sql连接器，预编译器
    var conn: Connection = _
    var insertStatement : PreparedStatement = _
    var updateStatement : PreparedStatement = _

    // 初始化 创建连接和预编译
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","123456")
      insertStatement = conn.prepareStatement("INSERT INTO temperature (sensor, temper) VALUES (?,?)")
      updateStatement = conn.prepareStatement("UPDATE temperature SET temper = ? WHERE sensor = ?")
    }

    // 调用连接，执行sql
    override def invoke(value: SensorEntity, context: SinkFunction.Context[_]): Unit = {

      updateStatement.setDouble(1,value.temperature)
      updateStatement.setString(2,value.id)
      updateStatement.execute()
      // 先执行更新操作，如果成功，则不进行插入
      if(updateStatement.getUpdateCount == 0){
        insertStatement.setString(1,value.id)
        insertStatement.setDouble(2,value.temperature)
        insertStatement.execute()
      }
    }

    override def close(): Unit = {
      conn.close()
      insertStatement.close()
      updateStatement.close()
    }
  }


}