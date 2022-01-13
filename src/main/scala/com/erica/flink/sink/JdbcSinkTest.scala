package com.erica.flink.sink

import com.erica.flink.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

import java.sql.{Connection, DriverManager, PreparedStatement}

object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val sourceFilePath = "/Users/tolo7e/Gits/flink-function-demo/src/main/scala/com/erica/flink/source/sensor_data.txt"
    val inputStream = env.readTextFile(sourceFilePath)

    // transform
    val dataStream = inputStream.map(
      data => {
        val dataArray = data.split(",")
        // transform to string => serialize output
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    )

    dataStream.addSink(new MyJdbcSink())
    env.execute("JDBC sink test")

  }
}

class MyJdbcSink extends RichSinkFunction[SensorReading]{
  // pre compile:
  var conn: Connection = _
  var insertStatement: PreparedStatement = _
  var updateStatement: PreparedStatement = _

  // initial, create connection, pre compile
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")

    insertStatement = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?, ?)")
    updateStatement = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
  }

  // call and exet sql
  override def invoke(value: SensorReading, context: SinkFunction.Context): Unit = {
    // run update
    updateStatement.setDouble(1, value.temperature)
    updateStatement.setString(2, value.id)
    updateStatement.execute()
    // if didn'f find it, run insert
    if (updateStatement.getUpdateCount == 0) {
      insertStatement.setString(1, value.id)
      insertStatement.setDouble(2, value.temperature)
      insertStatement.execute()
    } }
  override def close(): Unit = {
    insertStatement.close()
    updateStatement.close()
    conn.close()
  }
}
