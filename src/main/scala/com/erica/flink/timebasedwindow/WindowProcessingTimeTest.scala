package com.erica.flink.timebasedwindow

import com.erica.flink.source.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowProcessingTimeTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val sourceFilePath = "/Users/tolo7e/Gits/flink-function-demo/src/main/scala/com/erica/flink/source/sensor_data.txt"
//    val stream = env.readTextFile(sourceFilePath)
    val stream = env.socketTextStream("localhost", 7777)

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    val minTempPerWindow: DataStream[(String, Double)] = dataStream
      .map( data => (data.id, data.temperature))
      .keyBy(_._1)
//      .timeWindow(Time.seconds(10)) // time window
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)) )// time window
//      .window(TumblingEventTimeWindows.of(Time.seconds(10)) )// time window
      .reduce( (data1, data2) => (data1._1, data1._2.min(data2._2))) // Reduce incremental

    minTempPerWindow.print("min temp")
    dataStream.print("input data")

    env.execute("window test")
  }
}
