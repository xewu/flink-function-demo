package com.erica.flink.timebasedwindow

import com.erica.flink.source.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

object UserDefineFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // read from file
    val streamFromFile: DataStream[String] = env.readTextFile("/Users/tolo7e/Gits/Flink-Jobs/flink-demo-jobs/src/main/scala/com/tutorial/source/sensor_data.txt")

    // DataStream_1:
    val dataStream: DataStream[SensorReading] = streamFromFile.map( (data) => {
      val dataArray = data.split(", ")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    // UDF filter function
    dataStream.filter(new MyFilter()).print()
    dataStream.filter(data => data.id.startsWith("sensor_1"))
    dataStream.filter(_.id.startsWith("sensor_1"))

    // Rich Functions - RichMapFunction, RichFlatMapFunction, RichFilterFunction



    env.execute("UDF")



  }

  class MyFilter() extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean = {
      value.id.startsWith("sensor_1")
    }
  }
}

class MyMapper() extends RichMapFunction[SensorReading, String] {
  override def map(value: SensorReading): String = {
    "flink"
  }

//  override def open(parameters: Configuration): Unit = super.open(parameters)
}
