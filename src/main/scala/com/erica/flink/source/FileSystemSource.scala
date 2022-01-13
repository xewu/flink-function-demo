package com.erica.flink.source

import org.apache.flink.streaming.api.scala._


case class SensorReading(id: String, timestamp: Long, temperature: Double)

object Sensor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))

    val sourceFilePath = "/Users/tolo7e/Gits/flink-function-demo/src/main/scala/com/erica/flink/source/sensor_data.txt"
    val inputStream2 = env.readTextFile(sourceFilePath)

    val inputStream3 = env.fromElements(1, 2.0, "string", "time")

    inputStream.print("input stream: ").setParallelism(1)
    inputStream2.print("input stream2: ").setParallelism(1)
    inputStream3.print("input stream3: ").setParallelism(1)

    env.execute("Sensor Job")
  }

}
