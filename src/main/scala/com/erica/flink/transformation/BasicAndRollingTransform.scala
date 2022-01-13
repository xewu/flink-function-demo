package com.erica.flink.transformation

import com.erica.flink.source.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

object BasicAndRollingTransform {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // global setting parallelism:
    env.setParallelism(1)
    val sourceFilePath = "/Users/tolo7e/Gits/flink-function-demo/src/main/scala/com/erica/flink/source/sensor_data.txt"
    val streamFromFile: DataStream[String] = env.readTextFile(sourceFilePath)
    // map(String => SensorReading)
    val dataStream: DataStream[SensorReading] = streamFromFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //      .keyBy(0)
      //      .sum(2)
      .keyBy("id")
      //      .sum("temperature")
      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))


    // dataStream.keyBy(0): KeyedStream[SensorReading, Tuple]
    // dataStream.keyBy(0).sum(2): DataStream[SensorReading]
    //    val keyed: DataStream[SensorReading] = dataStream.keyBy(0).sum(2)

    //    val dataStream: KeyedStream[SensorReading, String] = DataStream.keyBy(_.id)
    //    val streamTmp = dataStream.keyBy(_.id).sum("temperature")

    // setParallelism(1) for "print()" only
    //    dataStream.print().setParallelism(1)
    dataStream.print()

    env.execute("transform test")
  }

}
