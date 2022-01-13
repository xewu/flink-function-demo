package com.erica.flink.timebasedwindow

import com.erica.flink.source.SensorReading
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment, createTypeInformation}

object MultiStreamTransform {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // split: Only split in logic, not physical
    env.setParallelism(1)

    // read from file
    val streamFromFile: DataStream[String] = env.readTextFile("/Users/tolo7e/Gits/Flink-Jobs/flink-demo-jobs/src/main/scala/com/tutorial/source/sensor_data.txt")

    // DataStream_1:
    val dataStream: DataStream[SensorReading] = streamFromFile.map( (data) => {
      val dataArray = data.split(", ")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    // DataStream2
    val warning: DataStream[String] = dataStream.map((sensorData => {
      if (sensorData.temperature > 35) "warning"
      else "health"
    })

    )

    // ConnectedStream vs CoMap.CoFlatMap
    // output data structure can be diff, but limited to 2 streams.
    val connectedStream: ConnectedStreams[String, SensorReading] = warning.connect(dataStream)

    val coMapDataStream = connectedStream.map(
      warningData => warningData + "... ...",
      dataStreamData => (dataStreamData.id, "checked")
    )

    // union merge multiple stream, data structure should be SAME

    val datastream_p1 = dataStream.filter(_.temperature > 35)
    val dataStream_p2 = dataStream.filter(_.temperature <= 35)

    val unionStream: DataStream[SensorReading] = datastream_p1.union(dataStream_p2)

    coMapDataStream.print()
    env.execute()


    // "split" has been deprecated:
//    val splitStream = streamFromFile.split(data -> {
//      if (data.temperature > 30) Seq("high")
//      else ("low")
//    })
//    val high = splitStream.select("high")
//    val low = splitStream.select("low")
//    val all = splitStream.select("high", "low")

  }
}
