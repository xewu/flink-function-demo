package com.erica.flink.timebasedwindow

import com.erica.flink.source.SensorReading
import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

// watermark delay 6000L ms
// watermark interval set 100L
// tumbling window 10s

object WindowEventTimeTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(100L)
//    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime)

//    val filePath = "/Users/tolo7e/Gits/Flink-Jobs/flink-demo-jobs/src/main/scala/com/tutorial/api/source/sensor_data.txt"
//        val stream = env.readTextFile(filePath)
    val stream = env.socketTextStream("localhost", 7777)

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    val withTimestampsAndWatermarks: DataStream[SensorReading] = dataStream
      .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks)
//      .assignAscendingTimestamps(_.timestamp * 1000) //ms

    val minTempPerWindowStream = withTimestampsAndWatermarks
      .map( data => (data.id, data.temperature))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce( (data1, data2) => (data1._1, data1._2.min(data2._2)))

    minTempPerWindowStream.print("min temp")
    dataStream.print("input data")

    env.execute("event time test")
  }
}

class MyTimestampsAndWatermarks() extends WatermarkStrategy[SensorReading] {
  override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[SensorReading] = {
    new MyTimestampsAssigner()
  }

  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[SensorReading] = {
    new BoundedOutOfOrdernessWatermarks[SensorReading](Duration.ofSeconds(5))
  }
}

// self-defined BoundedOutOfOrdernessWatermarksGenerator
class MyBoundedOutOfOrdernessWatermarksGenerator extends WatermarkGenerator[SensorReading] {
  val maxOutOfOrderness = 60000L // bound
  var currentMaxTimestamp: Long = Long.MinValue

  override def onEvent(t: SensorReading, l: Long, watermarkOutput: WatermarkOutput): Unit = {
    currentMaxTimestamp = currentMaxTimestamp.max(t.timestamp * 100)
  }

  override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
    watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness))
  }
}

class MyTimestampsAssigner extends TimestampAssigner[SensorReading] {
  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    t.timestamp * 1000
  }
}





