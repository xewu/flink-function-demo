package com.erica.flink.processfunction

import com.erica.flink.source.SensorReading
import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

import scala.tools.fusesource_embedded.jansi.AnsiConsole.out

object SideOutputTest {
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
      .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarkStrategyII)
    //      .assignAscendingTimestamps(_.timestamp * 1000) //ms

    /**
     * -- high temperature vs low temperature
     */
    val processedStream = dataStream.process( new FreezingAlert() )


    processedStream.print("processed data")
    processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print("alert data")
    env.execute("event time test")
  }
}

// < I, O> type
// if < 32, output alert message to side output stream
class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading] {
  lazy val alertOutput: OutputTag[String] = new OutputTag[String]("freezing alert")

  override def processElement(r: SensorReading,
                              ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                              out: Collector[SensorReading]):Unit = {
    if (r.temperature < 32.0) {
      ctx.output(alertOutput, "freezing alert for " + r.id)
    } else {
    out.collect(r)
    }
  }
}



//class MyProcess() extends KeyedProcessFunction[String, SensorReading, String] {
//  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
//    context.timestamp()
//    context.timerService().registerEventTimeTimer(2000L)
//  }
//}

class MyTimestampsAndWatermarkStrategyII() extends WatermarkStrategy[SensorReading] {
  override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[SensorReading] = {
    new MyTimestampsAssignerII()
  }

  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[SensorReading] = {
//    new BoundedOutOfOrdernessWatermarks[SensorReading](Duration.ofSeconds(5))
    new MyBoundedOutOfOrdernessWatermarksGeneratorII()
  }
}

// self-defined BoundedOutOfOrdernessWatermarksGenerator
class MyBoundedOutOfOrdernessWatermarksGeneratorII extends WatermarkGenerator[SensorReading] {
  val maxOutOfOrderness = 60000L // bound
  var currentMaxTimestamp: Long = Long.MinValue

  override def onEvent(t: SensorReading, l: Long, watermarkOutput: WatermarkOutput): Unit = {
    currentMaxTimestamp = currentMaxTimestamp.max(t.timestamp * 100)
  }

  override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
    watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness))
  }
}

class MyTimestampsAssignerII extends TimestampAssigner[SensorReading] {
  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    t.timestamp * 1000
  }
}