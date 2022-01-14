package com.erica.flink.processfunction

import com.erica.flink.source.SensorReading
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

import java.time.Duration

// temperature goes up in 2 secs

object ProcessFunctionTimerAlertTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.socketTextStream("localhost", 7777)

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks)
    //      .assignAscendingTimestamps(_.timestamp * 1000) //ms

    val processedStream = dataStream.keyBy(_.id)
      .process(new TemIncreAlert())

    dataStream.print("input data")
    processedStream.print("sensor alert")
    env.execute("event time test")
  }
}

// [key = id, input, output = alert message]
class TemIncreAlert extends KeyedProcessFunction[String, SensorReading, String] {
  // stateful programming
  // define a state, to store previous event temperature
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("lastTemp", classOf[Double]) )
  // define a state, to store timer timestamp for reference/delete later, default no setting value is 0
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("currentTimer", classOf[Long]) )

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    // compare with previous temp, data come -> create timer 1s -> if temp go down, delete it, else trigger alert:
    // -- get previous temperature
    val preTemp = lastTemp.value()
    // -- update lastTemp
    lastTemp.update(i.temperature)
    // -- if temperature goes up && no current timer, register a timer

    val currTimerTs = currentTimer.value()
    if (preTemp != 0.0 && i.temperature > preTemp && currTimerTs == 0) {
      // get current (system) time
      val timerTs = context.timerService().currentProcessingTime() + 1000L
      context.timerService().registerProcessingTimeTimer(timerTs) // timestamp = system time + timer 1s
      currentTimer.update(timerTs)
    } else if ( preTemp == 0.0 || preTemp > i.temperature) {
      // timer deleting + state clear
      context.timerService().deleteProcessingTimeTimer(currTimerTs)
      currentTimer.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // output alert
    out.collect( "sensor_id: " + ctx.getCurrentKey + " Thermo trap occurred ")
    currentTimer.clear()
  }
}

class MyTimestampsAndWatermarks() extends WatermarkStrategy[SensorReading] {
  override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[SensorReading] = {
    new MyTimestampsAssignerI()
  }

  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[SensorReading] = {
    new BoundedOutOfOrdernessWatermarks[SensorReading](Duration.ofSeconds(5))
  }
}

// self-defined BoundedOutOfOrdernessWatermarksGenerator
//class MyBoundedOutOfOrdernessWatermarksGenerator extends WatermarkGenerator[SensorReading] {
//  val maxOutOfOrderness = 60000L // bound
//  var currentMaxTimestamp: Long = Long.MinValue
//
//  override def onEvent(t: SensorReading, l: Long, watermarkOutput: WatermarkOutput): Unit = {
//    currentMaxTimestamp = currentMaxTimestamp.max(t.timestamp * 100)
//  }
//
//  override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
//    watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness))
//  }
//}

class MyTimestampsAssignerI extends TimestampAssigner[SensorReading] {
  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    t.timestamp * 1000
  }
}