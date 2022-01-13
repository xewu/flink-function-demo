package com.erica.flink.sink

import com.erica.flink.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
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

    // sink
    val conf: FlinkJedisConfigBase = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    dataStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))

    env.execute("Redis Sink Test")

  }
}

class MyRedisMapper() extends RedisMapper[SensorReading] {
  // data store in redis
  override def getCommandDescription: RedisCommandDescription = {
    // sensor vs temperature => HSET key field value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  // value stored in redis
  override def getValueFromData(t: SensorReading): String = {
    t.temperature.toString
  }

  // key stored in redis
  override def getKeyFromData(t: SensorReading): String = {
    t.id
  }
}
