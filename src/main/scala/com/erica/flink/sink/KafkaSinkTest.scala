package com.erica.flink.sink

import com.erica.flink.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import java.util.Properties

/*
Kafka source/sink
Cassandra sink
Kinesis source/sink
ES sink
HDFS sink
RabbitMQ source/sink
NiFi source/sink
Twitter Streaming API source

Apache Bahir
ActiveMQ source/sink
Flume sink
Redis sink
Akka sink
Netty source
 */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // source
//    val path = "/Users/tolo7e/Gits/Flink-Jobs/flink-demo-jobs/src/main/scala/com/tutorial/api/source/sensor_data.txt"
//    val inputStream = env.readTextFile(path)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val flinkKafkaConsumer = new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties)
    val inputStream = env.addSource(flinkKafkaConsumer)

    // transform
    val dataStream = inputStream.map(
      data => {
        val dataArray = data.split(",")
        // transform to string => serialize output
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
      }
    )

    // sink
    val flinkKafkaProducer = new FlinkKafkaProducer("localhost:9092", "sinkTest", new SimpleStringSchema())
    dataStream.addSink(flinkKafkaProducer)
    dataStream.print()
    env.execute()
  }
}
