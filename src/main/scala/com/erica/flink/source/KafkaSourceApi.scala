package com.erica.flink.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object KafkaSourceApi {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val flinkKafkaConsumer = new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties)
    val streamKafka = env.addSource(flinkKafkaConsumer)

    streamKafka.print("kafka sensor stream: ").setParallelism(1)
    env.execute("kafka sensor")
  }
}
