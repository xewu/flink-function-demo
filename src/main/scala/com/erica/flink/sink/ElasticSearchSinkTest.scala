package com.erica.flink.sink

import com.erica.flink.source.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util

object ElasticSearchSinkTest {
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

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        // indexer send request
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data: " + t )
          // pack into hashmap or JSON object
          val jsonObj = new util.HashMap[String,String]()
          jsonObj.put("sensor_id", t.id)
          jsonObj.put("temperature", t.temperature.toString)
          jsonObj.put("timestamp", t.timestamp.toString)

          val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(jsonObj)
          requestIndexer.add(indexRequest)

          println("saved successfully")
        }
      }
    )

    dataStream.addSink( esSinkBuilder.build() )

    env.execute("ES Sink Test")
  }
}
