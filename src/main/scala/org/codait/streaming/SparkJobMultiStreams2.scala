/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.codait.streaming

import java.util.UUID

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This is an implementation of multi-stream processing with the help of a single kafka queue.
 *
 * If there are two streams, producing with different rate of data, and from disparate sources, and
 * with even different schemas. How to handle such situations? One would be tempted to say, by
 * having two different spark structured streaming jobs for each stream.
 * But, how to handle such situations when there are 100 and even thousands such "ultra low
 * throughput streams", which put together results in significant throughput. By having a
 * separate job for each stream, one would essentially waste a lot of resources in scheduling
 * overhead or jobs holding up resources even when not in use.
 *
 * This example illustrates the point with just two stream and thus can become a starting point
 * for someone implementing multiple stream.
 */
object SparkJobMultiStreams2 {

  private def startConsoleStream(messages: DataFrame): StreamingQuery = {
    messages.writeStream
      .outputMode(OutputMode.Complete())
      .format("console").option("truncate", "false")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start()
  }

  def main(args: Array[String]) {

    val Array(kafkaServer, mqttBrokerUrl, _*) = if (args.length < 2) {
      Array("localhost:9092", "tcp://localhost:1883", "")
    } else {
      args
    }

    val spark = SparkSession
      .builder.master("local[4]") // at least 4 cores are required.
      .appName("SparkKafkaStreamingJob")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._
    val kafkaTopic = "mqttTopic"
    val kafkaTopic2 = "mqttTopic2"

    // Create DataSet representing all the incoming messages from kafka from different topics.
    val mainInputStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", s"$kafkaTopic,$kafkaTopic2")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val topic1Messages = mainInputStream.filter(x => x.startsWith("0")).flatMap {
      x => val array = x.replaceFirst("0", "").split(",")
        if (array.length == 4) {
          Some(array)
        } else {
          None
        }
    }.map(x => (x(0), x(1), Integer.parseInt(x(2).trim), x(3)))
      .as[(String, String, Int, String)]
      .toDF("Id", "Name", "Age", "Gender")
      .groupBy($"Gender").agg(min("age"), max("age"), avg("age"), count("Name"))

    val topic2Messages: DataFrame = mainInputStream.filter(x => x.startsWith("1")).flatMap {
      x => val array = x.replaceFirst("1", "").split(",")
        if (array.length == 4) {
          Some(array)
        } else {
          None
        }
    }.map(x => (x(0), x(1), x(2), Integer.parseInt(x(3))))
      .as[(String, String, String, Int)]
      .toDF("Id", "Make", "Model", "Year")
      .groupBy($"Make", $"Model", $"Year").agg($"Make", $"Model", count("Year"))


    // Start running the query for topic1 that prints the running counts to the console
    val query = startConsoleStream(topic1Messages)

    // Start running the query for topic2 that prints the running counts to the console
    val query2 = startConsoleStream(topic2Messages)

    query.awaitTermination()
    query2.awaitTermination()
  }
}
