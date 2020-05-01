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

import org.apache.spark.sql.SparkSession

/**
 * This example illustrates use case of using mqtt stream with spark structured streaming.
 *
 * A non distributed message queue, may lead to downtime .e.g. during a node failure. It is thus a
 * best practice, to use kafka queue as a medium to spark. Infact, many such streams can be directed
 * using a single kafka queue. The next example illustrates that point.
 *
 */
object SparkJobSimple {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder.master("yarn")
      .appName("SparkKafkaStreamingJob")
      .getOrCreate()

    import spark.implicits._
    val kafkaTopic = "test"

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "c134-node2.squadron.support.hortonworks.com:6667")
      .option("subscribe", kafkaTopic)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    // Calculate wordcounts.
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start()

    query.awaitTermination()

  }
}
