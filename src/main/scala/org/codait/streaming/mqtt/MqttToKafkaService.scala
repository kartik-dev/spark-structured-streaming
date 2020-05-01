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

package org.codait.streaming.mqtt

import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import org.slf4j.LoggerFactory

trait Logging {
  final val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
}

object MqttToKafkaService extends Logging {

  type BA = Array[Byte]

  private def initializeKafkaProducer(server: String): KafkaProducer[BA, BA] = {
    val props = new Properties()
    props.put("bootstrap.servers", server)
    props.put("acks", "0")
    props.put("retries", "0")
    props.put("linger.ms", "1")
    props.put("group.id", "kafka-test" + UUID.randomUUID())
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    new KafkaProducer[BA, BA](props)
  }

  private def sendToKafka(producer: KafkaProducer[BA, BA], message: BA, topic: String): Unit = {
    producer.send(new ProducerRecord[BA, BA](topic, null, message))
  }

  //   On arrival of each message from mqtt, send it to kafka topic configured.
  def mqttToKafka(mqttTopic: String, mqttBrokerUrl: String, kafkaTopic: String,
                  kafkaBrokerUrl: String, prefix: Array[Byte]): Unit = {
    val client = new MqttClient(mqttBrokerUrl, MqttClient.generateClientId(), new
        MemoryPersistence())
    val kafkaProducer = initializeKafkaProducer(kafkaBrokerUrl)

    class MqttCallbackForwardToKafka extends MqttCallbackExtended with Serializable {

      override def connectComplete(reconnect: Boolean, serverURI: String): Unit = {
        log.info(s"Mqtt connection established: $serverURI, reconnect? $reconnect.")
      }

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {

      }

      override def messageArrived(topic: String, message: MqttMessage): Unit = {
        // For each incoming message we will forward it to the Kafka Queue.
        sendToKafka(kafkaProducer, prefix ++ message.getPayload, kafkaTopic)
      }

      override def connectionLost(cause: Throwable): Unit = {

      }

    }

    client.setCallback(new MqttCallbackForwardToKafka)
    val mqttConnectOptions: MqttConnectOptions = new MqttConnectOptions()
    mqttConnectOptions.setConnectionTimeout(120)
    mqttConnectOptions.setKeepAliveInterval(2400)
    mqttConnectOptions.setMaxInflight(120)
    mqttConnectOptions.setCleanSession(true)
    client.connect(mqttConnectOptions)
    // It is not possible to initialize offset without `client.connect`
    client.subscribe(mqttTopic, 2)
  }


}