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

import java.io.File
import java.net.URI

import org.apache.activemq.broker.{BrokerService, TransportConnector}

object MQTTServer extends Logging {

  def setup(port: Int) = {
    val broker = new BrokerService()
    broker.setDataDirectoryFile(new File("/tmp/"))
    val connector = new TransportConnector()
    connector.setName("mqtt")
    connector.setUri(new URI("mqtt://" + "localhost:" + port))
    broker.addConnector(connector)
    broker.start()
    log.info("Broker URI: " + broker.getDefaultSocketURIString)
  }

  def main(params: Array[String]) = {
    if (params.length == 0) {
      setup(1883)
    }
    else {
      setup(params(0).toInt)
    }
  }
}