/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.cluster

import java.nio.ByteBuffer

import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.Test

import scala.collection.mutable

class BrokerEndPointTest extends Logging {

  @Test
  def testHashAndEquals() {
    val broker1 = TestUtils.createBroker(1, "myhost", 9092)
    val broker2 = TestUtils.createBroker(1, "myhost", 9092)
    val broker3 = TestUtils.createBroker(2, "myhost", 1111)
    val broker4 = TestUtils.createBroker(1, "other", 1111)

    assert(broker1 == broker2)
    assert(broker1 != broker3)
    assert(broker1 != broker4)
    assert(broker1.hashCode() == broker2.hashCode())
    assert(broker1.hashCode() != broker3.hashCode())
    assert(broker1.hashCode() != broker4.hashCode())

    val hashmap = new mutable.HashMap[Broker, Int]()
    hashmap.put(broker1, 1)
    assert(hashmap.getOrElse(broker1, -1) == 1)
  }

  @Test
  def testFromJsonFutureVersion() {
    // `createBroker` should support future compatible versions, we use a hypothetical future version here
    val brokerInfoStr = """{
      "foo":"bar",
      "version":100,
      "host":"localhost",
      "port":9092,
      "jmx_port":9999,
      "timestamp":"1416974968782",
      "endpoints":["SSL://localhost:9093"]
    }"""
    val broker = Broker.createBroker(1, brokerInfoStr)
    assert(broker.id == 1)
    val brokerEndPoint = broker.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.SSL))
    assert(brokerEndPoint.host == "localhost")
    assert(brokerEndPoint.port == 9093)
  }

  @Test
  def testFromJsonV2 {
    val brokerInfoStr = """{
      "version":2,
      "host":"localhost",
      "port":9092,
      "jmx_port":9999,
      "timestamp":"1416974968782",
      "endpoints":["PLAINTEXT://localhost:9092"]
    }"""
    val broker = Broker.createBroker(1, brokerInfoStr)
    assert(broker.id == 1)
    val brokerEndPoint = broker.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    assert(brokerEndPoint.host == "localhost")
    assert(brokerEndPoint.port == 9092)
  }

  @Test
  def testFromJsonV1() = {
    val brokerInfoStr = """{"jmx_port":-1,"timestamp":"1420485325400","host":"172.16.8.243","version":1,"port":9091}"""
    val broker = Broker.createBroker(1, brokerInfoStr)
    assert(broker.id == 1)
    val brokerEndPoint = broker.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    assert(brokerEndPoint.host == "172.16.8.243")
    assert(brokerEndPoint.port == 9091)
  }

  @Test
  def testBrokerEndpointFromUri() {
    var connectionString = "localhost:9092"
    var endpoint = BrokerEndPoint.createBrokerEndPoint(1, connectionString)
    assert(endpoint.host == "localhost")
    assert(endpoint.port == 9092)
    //KAFKA-3719
    connectionString = "local_host:9092"
    endpoint = BrokerEndPoint.createBrokerEndPoint(1, connectionString)
    assert(endpoint.host == "local_host")
    assert(endpoint.port == 9092)
    // also test for ipv6
    connectionString = "[::1]:9092"
    endpoint = BrokerEndPoint.createBrokerEndPoint(1, connectionString)
    assert(endpoint.host == "::1")
    assert(endpoint.port == 9092)
    // test for ipv6 with % character
    connectionString = "[fe80::b1da:69ca:57f7:63d8%3]:9092"
    endpoint = BrokerEndPoint.createBrokerEndPoint(1, connectionString)
    assert(endpoint.host == "fe80::b1da:69ca:57f7:63d8%3")
    assert(endpoint.port == 9092)
    // add test for uppercase in hostname
    connectionString = "MyHostname:9092"
    endpoint = BrokerEndPoint.createBrokerEndPoint(1, connectionString)
    assert(endpoint.host == "MyHostname")
    assert(endpoint.port == 9092)
  }

  @Test
  def testEndpointFromUri() {
    var connectionString = "PLAINTEXT://localhost:9092"
    var endpoint = EndPoint.createEndPoint(connectionString, None)
    assert(endpoint.host == "localhost")
    assert(endpoint.port == 9092)
    assert(endpoint.connectionString == "PLAINTEXT://localhost:9092")
    // KAFKA-3719
    connectionString = "PLAINTEXT://local_host:9092"
    endpoint = EndPoint.createEndPoint(connectionString, None)
    assert(endpoint.host == "local_host")
    assert(endpoint.port == 9092)
    assert(endpoint.connectionString == "PLAINTEXT://local_host:9092")
    // also test for default bind
    connectionString = "PLAINTEXT://:9092"
    endpoint = EndPoint.createEndPoint(connectionString, None)
    assert(endpoint.host == null)
    assert(endpoint.port == 9092)
    assert(endpoint.connectionString == "PLAINTEXT://:9092")
    // also test for ipv6
    connectionString = "PLAINTEXT://[::1]:9092"
    endpoint = EndPoint.createEndPoint(connectionString, None)
    assert(endpoint.host == "::1")
    assert(endpoint.port == 9092)
    assert(endpoint.connectionString ==  "PLAINTEXT://[::1]:9092")
    // test for ipv6 with % character
    connectionString = "PLAINTEXT://[fe80::b1da:69ca:57f7:63d8%3]:9092"
    endpoint = EndPoint.createEndPoint(connectionString, None)
    assert(endpoint.host == "fe80::b1da:69ca:57f7:63d8%3")
    assert(endpoint.port == 9092)
    assert(endpoint.connectionString ==  "PLAINTEXT://[fe80::b1da:69ca:57f7:63d8%3]:9092")
    // test hostname
    connectionString = "PLAINTEXT://MyHostname:9092"
    endpoint = EndPoint.createEndPoint(connectionString, None)
    assert(endpoint.host == "MyHostname")
    assert(endpoint.port == 9092)
    assert(endpoint.connectionString ==  "PLAINTEXT://MyHostname:9092")
  }
}
