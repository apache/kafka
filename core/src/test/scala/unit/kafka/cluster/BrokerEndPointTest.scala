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

import java.nio.charset.StandardCharsets

import kafka.utils.TestUtils
import kafka.zk.BrokerIdZNode
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.feature.Features._
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertNull}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class BrokerEndPointTest {

  @Test
  def testHashAndEquals(): Unit = {
    val broker1 = TestUtils.createBroker(1, "myhost", 9092)
    val broker2 = TestUtils.createBroker(1, "myhost", 9092)
    val broker3 = TestUtils.createBroker(2, "myhost", 1111)
    val broker4 = TestUtils.createBroker(1, "other", 1111)

    assertEquals(broker1, broker2)
    assertNotEquals(broker1, broker3)
    assertNotEquals(broker1, broker4)
    assertEquals(broker1.hashCode, broker2.hashCode)
    assertNotEquals(broker1.hashCode, broker3.hashCode)
    assertNotEquals(broker1.hashCode, broker4.hashCode)

    assertEquals(Some(1), Map(broker1 -> 1).get(broker1))
  }

  @Test
  def testFromJsonFutureVersion(): Unit = {
    // Future compatible versions should be supported, we use a hypothetical future version here
    val brokerInfoStr = """{
      "foo":"bar",
      "version":100,
      "host":"localhost",
      "port":9092,
      "jmx_port":9999,
      "timestamp":"1416974968782",
      "endpoints":["SSL://localhost:9093"]
    }"""
    val broker = parseBrokerJson(1, brokerInfoStr)
    assertEquals(1, broker.id)
    val brokerEndPoint = broker.brokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.SSL))
    assertEquals("localhost", brokerEndPoint.host)
    assertEquals(9093, brokerEndPoint.port)
  }

  @Test
  def testFromJsonV2(): Unit = {
    val brokerInfoStr = """{
      "version":2,
      "host":"localhost",
      "port":9092,
      "jmx_port":9999,
      "timestamp":"1416974968782",
      "endpoints":["PLAINTEXT://localhost:9092"]
    }"""
    val broker = parseBrokerJson(1, brokerInfoStr)
    assertEquals(1, broker.id)
    val brokerEndPoint = broker.brokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    assertEquals("localhost", brokerEndPoint.host)
    assertEquals(9092, brokerEndPoint.port)
  }

  @Test
  def testFromJsonV1(): Unit = {
    val brokerInfoStr = """{"jmx_port":-1,"timestamp":"1420485325400","host":"172.16.8.243","version":1,"port":9091}"""
    val broker = parseBrokerJson(1, brokerInfoStr)
    assertEquals(1, broker.id)
    val brokerEndPoint = broker.brokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    assertEquals("172.16.8.243", brokerEndPoint.host)
    assertEquals(9091, brokerEndPoint.port)
  }

  @Test
  def testFromJsonV3(): Unit = {
    val json = """{
      "version":3,
      "host":"localhost",
      "port":9092,
      "jmx_port":9999,
      "timestamp":"2233345666",
      "endpoints":["PLAINTEXT://host1:9092", "SSL://host1:9093"],
      "rack":"dc1"
    }"""
    val broker = parseBrokerJson(1, json)
    assertEquals(1, broker.id)
    val brokerEndPoint = broker.brokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.SSL))
    assertEquals("host1", brokerEndPoint.host)
    assertEquals(9093, brokerEndPoint.port)
    assertEquals(Some("dc1"), broker.rack)
  }

  @Test
  def testFromJsonV4WithNullRack(): Unit = {
    val json = """{
      "version":4,
      "host":"localhost",
      "port":9092,
      "jmx_port":9999,
      "timestamp":"2233345666",
      "endpoints":["CLIENT://host1:9092", "REPLICATION://host1:9093"],
      "listener_security_protocol_map":{"CLIENT":"SSL", "REPLICATION":"PLAINTEXT"},
      "rack":null
    }"""
    val broker = parseBrokerJson(1, json)
    assertEquals(1, broker.id)
    val brokerEndPoint = broker.brokerEndPoint(new ListenerName("CLIENT"))
    assertEquals("host1", brokerEndPoint.host)
    assertEquals(9092, brokerEndPoint.port)
    assertEquals(None, broker.rack)
  }

  @Test
  def testFromJsonV4WithNoRack(): Unit = {
    val json = """{
      "version":4,
      "host":"localhost",
      "port":9092,
      "jmx_port":9999,
      "timestamp":"2233345666",
      "endpoints":["CLIENT://host1:9092", "REPLICATION://host1:9093"],
      "listener_security_protocol_map":{"CLIENT":"SSL", "REPLICATION":"PLAINTEXT"}
    }"""
    val broker = parseBrokerJson(1, json)
    assertEquals(1, broker.id)
    val brokerEndPoint = broker.brokerEndPoint(new ListenerName("CLIENT"))
    assertEquals("host1", brokerEndPoint.host)
    assertEquals(9092, brokerEndPoint.port)
    assertEquals(None, broker.rack)
  }

  @Test
  def testFromJsonV4WithNoFeatures(): Unit = {
    val json = """{
      "version":4,
      "host":"localhost",
      "port":9092,
      "jmx_port":9999,
      "timestamp":"2233345666",
      "endpoints":["CLIENT://host1:9092", "REPLICATION://host1:9093"],
      "listener_security_protocol_map":{"CLIENT":"SSL", "REPLICATION":"PLAINTEXT"},
      "rack":"dc1"
    }"""
    val broker = parseBrokerJson(1, json)
    assertEquals(1, broker.id)
    val brokerEndPoint = broker.brokerEndPoint(new ListenerName("CLIENT"))
    assertEquals("host1", brokerEndPoint.host)
    assertEquals(9092, brokerEndPoint.port)
    assertEquals(Some("dc1"), broker.rack)
    assertEquals(emptySupportedFeatures, broker.features)
  }

  @Test
  def testFromJsonV5(): Unit = {
    val json = """{
      "version":5,
      "host":"localhost",
      "port":9092,
      "jmx_port":9999,
      "timestamp":"2233345666",
      "endpoints":["CLIENT://host1:9092", "REPLICATION://host1:9093"],
      "listener_security_protocol_map":{"CLIENT":"SSL", "REPLICATION":"PLAINTEXT"},
      "rack":"dc1",
      "features": {"feature1": {"min_version": 1, "max_version": 2}, "feature2": {"min_version": 2, "max_version": 4}}
    }"""
    val broker = parseBrokerJson(1, json)
    assertEquals(1, broker.id)
    val brokerEndPoint = broker.brokerEndPoint(new ListenerName("CLIENT"))
    assertEquals("host1", brokerEndPoint.host)
    assertEquals(9092, brokerEndPoint.port)
    assertEquals(Some("dc1"), broker.rack)
    assertEquals(Features.supportedFeatures(
      Map[String, SupportedVersionRange](
        "feature1" -> new SupportedVersionRange(1, 2),
        "feature2" -> new SupportedVersionRange(2, 4)).asJava),
      broker.features)
  }

  @Test
  def testBrokerEndpointFromUri(): Unit = {
    var connectionString = "localhost:9092"
    var endpoint = BrokerEndPoint.createBrokerEndPoint(1, connectionString)
    assertEquals("localhost", endpoint.host)
    assertEquals(9092, endpoint.port)
    //KAFKA-3719
    connectionString = "local_host:9092"
    endpoint = BrokerEndPoint.createBrokerEndPoint(1, connectionString)
    assertEquals("local_host", endpoint.host)
    assertEquals(9092, endpoint.port)
    // also test for ipv6
    connectionString = "[::1]:9092"
    endpoint = BrokerEndPoint.createBrokerEndPoint(1, connectionString)
    assertEquals("::1", endpoint.host)
    assertEquals(9092, endpoint.port)
    // test for ipv6 with % character
    connectionString = "[fe80::b1da:69ca:57f7:63d8%3]:9092"
    endpoint = BrokerEndPoint.createBrokerEndPoint(1, connectionString)
    assertEquals("fe80::b1da:69ca:57f7:63d8%3", endpoint.host)
    assertEquals(9092, endpoint.port)
    // add test for uppercase in hostname
    connectionString = "MyHostname:9092"
    endpoint = BrokerEndPoint.createBrokerEndPoint(1, connectionString)
    assertEquals("MyHostname", endpoint.host)
    assertEquals(9092, endpoint.port)
  }

  @Test
  def testEndpointFromUri(): Unit = {
    var connectionString = "PLAINTEXT://localhost:9092"
    var endpoint = EndPoint.createEndPoint(connectionString, None)
    assertEquals("localhost", endpoint.host)
    assertEquals(9092, endpoint.port)
    assertEquals("PLAINTEXT://localhost:9092", endpoint.connectionString)
    // KAFKA-3719
    connectionString = "PLAINTEXT://local_host:9092"
    endpoint = EndPoint.createEndPoint(connectionString, None)
    assertEquals("local_host", endpoint.host)
    assertEquals(9092, endpoint.port)
    assertEquals("PLAINTEXT://local_host:9092", endpoint.connectionString)
    // also test for default bind
    connectionString = "PLAINTEXT://:9092"
    endpoint = EndPoint.createEndPoint(connectionString, None)
    assertNull(endpoint.host)
    assertEquals(9092, endpoint.port)
    assertEquals( "PLAINTEXT://:9092", endpoint.connectionString)
    // also test for ipv6
    connectionString = "PLAINTEXT://[::1]:9092"
    endpoint = EndPoint.createEndPoint(connectionString, None)
    assertEquals("::1", endpoint.host)
    assertEquals(9092, endpoint.port)
    assertEquals("PLAINTEXT://[::1]:9092", endpoint.connectionString)
    // test for ipv6 with % character
    connectionString = "PLAINTEXT://[fe80::b1da:69ca:57f7:63d8%3]:9092"
    endpoint = EndPoint.createEndPoint(connectionString, None)
    assertEquals("fe80::b1da:69ca:57f7:63d8%3", endpoint.host)
    assertEquals(9092, endpoint.port)
    assertEquals("PLAINTEXT://[fe80::b1da:69ca:57f7:63d8%3]:9092", endpoint.connectionString)
    // test hostname
    connectionString = "PLAINTEXT://MyHostname:9092"
    endpoint = EndPoint.createEndPoint(connectionString, None)
    assertEquals("MyHostname", endpoint.host)
    assertEquals(9092, endpoint.port)
    assertEquals("PLAINTEXT://MyHostname:9092", endpoint.connectionString)
  }

  private def parseBrokerJson(id: Int, jsonString: String): Broker =
    BrokerIdZNode.decode(id, jsonString.getBytes(StandardCharsets.UTF_8)).broker
}
