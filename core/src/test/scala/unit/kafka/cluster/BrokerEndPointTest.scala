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
import kafka.zk.BrokerIdZNode
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.feature.Features._
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.network.BrokerEndPoint
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class BrokerEndPointTest {

  @Test
  def testHashAndEquals(): Unit = {
    val broker1 = new BrokerEndPoint(1, "myhost", 9092)
    val broker2 = new BrokerEndPoint(1, "myhost", 9092)
    val broker3 = new BrokerEndPoint(2, "myhost", 1111)
    val broker4 = new BrokerEndPoint(1, "other", 1111)

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

  private def parseBrokerJson(id: Int, jsonString: String): Broker =
    BrokerIdZNode.decode(id, jsonString.getBytes(StandardCharsets.UTF_8)).broker
}
