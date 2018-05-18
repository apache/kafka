/**
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

package kafka.server

import org.junit.Assert._
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.{After, Test}

import scala.collection.mutable.ArrayBuffer

class AdvertiseBrokerTest extends ZooKeeperTestHarness {
  val servers = ArrayBuffer[KafkaServer]()

  val brokerId = 0

  @After
  override def tearDown() {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testBrokerAdvertiseHostNameAndPortToZK(): Unit = {
    val advertisedHostName = "routable-host1"
    val advertisedPort = 1234
    val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
    props.put("advertised.host.name", advertisedHostName)
    props.put("advertised.port", advertisedPort.toString)
    servers += TestUtils.createServer(KafkaConfig.fromProps(props))

    val brokerInfo = zkClient.getBroker(brokerId).get
    assertEquals(1, brokerInfo.endPoints.size)
    val endpoint = brokerInfo.endPoints.head
    assertEquals(advertisedHostName, endpoint.host)
    assertEquals(advertisedPort, endpoint.port)
    assertEquals(SecurityProtocol.PLAINTEXT, endpoint.securityProtocol)
    assertEquals(SecurityProtocol.PLAINTEXT.name, endpoint.listenerName.value)
  }

  def testBrokerAdvertiseListenersToZK(): Unit = {
    val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
    props.put("advertised.listeners", "PLAINTEXT://routable-listener:3334")
    servers += TestUtils.createServer(KafkaConfig.fromProps(props))

    val brokerInfo = zkClient.getBroker(brokerId).get
    assertEquals(1, brokerInfo.endPoints.size)
    val endpoint = brokerInfo.endPoints.head
    assertEquals("routable-listener", endpoint.host)
    assertEquals(3334, endpoint.port)
    assertEquals(SecurityProtocol.PLAINTEXT, endpoint.securityProtocol)
    assertEquals(SecurityProtocol.PLAINTEXT.name, endpoint.listenerName)
  }

  def testBrokerAdvertiseListenersWithCustomNamesToZK(): Unit = {
    val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
    props.put("listeners", "INTERNAL://:0,EXTERNAL://:0")
    props.put("advertised.listeners", "EXTERNAL://external-listener:9999,INTERNAL://internal-listener:10999")
    props.put("listener.security.protocol.map", "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT")
    props.put("inter.broker.listener.name", "INTERNAL")
    servers += TestUtils.createServer(KafkaConfig.fromProps(props))

    val brokerInfo = zkClient.getBroker(brokerId).get
    assertEquals(1, brokerInfo.endPoints.size)
    val endpoint = brokerInfo.endPoints.head
    assertEquals("external-listener", endpoint.host)
    assertEquals(9999, endpoint.port)
    assertEquals(SecurityProtocol.PLAINTEXT, endpoint.securityProtocol)
    assertEquals("EXTERNAL", endpoint.listenerName.value)
    val endpoint2 = brokerInfo.endPoints(1)
    assertEquals("internal-listener", endpoint2.host)
    assertEquals(10999, endpoint2.port)
    assertEquals(SecurityProtocol.PLAINTEXT, endpoint.securityProtocol)
    assertEquals("INTERNAL", endpoint2.listenerName)
  }
  
}
