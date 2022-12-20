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

import kafka.cluster.EndPoint
import kafka.log.LogConfig
import kafka.utils.TestUtils.assertBadConfigContainingMessage
import kafka.utils.{CoreUtils, TestUtils}
import org.apache.kafka.common.config.{ConfigException, TopicConfig}
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.{CompressionType, Records}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.raft.RaftConfig.{AddressSpec, InetAddressSpec, UNKNOWN_ADDRESS_SPEC_INSTANCE}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.net.InetSocketAddress
import java.util
import java.util.{Collections, Properties}
import org.apache.kafka.common.Node
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.{IBP_0_8_2, IBP_3_0_IV1}
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.junit.jupiter.api.function.Executable

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

class KafkaConfigTest {

  @Test
  def testLogRetentionTimeHoursProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.LogRetentionTimeHoursProp, "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(60L * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeMinutesProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.LogRetentionTimeMinutesProp, "30")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeMsProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.LogRetentionTimeMillisProp, "1800000")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeNoConfigProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(24 * 7 * 60L * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeBothMinutesAndHoursProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.LogRetentionTimeMinutesProp, "30")
    props.setProperty(KafkaConfig.LogRetentionTimeHoursProp, "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeBothMinutesAndMsProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.LogRetentionTimeMillisProp, "1800000")
    props.setProperty(KafkaConfig.LogRetentionTimeMinutesProp, "10")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionUnlimited(): Unit = {
    val props1 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)
    val props2 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)
    val props3 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)
    val props4 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)
    val props5 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)

    props1.setProperty("log.retention.ms", "-1")
    props2.setProperty("log.retention.minutes", "-1")
    props3.setProperty("log.retention.hours", "-1")

    val cfg1 = KafkaConfig.fromProps(props1)
    val cfg2 = KafkaConfig.fromProps(props2)
    val cfg3 = KafkaConfig.fromProps(props3)
    assertEquals(-1, cfg1.logRetentionTimeMillis, "Should be -1")
    assertEquals(-1, cfg2.logRetentionTimeMillis, "Should be -1")
    assertEquals(-1, cfg3.logRetentionTimeMillis, "Should be -1")

    props4.setProperty("log.retention.ms", "-1")
    props4.setProperty("log.retention.minutes", "30")

    val cfg4 = KafkaConfig.fromProps(props4)
    assertEquals(-1, cfg4.logRetentionTimeMillis, "Should be -1")

    props5.setProperty("log.retention.ms", "0")

    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props5))
  }

  @Test
  def testLogRetentionValid(): Unit = {
    val props1 = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val props2 = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val props3 = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)

    props1.setProperty("log.retention.ms", "0")
    props2.setProperty("log.retention.minutes", "0")
    props3.setProperty("log.retention.hours", "0")

    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props1))
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props2))
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props3))

  }

  @Test
  def testAdvertiseDefaults(): Unit = {
    val port = 9999
    val hostName = "fake-host"
    val props = new Properties()
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")
    props.setProperty(KafkaConfig.ListenersProp, s"PLAINTEXT://$hostName:$port")
    val serverConfig = KafkaConfig.fromProps(props)

    val endpoints = serverConfig.effectiveAdvertisedListeners
    assertEquals(1, endpoints.size)
    val endpoint = endpoints.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).get
    assertEquals(endpoint.host, hostName)
    assertEquals(endpoint.port, port)
  }

  @Test
  def testAdvertiseConfigured(): Unit = {
    val advertisedHostName = "routable-host"
    val advertisedPort = 1234

    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.setProperty(KafkaConfig.AdvertisedListenersProp, s"PLAINTEXT://$advertisedHostName:$advertisedPort")

    val serverConfig = KafkaConfig.fromProps(props)
    val endpoints = serverConfig.effectiveAdvertisedListeners
    val endpoint = endpoints.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).get

    assertEquals(endpoint.host, advertisedHostName)
    assertEquals(endpoint.port, advertisedPort)
  }

  @Test
  def testDuplicateListeners(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")

    // listeners with duplicate port
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9091,SSL://localhost:9091")
    assertBadConfigContainingMessage(props, "Each listener must have a different port")

    // listeners with duplicate name
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9091,PLAINTEXT://localhost:9092")
    assertBadConfigContainingMessage(props, "Each listener must have a different name")

    // advertised listeners can have duplicate ports
    props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, "HOST:SASL_SSL,LB:SASL_SSL")
    props.setProperty(KafkaConfig.InterBrokerListenerNameProp, "HOST")
    props.setProperty(KafkaConfig.ListenersProp, "HOST://localhost:9091,LB://localhost:9092")
    props.setProperty(KafkaConfig.AdvertisedListenersProp, "HOST://localhost:9091,LB://localhost:9091")
    KafkaConfig.fromProps(props)

    // but not duplicate names
    props.setProperty(KafkaConfig.AdvertisedListenersProp, "HOST://localhost:9091,HOST://localhost:9091")
    assertBadConfigContainingMessage(props, "Each listener must have a different name")
  }

  @Test
  def testControlPlaneListenerName(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.setProperty("listeners", "PLAINTEXT://localhost:0,CONTROLLER://localhost:5000")
    props.setProperty("listener.security.protocol.map", "PLAINTEXT:PLAINTEXT,CONTROLLER:SSL")
    props.setProperty("control.plane.listener.name", "CONTROLLER")
    KafkaConfig.fromProps(props)

    val serverConfig = KafkaConfig.fromProps(props)
    val controlEndpoint = serverConfig.controlPlaneListener.get
    assertEquals("localhost", controlEndpoint.host)
    assertEquals(5000, controlEndpoint.port)
    assertEquals(SecurityProtocol.SSL, controlEndpoint.securityProtocol)

    //advertised listener should contain control-plane listener
    val advertisedEndpoints = serverConfig.effectiveAdvertisedListeners
    assertTrue(advertisedEndpoints.exists { endpoint =>
      endpoint.securityProtocol == controlEndpoint.securityProtocol && endpoint.listenerName.value().equals(controlEndpoint.listenerName.value())
    })

    // interBrokerListener name should be different from control-plane listener name
    val interBrokerListenerName = serverConfig.interBrokerListenerName
    assertFalse(interBrokerListenerName.value().equals(controlEndpoint.listenerName.value()))
  }

  @Test
  def testControllerListenerNames(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker,controller")
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:0,CONTROLLER://localhost:5000")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
    props.setProperty(KafkaConfig.NodeIdProp, "2")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:5000")
    props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, "PLAINTEXT:PLAINTEXT,CONTROLLER:SASL_SSL")

    val serverConfig = KafkaConfig.fromProps(props)
    val controllerEndpoints = serverConfig.controllerListeners
    assertEquals(1, controllerEndpoints.size)
    val controllerEndpoint = controllerEndpoints.iterator.next()
    assertEquals("localhost", controllerEndpoint.host)
    assertEquals(5000, controllerEndpoint.port)
    assertEquals(SecurityProtocol.SASL_SSL, controllerEndpoint.securityProtocol)
  }

  @Test
  def testControlPlaneListenerNameNotAllowedWithKRaft(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker,controller")
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092,SSL://localhost:9093")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    props.setProperty(KafkaConfig.NodeIdProp, "2")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093")
    props.setProperty(KafkaConfig.ControlPlaneListenerNameProp, "SSL")

    assertFalse(isValidKafkaConfig(props))
    assertBadConfigContainingMessage(props, "control.plane.listener.name is not supported in KRaft mode.")

    props.remove(KafkaConfig.ControlPlaneListenerNameProp)
    KafkaConfig.fromProps(props)
  }

  @Test
  def testControllerListenerDefinedForKRaftController(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "controller")
    props.setProperty(KafkaConfig.ListenersProp, "SSL://localhost:9093")
    props.setProperty(KafkaConfig.NodeIdProp, "2")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093")

    assertBadConfigContainingMessage(props, "The listeners config must only contain KRaft controller listeners from controller.listener.names when process.roles=controller")

    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    KafkaConfig.fromProps(props)

    // confirm that redirecting via listener.security.protocol.map is acceptable
    props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, "CONTROLLER:SSL")
    props.setProperty(KafkaConfig.ListenersProp, "CONTROLLER://localhost:9093")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testControllerListenerDefinedForKRaftBroker(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    props.setProperty(KafkaConfig.NodeIdProp, "1")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093")

    assertFalse(isValidKafkaConfig(props))
    assertBadConfigContainingMessage(props, "controller.listener.names must contain at least one value when running KRaft with just the broker role")

    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    KafkaConfig.fromProps(props)

    // confirm that redirecting via listener.security.protocol.map is acceptable
    props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, "PLAINTEXT:PLAINTEXT,CONTROLLER:SSL")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testPortInQuorumVotersNotRequiredToMatchFirstControllerListenerPortForThisKRaftController(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "controller,broker")
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092,SSL://localhost:9093,SASL_SSL://localhost:9094")
    props.setProperty(KafkaConfig.NodeIdProp, "2")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093,3@anotherhost:9094")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL,SASL_SSL")
    KafkaConfig.fromProps(props)

    // change each of the 4 ports to port 5555 -- should pass in all circumstances since we can't validate the
    // controller.quorum.voters ports (which are the ports that clients use and are semantically "advertised" ports
    // even though the controller configuration doesn't list them in advertised.listeners) against the
    // listener ports (which are semantically different then the ports that clients use).
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092,SSL://localhost:5555,SASL_SSL://localhost:9094")
    KafkaConfig.fromProps(props)
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092,SSL://localhost:9093,SASL_SSL://localhost:5555")
    KafkaConfig.fromProps(props)
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092,SSL://localhost:9093,SASL_SSL://localhost:9094") // reset to original value
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:5555,3@anotherhost:9094")
    KafkaConfig.fromProps(props)
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093,3@anotherhost:5555")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testSeparateControllerListenerDefinedForKRaftBrokerController(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker,controller")
    props.setProperty(KafkaConfig.ListenersProp, "SSL://localhost:9093")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    props.setProperty(KafkaConfig.NodeIdProp, "2")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093")

    assertFalse(isValidKafkaConfig(props))
    assertBadConfigContainingMessage(props, "There must be at least one advertised listener. Perhaps all listeners appear in controller.listener.names?")

    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092,SSL://localhost:9093")
    KafkaConfig.fromProps(props)

    // confirm that redirecting via listener.security.protocol.map is acceptable
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093")
    props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, "PLAINTEXT:PLAINTEXT,CONTROLLER:SSL")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testControllerListenerNameMapsToPlaintextByDefaultForKRaft(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
    props.setProperty(KafkaConfig.NodeIdProp, "1")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093")
    val controllerListenerName = new ListenerName("CONTROLLER")
    assertEquals(Some(SecurityProtocol.PLAINTEXT),
      KafkaConfig.fromProps(props).effectiveListenerSecurityProtocolMap.get(controllerListenerName))
    // ensure we don't map it to PLAINTEXT when there is a SSL or SASL controller listener
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER,SSL")
    val controllerNotFoundInMapMessage = "Controller listener with name CONTROLLER defined in controller.listener.names not found in listener.security.protocol.map"
    assertBadConfigContainingMessage(props, controllerNotFoundInMapMessage)
    // ensure we don't map it to PLAINTEXT when there is a SSL or SASL listener
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
    props.setProperty(KafkaConfig.ListenersProp, "SSL://localhost:9092")
    assertBadConfigContainingMessage(props, controllerNotFoundInMapMessage)
    props.remove(KafkaConfig.ListenersProp)
    // ensure we don't map it to PLAINTEXT when it is explicitly mapped otherwise
    props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, "PLAINTEXT:PLAINTEXT,CONTROLLER:SSL")
    assertEquals(Some(SecurityProtocol.SSL),
      KafkaConfig.fromProps(props).effectiveListenerSecurityProtocolMap.get(controllerListenerName))
    // ensure we don't map it to PLAINTEXT when anything is explicitly given
    // (i.e. it is only part of the default value, even with KRaft)
    props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, "PLAINTEXT:PLAINTEXT")
    assertBadConfigContainingMessage(props, controllerNotFoundInMapMessage)
    // ensure we can map it to a non-PLAINTEXT security protocol by default (i.e. when nothing is given)
    props.remove(KafkaConfig.ListenerSecurityProtocolMapProp)
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    assertEquals(Some(SecurityProtocol.SSL),
      KafkaConfig.fromProps(props).effectiveListenerSecurityProtocolMap.get(new ListenerName("SSL")))
  }

  @Test
  def testMultipleControllerListenerNamesMapToPlaintextByDefaultForKRaft(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "controller")
    props.setProperty(KafkaConfig.ListenersProp, "CONTROLLER1://localhost:9092,CONTROLLER2://localhost:9093")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER1,CONTROLLER2")
    props.setProperty(KafkaConfig.NodeIdProp, "1")
    props.setProperty(KafkaConfig.QuorumVotersProp, "1@localhost:9092")
    assertEquals(Some(SecurityProtocol.PLAINTEXT),
      KafkaConfig.fromProps(props).effectiveListenerSecurityProtocolMap.get(new ListenerName("CONTROLLER1")))
    assertEquals(Some(SecurityProtocol.PLAINTEXT),
      KafkaConfig.fromProps(props).effectiveListenerSecurityProtocolMap.get(new ListenerName("CONTROLLER2")))
  }

  @Test
  def testControllerListenerNameDoesNotMapToPlaintextByDefaultForNonKRaft(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")
    props.setProperty(KafkaConfig.ListenersProp, "CONTROLLER://localhost:9092")
    assertBadConfigContainingMessage(props,
      "Error creating broker listeners from 'CONTROLLER://localhost:9092': No security protocol defined for listener CONTROLLER")
    // Valid now
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092")
    assertEquals(None, KafkaConfig.fromProps(props).effectiveListenerSecurityProtocolMap.get(new ListenerName("CONTROLLER")))
  }

  @Test
  def testBadListenerProtocol(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")
    props.setProperty(KafkaConfig.ListenersProp, "BAD://localhost:9091")

    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testListenerNamesWithAdvertisedListenerUnset(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")

    props.setProperty(KafkaConfig.ListenersProp, "CLIENT://localhost:9091,REPLICATION://localhost:9092,INTERNAL://localhost:9093")
    props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, "CLIENT:SSL,REPLICATION:SSL,INTERNAL:PLAINTEXT")
    props.setProperty(KafkaConfig.InterBrokerListenerNameProp, "REPLICATION")
    val config = KafkaConfig.fromProps(props)
    val expectedListeners = Seq(
      EndPoint("localhost", 9091, new ListenerName("CLIENT"), SecurityProtocol.SSL),
      EndPoint("localhost", 9092, new ListenerName("REPLICATION"), SecurityProtocol.SSL),
      EndPoint("localhost", 9093, new ListenerName("INTERNAL"), SecurityProtocol.PLAINTEXT))
    assertEquals(expectedListeners, config.listeners)
    assertEquals(expectedListeners, config.effectiveAdvertisedListeners)
    val expectedSecurityProtocolMap = Map(
      new ListenerName("CLIENT") -> SecurityProtocol.SSL,
      new ListenerName("REPLICATION") -> SecurityProtocol.SSL,
      new ListenerName("INTERNAL") -> SecurityProtocol.PLAINTEXT
    )
    assertEquals(expectedSecurityProtocolMap, config.effectiveListenerSecurityProtocolMap)
  }

  @Test
  def testListenerAndAdvertisedListenerNames(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")

    props.setProperty(KafkaConfig.ListenersProp, "EXTERNAL://localhost:9091,INTERNAL://localhost:9093")
    props.setProperty(KafkaConfig.AdvertisedListenersProp, "EXTERNAL://lb1.example.com:9000,INTERNAL://host1:9093")
    props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, "EXTERNAL:SSL,INTERNAL:PLAINTEXT")
    props.setProperty(KafkaConfig.InterBrokerListenerNameProp, "INTERNAL")
    val config = KafkaConfig.fromProps(props)

    val expectedListeners = Seq(
      EndPoint("localhost", 9091, new ListenerName("EXTERNAL"), SecurityProtocol.SSL),
      EndPoint("localhost", 9093, new ListenerName("INTERNAL"), SecurityProtocol.PLAINTEXT)
    )
    assertEquals(expectedListeners, config.listeners)

    val expectedAdvertisedListeners = Seq(
      EndPoint("lb1.example.com", 9000, new ListenerName("EXTERNAL"), SecurityProtocol.SSL),
      EndPoint("host1", 9093, new ListenerName("INTERNAL"), SecurityProtocol.PLAINTEXT)
    )
    assertEquals(expectedAdvertisedListeners, config.effectiveAdvertisedListeners)

    val expectedSecurityProtocolMap = Map(
      new ListenerName("EXTERNAL") -> SecurityProtocol.SSL,
      new ListenerName("INTERNAL") -> SecurityProtocol.PLAINTEXT
    )
    assertEquals(expectedSecurityProtocolMap, config.effectiveListenerSecurityProtocolMap)
  }

  @Test
  def testListenerNameMissingFromListenerSecurityProtocolMap(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")

    props.setProperty(KafkaConfig.ListenersProp, "SSL://localhost:9091,REPLICATION://localhost:9092")
    props.setProperty(KafkaConfig.InterBrokerListenerNameProp, "SSL")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testInterBrokerListenerNameMissingFromListenerSecurityProtocolMap(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")

    props.setProperty(KafkaConfig.ListenersProp, "SSL://localhost:9091")
    props.setProperty(KafkaConfig.InterBrokerListenerNameProp, "REPLICATION")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testInterBrokerListenerNameAndSecurityProtocolSet(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")

    props.setProperty(KafkaConfig.ListenersProp, "SSL://localhost:9091")
    props.setProperty(KafkaConfig.InterBrokerListenerNameProp, "SSL")
    props.setProperty(KafkaConfig.InterBrokerSecurityProtocolProp, "SSL")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testCaseInsensitiveListenerProtocol(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")
    props.setProperty(KafkaConfig.ListenersProp, "plaintext://localhost:9091,SsL://localhost:9092")
    val config = KafkaConfig.fromProps(props)
    assertEquals(Some("SSL://localhost:9092"), config.listeners.find(_.listenerName.value == "SSL").map(_.connectionString))
    assertEquals(Some("PLAINTEXT://localhost:9091"), config.listeners.find(_.listenerName.value == "PLAINTEXT").map(_.connectionString))
  }

  private def listenerListToEndPoints(listenerList: String,
                              securityProtocolMap: collection.Map[ListenerName, SecurityProtocol] = EndPoint.DefaultSecurityProtocolMap) =
    CoreUtils.listenerListToEndPoints(listenerList, securityProtocolMap)

  @Test
  def testListenerDefaults(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")

    // configuration with no listeners
    val conf = KafkaConfig.fromProps(props)
    assertEquals(listenerListToEndPoints("PLAINTEXT://:9092"), conf.listeners)
    assertNull(conf.listeners.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).get.host)
    assertEquals(conf.effectiveAdvertisedListeners, listenerListToEndPoints("PLAINTEXT://:9092"))
  }

  @nowarn("cat=deprecation")
  @Test
  def testVersionConfiguration(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")
    val conf = KafkaConfig.fromProps(props)
    assertEquals(MetadataVersion.latest, conf.interBrokerProtocolVersion)

    props.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, "0.8.2.0")
    // We need to set the message format version to make the configuration valid.
    props.setProperty(KafkaConfig.LogMessageFormatVersionProp, "0.8.2.0")
    val conf2 = KafkaConfig.fromProps(props)
    assertEquals(IBP_0_8_2, conf2.interBrokerProtocolVersion)

    // check that 0.8.2.0 is the same as 0.8.2.1
    props.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, "0.8.2.1")
    // We need to set the message format version to make the configuration valid
    props.setProperty(KafkaConfig.LogMessageFormatVersionProp, "0.8.2.1")
    val conf3 = KafkaConfig.fromProps(props)
    assertEquals(IBP_0_8_2, conf3.interBrokerProtocolVersion)

    //check that latest is newer than 0.8.2
    assertTrue(MetadataVersion.latest.isAtLeast(conf3.interBrokerProtocolVersion))
  }

  private def isValidKafkaConfig(props: Properties): Boolean = {
    try {
      KafkaConfig.fromProps(props)
      true
    } catch {
      case _: IllegalArgumentException | _: ConfigException => false
    }
  }

  @Test
  def testUncleanLeaderElectionDefault(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, false)
  }

  @Test
  def testUncleanElectionDisabled(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.UncleanLeaderElectionEnableProp, String.valueOf(false))
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, false)
  }

  @Test
  def testUncleanElectionEnabled(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.UncleanLeaderElectionEnableProp, String.valueOf(true))
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, true)
  }

  @Test
  def testUncleanElectionInvalid(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.UncleanLeaderElectionEnableProp, "invalid")

    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testLogRollTimeMsProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.LogRollTimeMillisProp, "1800000")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRollTimeMillis)
  }

  @Test
  def testLogRollTimeBothMsAndHoursProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.LogRollTimeMillisProp, "1800000")
    props.setProperty(KafkaConfig.LogRollTimeHoursProp, "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRollTimeMillis)
  }

  @Test
  def testLogRollTimeNoConfigProvided(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(24 * 7 * 60L * 60L * 1000L, cfg.logRollTimeMillis																									)
  }

  @Test
  def testDefaultCompressionType(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.compressionType, "producer")
  }

  @Test
  def testValidCompressionType(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty("compression.type", "gzip")
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.compressionType, "gzip")
  }

  @Test
  def testInvalidCompressionType(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.CompressionTypeProp, "abc")
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testInvalidInterBrokerSecurityProtocol(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.ListenersProp, "SSL://localhost:0")
    props.setProperty(KafkaConfig.InterBrokerSecurityProtocolProp, SecurityProtocol.PLAINTEXT.toString)
    assertThrows(classOf[IllegalArgumentException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testEqualAdvertisedListenersProtocol(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092,SSL://localhost:9093")
    props.setProperty(KafkaConfig.AdvertisedListenersProp, "PLAINTEXT://localhost:9092,SSL://localhost:9093")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testInvalidAdvertisedListenersProtocol(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.ListenersProp, "TRACE://localhost:9091,SSL://localhost:9093")
    props.setProperty(KafkaConfig.AdvertisedListenersProp, "PLAINTEXT://localhost:9092")
    assertBadConfigContainingMessage(props, "No security protocol defined for listener TRACE")

    props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, "PLAINTEXT:PLAINTEXT,TRACE:PLAINTEXT,SSL:SSL")
    assertBadConfigContainingMessage(props, "advertised.listeners listener names must be equal to or a subset of the ones defined in listeners")
  }

  @nowarn("cat=deprecation")
  @Test
  def testInterBrokerVersionMessageFormatCompatibility(): Unit = {
    def buildConfig(interBrokerProtocol: MetadataVersion, messageFormat: MetadataVersion): KafkaConfig = {
      val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
      props.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, interBrokerProtocol.version)
      props.setProperty(KafkaConfig.LogMessageFormatVersionProp, messageFormat.version)
      KafkaConfig.fromProps(props)
    }

    MetadataVersion.VERSIONS.foreach { interBrokerVersion =>
      MetadataVersion.VERSIONS.foreach { messageFormatVersion =>
        if (interBrokerVersion.highestSupportedRecordVersion.value >= messageFormatVersion.highestSupportedRecordVersion.value) {
          val config = buildConfig(interBrokerVersion, messageFormatVersion)
          assertEquals(interBrokerVersion, config.interBrokerProtocolVersion)
          if (interBrokerVersion.isAtLeast(IBP_3_0_IV1))
            assertEquals(IBP_3_0_IV1, config.logMessageFormatVersion)
          else
            assertEquals(messageFormatVersion, config.logMessageFormatVersion)
        } else {
          assertThrows(classOf[IllegalArgumentException], () => buildConfig(interBrokerVersion, messageFormatVersion))
        }
      }
    }
  }

  @Test
  def testFromPropsInvalid(): Unit = {
    def baseProperties: Properties = {
      val validRequiredProperties = new Properties()
      validRequiredProperties.setProperty(KafkaConfig.ZkConnectProp, "127.0.0.1:2181")
      validRequiredProperties
    }
    // to ensure a basis is valid - bootstraps all needed validation
    KafkaConfig.fromProps(baseProperties)

    KafkaConfig.configNames.foreach { name =>
      name match {
        case KafkaConfig.ZkConnectProp => // ignore string
        case KafkaConfig.ZkSessionTimeoutMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ZkConnectionTimeoutMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ZkEnableSecureAclsProp => assertPropertyInvalid(baseProperties, name, "not_a_boolean")
        case KafkaConfig.ZkMaxInFlightRequestsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.ZkSslClientEnableProp => assertPropertyInvalid(baseProperties, name, "not_a_boolean")
        case KafkaConfig.ZkClientCnxnSocketProp =>  //ignore string
        case KafkaConfig.ZkSslKeyStoreLocationProp =>  //ignore string
        case KafkaConfig.ZkSslKeyStorePasswordProp =>  //ignore string
        case KafkaConfig.ZkSslKeyStoreTypeProp =>  //ignore string
        case KafkaConfig.ZkSslTrustStoreLocationProp =>  //ignore string
        case KafkaConfig.ZkSslTrustStorePasswordProp =>  //ignore string
        case KafkaConfig.ZkSslTrustStoreTypeProp =>  //ignore string
        case KafkaConfig.ZkSslProtocolProp =>  //ignore string
        case KafkaConfig.ZkSslEnabledProtocolsProp =>  //ignore string
        case KafkaConfig.ZkSslCipherSuitesProp =>  //ignore string
        case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp => //ignore string
        case KafkaConfig.ZkSslCrlEnableProp => assertPropertyInvalid(baseProperties, name, "not_a_boolean")
        case KafkaConfig.ZkSslOcspEnableProp => assertPropertyInvalid(baseProperties, name, "not_a_boolean")

        case KafkaConfig.BrokerIdProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.NumNetworkThreadsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.NumIoThreadsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.BackgroundThreadsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.QueuedMaxRequestsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.NumReplicaAlterLogDirsThreadsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.QueuedMaxBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.RequestTimeoutMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ConnectionSetupTimeoutMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ConnectionSetupTimeoutMaxMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")

        // KRaft mode configs
        case KafkaConfig.ProcessRolesProp => // ignore
        case KafkaConfig.InitialBrokerRegistrationTimeoutMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.BrokerHeartbeatIntervalMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.BrokerSessionTimeoutMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.NodeIdProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.MetadataLogDirProp => // ignore string
        case KafkaConfig.MetadataLogSegmentBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.MetadataLogSegmentMillisProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.MetadataMaxRetentionBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.MetadataMaxRetentionMillisProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ControllerListenerNamesProp => // ignore string
        case KafkaConfig.MetadataMaxIdleIntervalMsProp  => assertPropertyInvalid(baseProperties, name, "not_a_number")

        case KafkaConfig.AuthorizerClassNameProp => //ignore string
        case KafkaConfig.CreateTopicPolicyClassNameProp => //ignore string

        case KafkaConfig.SocketSendBufferBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.SocketReceiveBufferBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.SocketListenBacklogSizeProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.MaxConnectionsPerIpOverridesProp =>
          assertPropertyInvalid(baseProperties, name, "127.0.0.1:not_a_number")
        case KafkaConfig.ConnectionsMaxIdleMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.FailedAuthenticationDelayMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "-1")

        case KafkaConfig.NumPartitionsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.LogDirsProp => // ignore string
        case KafkaConfig.LogDirProp => // ignore string
        case KafkaConfig.LogSegmentBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number", Records.LOG_OVERHEAD - 1)

        case KafkaConfig.LogRollTimeMillisProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.LogRollTimeHoursProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")

        case KafkaConfig.LogRetentionTimeMillisProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.LogRetentionTimeMinutesProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.LogRetentionTimeHoursProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")

        case KafkaConfig.LogRetentionBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.LogCleanupIntervalMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.LogCleanupPolicyProp => assertPropertyInvalid(baseProperties, name, "unknown_policy", "0")
        case KafkaConfig.LogCleanerIoMaxBytesPerSecondProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.LogCleanerDedupeBufferSizeProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "1024")
        case KafkaConfig.LogCleanerDedupeBufferLoadFactorProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.LogCleanerEnableProp => assertPropertyInvalid(baseProperties, name, "not_a_boolean")
        case KafkaConfig.LogCleanerDeleteRetentionMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.LogCleanerMinCompactionLagMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.LogCleanerMaxCompactionLagMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.LogCleanerMinCleanRatioProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.LogIndexSizeMaxBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "3")
        case KafkaConfig.LogFlushIntervalMessagesProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.LogFlushSchedulerIntervalMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.LogFlushIntervalMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.LogMessageTimestampDifferenceMaxMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.LogFlushStartOffsetCheckpointIntervalMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.NumRecoveryThreadsPerDataDirProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.AutoCreateTopicsEnableProp => assertPropertyInvalid(baseProperties, name, "not_a_boolean", "0")
        case KafkaConfig.MinInSyncReplicasProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.ControllerSocketTimeoutMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.DefaultReplicationFactorProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ReplicaLagTimeMaxMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ReplicaSocketTimeoutMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "-2")
        case KafkaConfig.ReplicaSocketReceiveBufferBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ReplicaFetchMaxBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ReplicaFetchWaitMaxMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ReplicaFetchMinBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ReplicaFetchResponseMaxBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ReplicaSelectorClassProp => // Ignore string
        case KafkaConfig.NumReplicaFetchersProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.FetchPurgatoryPurgeIntervalRequestsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ProducerPurgatoryPurgeIntervalRequestsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.DeleteRecordsPurgatoryPurgeIntervalRequestsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.AutoLeaderRebalanceEnableProp => assertPropertyInvalid(baseProperties, name, "not_a_boolean", "0")
        case KafkaConfig.LeaderImbalancePerBrokerPercentageProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.UncleanLeaderElectionEnableProp => assertPropertyInvalid(baseProperties, name, "not_a_boolean", "0")
        case KafkaConfig.ControlledShutdownMaxRetriesProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ControlledShutdownRetryBackoffMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.ControlledShutdownEnableProp => assertPropertyInvalid(baseProperties, name, "not_a_boolean", "0")
        case KafkaConfig.GroupMinSessionTimeoutMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.GroupMaxSessionTimeoutMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.GroupInitialRebalanceDelayMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.GroupMaxSizeProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-1")
        case KafkaConfig.OffsetMetadataMaxSizeProp => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case KafkaConfig.OffsetsLoadBufferSizeProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.OffsetsTopicReplicationFactorProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.OffsetsTopicPartitionsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.OffsetsTopicSegmentBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.OffsetsTopicCompressionCodecProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "-1")
        case KafkaConfig.OffsetsRetentionMinutesProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.OffsetsRetentionCheckIntervalMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.OffsetCommitTimeoutMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.OffsetCommitRequiredAcksProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "-2")
        case KafkaConfig.TransactionalIdExpirationMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case KafkaConfig.TransactionsMaxTimeoutMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case KafkaConfig.TransactionsTopicMinISRProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case KafkaConfig.TransactionsLoadBufferSizeProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case KafkaConfig.TransactionsTopicPartitionsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case KafkaConfig.TransactionsTopicSegmentBytesProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case KafkaConfig.TransactionsTopicReplicationFactorProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0", "-2")
        case KafkaConfig.NumQuotaSamplesProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.QuotaWindowSizeSecondsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.DeleteTopicEnableProp => assertPropertyInvalid(baseProperties, name, "not_a_boolean", "0")

        case KafkaConfig.MetricNumSamplesProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "-1", "0")
        case KafkaConfig.MetricSampleWindowMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "-1", "0")
        case KafkaConfig.MetricReporterClassesProp => // ignore string
        case KafkaConfig.MetricRecordingLevelProp => // ignore string
        case KafkaConfig.RackProp => // ignore string
        //SSL Configs
        case KafkaConfig.PrincipalBuilderClassProp =>
        case KafkaConfig.ConnectionsMaxReauthMsProp =>
        case KafkaConfig.SslProtocolProp => // ignore string
        case KafkaConfig.SslProviderProp => // ignore string
        case KafkaConfig.SslEnabledProtocolsProp =>
        case KafkaConfig.SslKeystoreTypeProp => // ignore string
        case KafkaConfig.SslKeystoreLocationProp => // ignore string
        case KafkaConfig.SslKeystorePasswordProp => // ignore string
        case KafkaConfig.SslKeyPasswordProp => // ignore string
        case KafkaConfig.SslKeystoreCertificateChainProp => // ignore string
        case KafkaConfig.SslKeystoreKeyProp => // ignore string
        case KafkaConfig.SslTruststoreTypeProp => // ignore string
        case KafkaConfig.SslTruststorePasswordProp => // ignore string
        case KafkaConfig.SslTruststoreLocationProp => // ignore string
        case KafkaConfig.SslTruststoreCertificatesProp => // ignore string
        case KafkaConfig.SslKeyManagerAlgorithmProp =>
        case KafkaConfig.SslTrustManagerAlgorithmProp =>
        case KafkaConfig.SslClientAuthProp => // ignore string
        case KafkaConfig.SslEndpointIdentificationAlgorithmProp => // ignore string
        case KafkaConfig.SslSecureRandomImplementationProp => // ignore string
        case KafkaConfig.SslCipherSuitesProp => // ignore string
        case KafkaConfig.SslPrincipalMappingRulesProp => // ignore string

        //Sasl Configs
        case KafkaConfig.SaslMechanismControllerProtocolProp => // ignore
        case KafkaConfig.SaslMechanismInterBrokerProtocolProp => // ignore
        case KafkaConfig.SaslEnabledMechanismsProp =>
        case KafkaConfig.SaslClientCallbackHandlerClassProp =>
        case KafkaConfig.SaslServerCallbackHandlerClassProp =>
        case KafkaConfig.SaslLoginClassProp =>
        case KafkaConfig.SaslLoginCallbackHandlerClassProp =>
        case KafkaConfig.SaslKerberosServiceNameProp => // ignore string
        case KafkaConfig.SaslKerberosKinitCmdProp =>
        case KafkaConfig.SaslKerberosTicketRenewWindowFactorProp =>
        case KafkaConfig.SaslKerberosTicketRenewJitterProp =>
        case KafkaConfig.SaslKerberosMinTimeBeforeReloginProp =>
        case KafkaConfig.SaslKerberosPrincipalToLocalRulesProp => // ignore string
        case KafkaConfig.SaslJaasConfigProp =>
        case KafkaConfig.SaslLoginRefreshWindowFactorProp =>
        case KafkaConfig.SaslLoginRefreshWindowJitterProp =>
        case KafkaConfig.SaslLoginRefreshMinPeriodSecondsProp =>
        case KafkaConfig.SaslLoginRefreshBufferSecondsProp =>
        case KafkaConfig.SaslLoginConnectTimeoutMsProp =>
        case KafkaConfig.SaslLoginReadTimeoutMsProp =>
        case KafkaConfig.SaslLoginRetryBackoffMaxMsProp =>
        case KafkaConfig.SaslLoginRetryBackoffMsProp =>
        case KafkaConfig.SaslOAuthBearerScopeClaimNameProp =>
        case KafkaConfig.SaslOAuthBearerSubClaimNameProp =>
        case KafkaConfig.SaslOAuthBearerTokenEndpointUrlProp =>
        case KafkaConfig.SaslOAuthBearerJwksEndpointUrlProp =>
        case KafkaConfig.SaslOAuthBearerJwksEndpointRefreshMsProp =>
        case KafkaConfig.SaslOAuthBearerJwksEndpointRetryBackoffMaxMsProp =>
        case KafkaConfig.SaslOAuthBearerJwksEndpointRetryBackoffMsProp =>
        case KafkaConfig.SaslOAuthBearerClockSkewSecondsProp =>
        case KafkaConfig.SaslOAuthBearerExpectedAudienceProp =>
        case KafkaConfig.SaslOAuthBearerExpectedIssuerProp =>

        // Security config
        case KafkaConfig.securityProviderClassProp =>

        // Password encoder configs
        case KafkaConfig.PasswordEncoderSecretProp =>
        case KafkaConfig.PasswordEncoderOldSecretProp =>
        case KafkaConfig.PasswordEncoderKeyFactoryAlgorithmProp =>
        case KafkaConfig.PasswordEncoderCipherAlgorithmProp =>
        case KafkaConfig.PasswordEncoderKeyLengthProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "-1", "0")
        case KafkaConfig.PasswordEncoderIterationsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "-1", "0")

        //delegation token configs
        case KafkaConfig.DelegationTokenSecretKeyAliasProp => // ignore
        case KafkaConfig.DelegationTokenSecretKeyProp => // ignore
        case KafkaConfig.DelegationTokenMaxLifeTimeProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.DelegationTokenExpiryTimeMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")
        case KafkaConfig.DelegationTokenExpiryCheckIntervalMsProp => assertPropertyInvalid(baseProperties, name, "not_a_number", "0")

        //Kafka Yammer metrics reporter configs
        case KafkaConfig.KafkaMetricsReporterClassesProp => // ignore
        case KafkaConfig.KafkaMetricsPollingIntervalSecondsProp => //ignore

        case KafkaConfig.SaslServerMaxReceiveSizeProp => assertPropertyInvalid(baseProperties, name, "not_a_number")

        // Raft Quorum Configs
        case RaftConfig.QUORUM_VOTERS_CONFIG => // ignore string
        case RaftConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case RaftConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case RaftConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case RaftConfig.QUORUM_LINGER_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case RaftConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")
        case RaftConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number")

        // Remote Log Manager Configs
        case RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP => assertPropertyInvalid(baseProperties, name, "not_a_boolean")
        case RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP => // ignore string
        case RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", -1, 0.51)
        case RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)
        case RemoteLogManagerConfig.REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -1)

        case TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -2)
        case TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG => assertPropertyInvalid(baseProperties, name, "not_a_number", 0, -2)

        case _ => assertPropertyInvalid(baseProperties, name, "not_a_number", "-1")
      }
    }
  }

  @nowarn("cat=deprecation")
  @Test
  def testDynamicLogConfigs(): Unit = {
    def baseProperties: Properties = {
      val validRequiredProperties = new Properties()
      validRequiredProperties.setProperty(KafkaConfig.ZkConnectProp, "127.0.0.1:2181")
      validRequiredProperties
    }

    val props = baseProperties
    val config = KafkaConfig.fromProps(props)

    def assertDynamic(property: String, value: Any, accessor: () => Any): Unit = {
      val initial = accessor()
      props.setProperty(property, value.toString)
      config.updateCurrentConfig(new KafkaConfig(props))
      assertNotEquals(initial, accessor())
    }

    // Test dynamic log config values can be correctly passed through via KafkaConfig to LogConfig
    // Every log config prop must be explicitly accounted for here.
    // A value other than the default value for this config should be set to ensure that we can check whether
    // the value is dynamically updatable.
    LogConfig.TopicConfigSynonyms.foreach { case (logConfig, kafkaConfigProp) =>
      logConfig match {
        case LogConfig.CleanupPolicyProp =>
          assertDynamic(kafkaConfigProp, Defaults.Compact, () => config.logCleanupPolicy)
        case LogConfig.CompressionTypeProp =>
          assertDynamic(kafkaConfigProp, "lz4", () => config.compressionType)
        case LogConfig.SegmentBytesProp =>
          assertDynamic(kafkaConfigProp, 10000, () => config.logSegmentBytes)
        case LogConfig.SegmentMsProp =>
          assertDynamic(kafkaConfigProp, 10001L, () => config.logRollTimeMillis)
        case LogConfig.DeleteRetentionMsProp =>
          assertDynamic(kafkaConfigProp, 10002L, () => config.logCleanerDeleteRetentionMs)
        case LogConfig.FileDeleteDelayMsProp =>
          assertDynamic(kafkaConfigProp, 10003L, () => config.logDeleteDelayMs)
        case LogConfig.FlushMessagesProp =>
          assertDynamic(kafkaConfigProp, 10004L, () => config.logFlushIntervalMessages)
        case LogConfig.FlushMsProp =>
          assertDynamic(kafkaConfigProp, 10005L, () => config.logFlushIntervalMs)
        case LogConfig.MaxCompactionLagMsProp =>
          assertDynamic(kafkaConfigProp, 10006L, () => config.logCleanerMaxCompactionLagMs)
        case LogConfig.IndexIntervalBytesProp =>
          assertDynamic(kafkaConfigProp, 10007, () => config.logIndexIntervalBytes)
        case LogConfig.MaxMessageBytesProp =>
          assertDynamic(kafkaConfigProp, 10008, () => config.messageMaxBytes)
        case LogConfig.MessageDownConversionEnableProp =>
          assertDynamic(kafkaConfigProp, false, () => config.logMessageDownConversionEnable)
        case LogConfig.MessageTimestampDifferenceMaxMsProp =>
          assertDynamic(kafkaConfigProp, 10009, () => config.logMessageTimestampDifferenceMaxMs)
        case LogConfig.MessageTimestampTypeProp =>
          assertDynamic(kafkaConfigProp, "LogAppendTime", () => config.logMessageTimestampType.name)
        case LogConfig.MinCleanableDirtyRatioProp =>
          assertDynamic(kafkaConfigProp, 0.01, () => config.logCleanerMinCleanRatio)
        case LogConfig.MinCompactionLagMsProp =>
          assertDynamic(kafkaConfigProp, 10010L, () => config.logCleanerMinCompactionLagMs)
        case LogConfig.MinInSyncReplicasProp =>
          assertDynamic(kafkaConfigProp, 4, () => config.minInSyncReplicas)
        case LogConfig.PreAllocateEnableProp =>
          assertDynamic(kafkaConfigProp, true, () => config.logPreAllocateEnable)
        case LogConfig.RetentionBytesProp =>
          assertDynamic(kafkaConfigProp, 10011L, () => config.logRetentionBytes)
        case LogConfig.RetentionMsProp =>
          assertDynamic(kafkaConfigProp, 10012L, () => config.logRetentionTimeMillis)
        case LogConfig.SegmentIndexBytesProp =>
          assertDynamic(kafkaConfigProp, 10013, () => config.logIndexSizeMaxBytes)
        case LogConfig.SegmentJitterMsProp =>
          assertDynamic(kafkaConfigProp, 10014L, () => config.logRollTimeJitterMillis)
        case LogConfig.UncleanLeaderElectionEnableProp =>
          assertDynamic(kafkaConfigProp, true, () => config.uncleanLeaderElectionEnable)
        case LogConfig.MessageFormatVersionProp =>
        // not dynamically updatable
        case LogConfig.FollowerReplicationThrottledReplicasProp =>
        // topic only config
        case LogConfig.LeaderReplicationThrottledReplicasProp =>
        // topic only config
        case prop =>
          fail(prop + " must be explicitly checked for dynamic updatability. Note that LogConfig(s) require that KafkaConfig value lookups are dynamic and not static values.")
      }
    }
  }

  @Test
  def testSpecificProperties(): Unit = {
    val defaults = new Properties()
    defaults.setProperty(KafkaConfig.ZkConnectProp, "127.0.0.1:2181")
    // For ZkConnectionTimeoutMs
    defaults.setProperty(KafkaConfig.ZkSessionTimeoutMsProp, "1234")
    defaults.setProperty(KafkaConfig.BrokerIdGenerationEnableProp, "false")
    defaults.setProperty(KafkaConfig.MaxReservedBrokerIdProp, "1")
    defaults.setProperty(KafkaConfig.BrokerIdProp, "1")
    defaults.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://127.0.0.1:1122")
    defaults.setProperty(KafkaConfig.MaxConnectionsPerIpOverridesProp, "127.0.0.1:2, 127.0.0.2:3")
    defaults.setProperty(KafkaConfig.LogDirProp, "/tmp1,/tmp2")
    defaults.setProperty(KafkaConfig.LogRollTimeHoursProp, "12")
    defaults.setProperty(KafkaConfig.LogRollTimeJitterHoursProp, "11")
    defaults.setProperty(KafkaConfig.LogRetentionTimeHoursProp, "10")
    //For LogFlushIntervalMsProp
    defaults.setProperty(KafkaConfig.LogFlushSchedulerIntervalMsProp, "123")
    defaults.setProperty(KafkaConfig.OffsetsTopicCompressionCodecProp, CompressionType.SNAPPY.id.toString)
    // For MetricRecordingLevelProp
    defaults.setProperty(KafkaConfig.MetricRecordingLevelProp, Sensor.RecordingLevel.DEBUG.toString)

    val config = KafkaConfig.fromProps(defaults)
    assertEquals("127.0.0.1:2181", config.zkConnect)
    assertEquals(1234, config.zkConnectionTimeoutMs)
    assertEquals(false, config.brokerIdGenerationEnable)
    assertEquals(1, config.maxReservedBrokerId)
    assertEquals(1, config.brokerId)
    assertEquals(Seq("PLAINTEXT://127.0.0.1:1122"), config.effectiveAdvertisedListeners.map(_.connectionString))
    assertEquals(Map("127.0.0.1" -> 2, "127.0.0.2" -> 3), config.maxConnectionsPerIpOverrides)
    assertEquals(List("/tmp1", "/tmp2"), config.logDirs)
    assertEquals(12 * 60L * 1000L * 60, config.logRollTimeMillis)
    assertEquals(11 * 60L * 1000L * 60, config.logRollTimeJitterMillis)
    assertEquals(10 * 60L * 1000L * 60, config.logRetentionTimeMillis)
    assertEquals(123L, config.logFlushIntervalMs)
    assertEquals(CompressionType.SNAPPY, config.offsetsTopicCompressionType)
    assertEquals(Sensor.RecordingLevel.DEBUG.toString, config.metricRecordingLevel)
    assertEquals(false, config.tokenAuthEnabled)
    assertEquals(7 * 24 * 60L * 60L * 1000L, config.delegationTokenMaxLifeMs)
    assertEquals(24 * 60L * 60L * 1000L, config.delegationTokenExpiryTimeMs)
    assertEquals(1 * 60L * 1000L * 60, config.delegationTokenExpiryCheckIntervalMs)

    defaults.setProperty(KafkaConfig.DelegationTokenSecretKeyProp, "1234567890")
    val config1 = KafkaConfig.fromProps(defaults)
    assertEquals(true, config1.tokenAuthEnabled)
  }

  @Test
  def testNonroutableAdvertisedListeners(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ZkConnectProp, "127.0.0.1:2181")
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://0.0.0.0:9092")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testMaxConnectionsPerIpProp(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.MaxConnectionsPerIpProp, "0")
    assertFalse(isValidKafkaConfig(props))
    props.setProperty(KafkaConfig.MaxConnectionsPerIpOverridesProp, "127.0.0.1:100")
    KafkaConfig.fromProps(props)
    props.setProperty(KafkaConfig.MaxConnectionsPerIpOverridesProp, "127.0.0.0#:100")
    assertFalse(isValidKafkaConfig(props))
  }

  private def assertPropertyInvalid(validRequiredProps: => Properties, name: String, values: Any*): Unit = {
    values.foreach { value =>
      val props = validRequiredProps
      props.setProperty(name, value.toString)

      val buildConfig: Executable = () => KafkaConfig.fromProps(props)
      assertThrows(classOf[Exception], buildConfig,
      s"Expected exception for property `$name` with invalid value `$value` was not thrown")
    }
  }

  @Test
  def testDistinctControllerAndAdvertisedListenersAllowedForKRaftBroker(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094")
    props.setProperty(KafkaConfig.AdvertisedListenersProp, "PLAINTEXT://A:9092,SSL://B:9093") // explicitly setting it in KRaft
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SASL_SSL")
    props.setProperty(KafkaConfig.NodeIdProp, "2")
    props.setProperty(KafkaConfig.QuorumVotersProp, "3@localhost:9094")

    // invalid due to extra listener also appearing in controller listeners
    assertBadConfigContainingMessage(props,
      "controller.listener.names must not contain a value appearing in the 'listeners' configuration when running KRaft with just the broker role")

    // Valid now
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://A:9092,SSL://B:9093")
    KafkaConfig.fromProps(props)

    // Also valid if we let advertised listeners be derived from listeners/controller.listener.names
    // since listeners and advertised.listeners are explicitly identical at this point
    props.remove(KafkaConfig.AdvertisedListenersProp)
    KafkaConfig.fromProps(props)
  }

  @Test
  def testControllerListenersCannotBeAdvertisedForKRaftBroker(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker,controller")
    val listeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094"
    props.setProperty(KafkaConfig.ListenersProp, listeners)
    props.setProperty(KafkaConfig.AdvertisedListenersProp, listeners) // explicitly setting it in KRaft
    props.setProperty(KafkaConfig.InterBrokerListenerNameProp, "SASL_SSL")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "PLAINTEXT,SSL")
    props.setProperty(KafkaConfig.NodeIdProp, "2")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9092")
    assertBadConfigContainingMessage(props,
      "The advertised.listeners config must not contain KRaft controller listeners from controller.listener.names when process.roles contains the broker role")

    // Valid now
    props.setProperty(KafkaConfig.AdvertisedListenersProp, "SASL_SSL://C:9094")
    KafkaConfig.fromProps(props)

    // Also valid if we allow advertised listeners to derive from listeners/controller.listener.names
    props.remove(KafkaConfig.AdvertisedListenersProp)
    KafkaConfig.fromProps(props)
  }

  @Test
  def testAdvertisedListenersDisallowedForKRaftControllerOnlyRole(): Unit = {
    // Test that advertised listeners cannot be set when KRaft and server is controller only.
    // Test that listeners must enumerate every controller listener
    // Test that controller listener must enumerate every listener
    val correctListeners = "PLAINTEXT://A:9092,SSL://B:9093"
    val incorrectListeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094"

    val correctControllerListenerNames = "PLAINTEXT,SSL"

    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "controller")
    props.setProperty(KafkaConfig.ListenersProp, correctListeners)
    props.setProperty(KafkaConfig.AdvertisedListenersProp, incorrectListeners)
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, correctControllerListenerNames)
    props.setProperty(KafkaConfig.NodeIdProp, "2")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9092")
    var expectedExceptionContainsText = "The advertised.listeners config must be empty when process.roles=controller"
    assertBadConfigContainingMessage(props, expectedExceptionContainsText)

    // Invalid if advertised listeners is explicitly to the set
    props.setProperty(KafkaConfig.AdvertisedListenersProp, correctListeners)
    assertBadConfigContainingMessage(props, expectedExceptionContainsText)

    // Invalid if listeners contains names not in controller.listener.names
    props.remove(KafkaConfig.AdvertisedListenersProp)
    props.setProperty(KafkaConfig.ListenersProp, incorrectListeners)
    expectedExceptionContainsText = """The listeners config must only contain KRaft controller listeners from
    |controller.listener.names when process.roles=controller""".stripMargin.replaceAll("\n", " ")
    assertBadConfigContainingMessage(props, expectedExceptionContainsText)

    // Invalid if listeners doesn't contain every name in controller.listener.names
    props.setProperty(KafkaConfig.ListenersProp, correctListeners)
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, correctControllerListenerNames + ",SASL_SSL")
    expectedExceptionContainsText = """controller.listener.names must only contain values appearing in the 'listeners'
    |configuration when running the KRaft controller role""".stripMargin.replaceAll("\n", " ")
    assertBadConfigContainingMessage(props, expectedExceptionContainsText)

    // Valid now
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, correctControllerListenerNames)
    KafkaConfig.fromProps(props)
  }

  @Test
  def testControllerQuorumVoterStringsToNodes(): Unit = {
    assertThrows(classOf[ConfigException], () => RaftConfig.quorumVoterStringsToNodes(Collections.singletonList("")))
    assertEquals(Seq(new Node(3000, "example.com", 9093)),
      RaftConfig.quorumVoterStringsToNodes(util.Arrays.asList("3000@example.com:9093")).asScala.toSeq)
    assertEquals(Seq(new Node(3000, "example.com", 9093),
      new Node(3001, "example.com", 9094)),
      RaftConfig.quorumVoterStringsToNodes(util.Arrays.asList("3000@example.com:9093","3001@example.com:9094")).asScala.toSeq)
  }

  @Test
  def testInvalidQuorumVoterConfig(): Unit = {
    assertInvalidQuorumVoters("1")
    assertInvalidQuorumVoters("1@")
    assertInvalidQuorumVoters("1:")
    assertInvalidQuorumVoters("blah@")
    assertInvalidQuorumVoters("1@kafka1")
    assertInvalidQuorumVoters("1@kafka1:9092,")
    assertInvalidQuorumVoters("1@kafka1:9092,")
    assertInvalidQuorumVoters("1@kafka1:9092,2")
    assertInvalidQuorumVoters("1@kafka1:9092,2@")
    assertInvalidQuorumVoters("1@kafka1:9092,2@blah")
    assertInvalidQuorumVoters("1@kafka1:9092,2@blah,")
    assertInvalidQuorumVoters("1@kafka1:9092:1@kafka2:9092")
  }

  private def assertInvalidQuorumVoters(value: String): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.setProperty(KafkaConfig.QuorumVotersProp, value)
    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
  }

  @Test
  def testValidQuorumVotersConfig(): Unit = {
    val expected = new util.HashMap[Integer, AddressSpec]()
    assertValidQuorumVoters("", expected)

    expected.put(1, new InetAddressSpec(new InetSocketAddress("127.0.0.1", 9092)))
    assertValidQuorumVoters("1@127.0.0.1:9092", expected)

    expected.clear()
    expected.put(1, UNKNOWN_ADDRESS_SPEC_INSTANCE)
    assertValidQuorumVoters("1@0.0.0.0:0", expected)

    expected.clear()
    expected.put(1, new InetAddressSpec(new InetSocketAddress("kafka1", 9092)))
    expected.put(2, new InetAddressSpec(new InetSocketAddress("kafka2", 9092)))
    expected.put(3, new InetAddressSpec(new InetSocketAddress("kafka3", 9092)))
    assertValidQuorumVoters("1@kafka1:9092,2@kafka2:9092,3@kafka3:9092", expected)
  }

  private def assertValidQuorumVoters(value: String, expectedVoters: util.Map[Integer, AddressSpec]): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.setProperty(KafkaConfig.QuorumVotersProp, value)
    val raftConfig = new RaftConfig(KafkaConfig.fromProps(props))
    assertEquals(expectedVoters, raftConfig.quorumVoterConnections())
  }

  @Test
  def testAcceptsLargeNodeIdForRaftBasedCase(): Unit = {
    // Generation of Broker IDs is not supported when using Raft-based controller quorums,
    // so pick a broker ID greater than reserved.broker.max.id, which defaults to 1000,
    // and make sure it is allowed despite broker.id.generation.enable=true (true is the default)
    val largeBrokerId = 2000
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093")
    props.setProperty(KafkaConfig.NodeIdProp, largeBrokerId.toString)
    KafkaConfig.fromProps(props)
  }

  @Test
  def testRejectsNegativeNodeIdForRaftBasedBrokerCaseWithAutoGenEnabled(): Unit = {
    // -1 is the default for both node.id and broker.id
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testRejectsNegativeNodeIdForRaftBasedControllerCaseWithAutoGenEnabled(): Unit = {
    // -1 is the default for both node.id and broker.id
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "controller")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testRejectsNegativeNodeIdForRaftBasedCaseWithAutoGenDisabled(): Unit = {
    // -1 is the default for both node.id and broker.id
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    props.setProperty(KafkaConfig.BrokerIdGenerationEnableProp, "false")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testRejectsLargeNodeIdForZkBasedCaseWithAutoGenEnabled(): Unit = {
    // Generation of Broker IDs is supported when using ZooKeeper-based controllers,
    // so pick a broker ID greater than reserved.broker.max.id, which defaults to 1000,
    // and make sure it is not allowed with broker.id.generation.enable=true (true is the default)
    val largeBrokerId = 2000
    val props = TestUtils.createBrokerConfig(largeBrokerId, TestUtils.MockZkConnect, port = TestUtils.MockZkPort)
    val listeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094"
    props.setProperty(KafkaConfig.ListenersProp, listeners)
    props.setProperty(KafkaConfig.AdvertisedListenersProp, listeners)
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testAcceptsNegativeOneNodeIdForZkBasedCaseWithAutoGenEnabled(): Unit = {
    // -1 is the default for both node.id and broker.id; it implies "auto-generate" and should succeed
    val props = TestUtils.createBrokerConfig(-1, TestUtils.MockZkConnect, port = TestUtils.MockZkPort)
    val listeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094"
    props.setProperty(KafkaConfig.ListenersProp, listeners)
    props.setProperty(KafkaConfig.AdvertisedListenersProp, listeners)
    KafkaConfig.fromProps(props)
  }

  @Test
  def testRejectsNegativeTwoNodeIdForZkBasedCaseWithAutoGenEnabled(): Unit = {
    // -1 implies "auto-generate" and should succeed, but -2 does not and should fail
    val negativeTwoNodeId = -2
    val props = TestUtils.createBrokerConfig(negativeTwoNodeId, TestUtils.MockZkConnect, port = TestUtils.MockZkPort)
    val listeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094"
    props.setProperty(KafkaConfig.ListenersProp, listeners)
    props.setProperty(KafkaConfig.AdvertisedListenersProp, listeners)
    props.setProperty(KafkaConfig.NodeIdProp, negativeTwoNodeId.toString)
    props.setProperty(KafkaConfig.BrokerIdProp, negativeTwoNodeId.toString)
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testAcceptsLargeNodeIdForZkBasedCaseWithAutoGenDisabled(): Unit = {
    // Ensure a broker ID greater than reserved.broker.max.id, which defaults to 1000,
    // is allowed with broker.id.generation.enable=false
    val largeBrokerId = 2000
    val props = TestUtils.createBrokerConfig(largeBrokerId, TestUtils.MockZkConnect, port = TestUtils.MockZkPort)
    val listeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094"
    props.setProperty(KafkaConfig.ListenersProp, listeners)
    props.setProperty(KafkaConfig.AdvertisedListenersProp, listeners)
    props.setProperty(KafkaConfig.BrokerIdGenerationEnableProp, "false")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testRejectsNegativeNodeIdForZkBasedCaseWithAutoGenDisabled(): Unit = {
    // -1 is the default for both node.id and broker.id
    val props = TestUtils.createBrokerConfig(-1, TestUtils.MockZkConnect, port = TestUtils.MockZkPort)
    val listeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094"
    props.setProperty(KafkaConfig.ListenersProp, listeners)
    props.setProperty(KafkaConfig.BrokerIdGenerationEnableProp, "false")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testZookeeperConnectRequiredIfEmptyProcessRoles(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "")
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://127.0.0.1:9092")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testZookeeperConnectNotRequiredIfNonEmptyProcessRoles(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://127.0.0.1:9092")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    props.setProperty(KafkaConfig.NodeIdProp, "1")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testCustomMetadataLogDir(): Unit = {
    val metadataDir = "/path/to/metadata/dir"
    val dataDir = "/path/to/data/dir"

    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    props.setProperty(KafkaConfig.MetadataLogDirProp, metadataDir)
    props.setProperty(KafkaConfig.LogDirProp, dataDir)
    props.setProperty(KafkaConfig.NodeIdProp, "1")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093")
    KafkaConfig.fromProps(props)

    val config = KafkaConfig.fromProps(props)
    assertEquals(metadataDir, config.metadataLogDir)
    assertEquals(Seq(dataDir), config.logDirs)
  }

  @Test
  def testDefaultMetadataLogDir(): Unit = {
    val dataDir1 = "/path/to/data/dir/1"
    val dataDir2 = "/path/to/data/dir/2"

    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    props.setProperty(KafkaConfig.LogDirProp, s"$dataDir1,$dataDir2")
    props.setProperty(KafkaConfig.NodeIdProp, "1")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093")
    KafkaConfig.fromProps(props)

    val config = KafkaConfig.fromProps(props)
    assertEquals(dataDir1, config.metadataLogDir)
    assertEquals(Seq(dataDir1, dataDir2), config.logDirs)
  }

  @Test
  def testPopulateSynonymsOnEmptyMap(): Unit = {
    assertEquals(Collections.emptyMap(), KafkaConfig.populateSynonyms(Collections.emptyMap()))
  }

  @Test
  def testPopulateSynonymsOnMapWithoutNodeId(): Unit = {
    val input =  new util.HashMap[String, String]()
    input.put(KafkaConfig.BrokerIdProp, "4")
    val expectedOutput = new util.HashMap[String, String]()
    expectedOutput.put(KafkaConfig.BrokerIdProp, "4")
    expectedOutput.put(KafkaConfig.NodeIdProp, "4")
    assertEquals(expectedOutput, KafkaConfig.populateSynonyms(input))
  }

  @Test
  def testPopulateSynonymsOnMapWithoutBrokerId(): Unit = {
    val input =  new util.HashMap[String, String]()
    input.put(KafkaConfig.NodeIdProp, "4")
    val expectedOutput = new util.HashMap[String, String]()
    expectedOutput.put(KafkaConfig.BrokerIdProp, "4")
    expectedOutput.put(KafkaConfig.NodeIdProp, "4")
    assertEquals(expectedOutput, KafkaConfig.populateSynonyms(input))
  }

  @Test
  def testNodeIdMustNotBeDifferentThanBrokerId(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.BrokerIdProp, "1")
    props.setProperty(KafkaConfig.NodeIdProp, "2")
    assertEquals("You must set `node.id` to the same value as `broker.id`.",
      assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props)).getMessage())
  }

  @Test
  def testNodeIdOrBrokerIdMustBeSetWithKraft(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093")
    assertEquals("Missing configuration `node.id` which is required when `process.roles` " +
      "is defined (i.e. when running in KRaft mode).",
      assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props)).getMessage())
  }

  @Test
  def testNodeIdIsInferredByBrokerIdWithKraft(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    props.setProperty(KafkaConfig.BrokerIdProp, "3")
    props.setProperty(KafkaConfig.QuorumVotersProp, "2@localhost:9093")
    val config = KafkaConfig.fromProps(props)
    assertEquals(3, config.brokerId)
    assertEquals(3, config.nodeId)
    val originals = config.originals()
    assertEquals("3", originals.get(KafkaConfig.BrokerIdProp))
    assertEquals("3", originals.get(KafkaConfig.NodeIdProp))
  }

  def kraftProps(): Properties = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "broker")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
    props.setProperty(KafkaConfig.NodeIdProp, "3")
    props.setProperty(KafkaConfig.QuorumVotersProp, "1@localhost:9093")
    props
  }

  @Test
  def testBrokerIdIsInferredByNodeIdWithKraft(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())
    val config = KafkaConfig.fromProps(props)
    assertEquals(3, config.brokerId)
    assertEquals(3, config.nodeId)
    val originals = config.originals()
    assertEquals("3", originals.get(KafkaConfig.BrokerIdProp))
    assertEquals("3", originals.get(KafkaConfig.NodeIdProp))
  }

  @Test
  def testSaslJwksEndpointRetryDefaults(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ZkConnectProp, "localhost:2181")
    val config = KafkaConfig.fromProps(props)
    assertNotNull(config.getLong(KafkaConfig.SaslOAuthBearerJwksEndpointRetryBackoffMsProp))
    assertNotNull(config.getLong(KafkaConfig.SaslOAuthBearerJwksEndpointRetryBackoffMaxMsProp))
  }

  @Test
  def testInvalidAuthorizerClassName(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val configs = new util.HashMap[Object, Object](props)
    configs.put(KafkaConfig.AuthorizerClassNameProp, null)
    val ce = assertThrows(classOf[ConfigException], () => KafkaConfig.apply(configs))
    assertTrue(ce.getMessage.contains(KafkaConfig.AuthorizerClassNameProp))
  }

  @Test
  def testInvalidSecurityInterBrokerProtocol(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.setProperty(KafkaConfig.InterBrokerSecurityProtocolProp, "abc")
    val ce = assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
    assertTrue(ce.getMessage.contains(KafkaConfig.InterBrokerSecurityProtocolProp))
  }

  @Test
  def testEarlyStartListenersDefault(): Unit = {
    val props = new Properties()
    props.setProperty(KafkaConfig.ProcessRolesProp, "controller")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
    props.setProperty(KafkaConfig.ListenersProp, "CONTROLLER://:8092")
    props.setProperty(KafkaConfig.NodeIdProp, "1")
    props.setProperty(KafkaConfig.QuorumVotersProp, "1@localhost:9093")
    val config = new KafkaConfig(props)
    assertEquals(Set("CONTROLLER"), config.earlyStartListeners.map(_.value()))
  }

  @Test
  def testEarlyStartListeners(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())
    props.setProperty(KafkaConfig.EarlyStartListenersProp, "INTERNAL,INTERNAL2")
    props.setProperty(KafkaConfig.InterBrokerListenerNameProp, "INTERNAL")
    props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp,
      "INTERNAL:PLAINTEXT,INTERNAL2:PLAINTEXT,CONTROLLER:PLAINTEXT")
    props.setProperty(KafkaConfig.ListenersProp,
      "INTERNAL://127.0.0.1:9092,INTERNAL2://127.0.0.1:9093")
    val config = new KafkaConfig(props)
    assertEquals(Set(new ListenerName("INTERNAL"), new ListenerName("INTERNAL2")),
      config.earlyStartListeners)
  }

  @Test
  def testEarlyStartListenersMustBeListeners(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())
    props.setProperty(KafkaConfig.EarlyStartListenersProp, "INTERNAL")
    assertEquals("early.start.listeners contains listener INTERNAL, but this is not " +
      "contained in listeners or controller.listener.names",
        assertThrows(classOf[ConfigException], () => new KafkaConfig(props)).getMessage)
  }

  @Test
  def testIgnoreUserInterBrokerProtocolVersionKRaft(): Unit = {
    for (ibp <- Seq("3.0", "3.1", "3.2")) {
      val props = new Properties()
      props.putAll(kraftProps())
      props.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, ibp)
      val config = new KafkaConfig(props)
      assertEquals(config.interBrokerProtocolVersion, MetadataVersion.MINIMUM_KRAFT_VERSION)
    }
  }

  @Test
  def testInvalidInterBrokerProtocolVersionKRaft(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())
    props.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, "2.8")
    assertEquals("A non-KRaft version 2.8 given for inter.broker.protocol.version. The minimum version is 3.0-IV1",
      assertThrows(classOf[ConfigException], () => new KafkaConfig(props)).getMessage)
  }

  @Test
  def testDefaultInterBrokerProtocolVersionKRaft(): Unit = {
    val props = new Properties()
    props.putAll(kraftProps())
    val config = KafkaConfig.fromProps(props)
    assertEquals(config.interBrokerProtocolVersion, MetadataVersion.MINIMUM_KRAFT_VERSION)
  }

  @Test
  def testMetadataMaxSnapshotInterval(): Unit = {
    val validValue = 100
    val props = new Properties()
    props.putAll(kraftProps())
    props.setProperty(KafkaConfig.MetadataSnapshotMaxIntervalMsProp, validValue.toString)

    val config = KafkaConfig.fromProps(props)
    assertEquals(validValue, config.metadataSnapshotMaxIntervalMs)

    props.setProperty(KafkaConfig.MetadataSnapshotMaxIntervalMsProp, "-1")
    val errorMessage = assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props)).getMessage

    assertEquals(
      "Invalid value -1 for configuration metadata.log.max.snapshot.interval.ms: Value must be at least 0",
      errorMessage
    )
  }
}
