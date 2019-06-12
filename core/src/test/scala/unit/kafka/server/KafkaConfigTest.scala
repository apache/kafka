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

import java.util.Properties

import kafka.api.{ApiVersion, KAFKA_0_8_2}
import kafka.cluster.EndPoint
import kafka.message._
import kafka.utils.{CoreUtils, TestUtils}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.intercept

class KafkaConfigTest {

  @Test
  def testLogRetentionTimeHoursProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRetentionTimeHoursProp, "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(60L * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeMinutesProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRetentionTimeMinutesProp, "30")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeMsProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRetentionTimeMillisProp, "1800000")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeNoConfigProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(24 * 7 * 60L * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeBothMinutesAndHoursProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRetentionTimeMinutesProp, "30")
    props.put(KafkaConfig.LogRetentionTimeHoursProp, "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionTimeBothMinutesAndMsProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRetentionTimeMillisProp, "1800000")
    props.put(KafkaConfig.LogRetentionTimeMinutesProp, "10")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testLogRetentionUnlimited() {
    val props1 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)
    val props2 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)
    val props3 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)
    val props4 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)
    val props5 = TestUtils.createBrokerConfig(0,TestUtils.MockZkConnect, port = 8181)

    props1.put("log.retention.ms", "-1")
    props2.put("log.retention.minutes", "-1")
    props3.put("log.retention.hours", "-1")

    val cfg1 = KafkaConfig.fromProps(props1)
    val cfg2 = KafkaConfig.fromProps(props2)
    val cfg3 = KafkaConfig.fromProps(props3)
    assertEquals("Should be -1", -1, cfg1.logRetentionTimeMillis)
    assertEquals("Should be -1", -1, cfg2.logRetentionTimeMillis)
    assertEquals("Should be -1", -1, cfg3.logRetentionTimeMillis)

    props4.put("log.retention.ms", "-1")
    props4.put("log.retention.minutes", "30")

    val cfg4 = KafkaConfig.fromProps(props4)
    assertEquals("Should be -1", -1, cfg4.logRetentionTimeMillis)

    props5.put("log.retention.ms", "0")

    intercept[IllegalArgumentException] {
      KafkaConfig.fromProps(props5)
    }
  }

  @Test
  def testLogRetentionValid(): Unit = {
    val props1 = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val props2 = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val props3 = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)

    props1.put("log.retention.ms", "0")
    props2.put("log.retention.minutes", "0")
    props3.put("log.retention.hours", "0")

    intercept[IllegalArgumentException] {
      KafkaConfig.fromProps(props1)
    }
    intercept[IllegalArgumentException] {
      KafkaConfig.fromProps(props2)
    }
    intercept[IllegalArgumentException] {
      KafkaConfig.fromProps(props3)
    }

  }

  @Test
  def testAdvertiseDefaults() {
    val port = "9999"
    val hostName = "fake-host"

    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.remove(KafkaConfig.ListenersProp)
    props.put(KafkaConfig.HostNameProp, hostName)
    props.put(KafkaConfig.PortProp, port)
    val serverConfig = KafkaConfig.fromProps(props)
    val endpoints = serverConfig.advertisedListeners
    val endpoint = endpoints.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).get
    assertEquals(endpoint.host, hostName)
    assertEquals(endpoint.port, port.toInt)
  }

  @Test
  def testAdvertiseConfigured() {
    val advertisedHostName = "routable-host"
    val advertisedPort = "1234"

    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put(KafkaConfig.AdvertisedHostNameProp, advertisedHostName)
    props.put(KafkaConfig.AdvertisedPortProp, advertisedPort)

    val serverConfig = KafkaConfig.fromProps(props)
    val endpoints = serverConfig.advertisedListeners
    val endpoint = endpoints.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).get

    assertEquals(endpoint.host, advertisedHostName)
    assertEquals(endpoint.port, advertisedPort.toInt)
  }

  @Test
  def testAdvertisePortDefault() {
    val advertisedHostName = "routable-host"
    val port = "9999"

    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put(KafkaConfig.AdvertisedHostNameProp, advertisedHostName)
    props.put(KafkaConfig.PortProp, port)

    val serverConfig = KafkaConfig.fromProps(props)
    val endpoints = serverConfig.advertisedListeners
    val endpoint = endpoints.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).get

    assertEquals(endpoint.host, advertisedHostName)
    assertEquals(endpoint.port, port.toInt)
  }

  @Test
  def testAdvertiseHostNameDefault() {
    val hostName = "routable-host"
    val advertisedPort = "9999"

    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put(KafkaConfig.HostNameProp, hostName)
    props.put(KafkaConfig.AdvertisedPortProp, advertisedPort)

    val serverConfig = KafkaConfig.fromProps(props)
    val endpoints = serverConfig.advertisedListeners
    val endpoint = endpoints.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).get

    assertEquals(endpoint.host, hostName)
    assertEquals(endpoint.port, advertisedPort.toInt)
  }

  @Test
  def testDuplicateListeners() {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")

    // listeners with duplicate port
    props.put(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9091,TRACE://localhost:9091")
    assertFalse(isValidKafkaConfig(props))

    // listeners with duplicate protocol
    props.put(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9091,PLAINTEXT://localhost:9092")
    assertFalse(isValidKafkaConfig(props))

    // advertised listeners with duplicate port
    props.put(KafkaConfig.AdvertisedListenersProp, "PLAINTEXT://localhost:9091,TRACE://localhost:9091")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testControlPlaneListenerName() = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put("listeners", "PLAINTEXT://localhost:0,CONTROLLER://localhost:5000")
    props.put("listener.security.protocol.map", "PLAINTEXT:PLAINTEXT,CONTROLLER:SSL")
    props.put("control.plane.listener.name", "CONTROLLER")
    assertTrue(isValidKafkaConfig(props))

    val serverConfig = KafkaConfig.fromProps(props)
    val controlEndpoint = serverConfig.controlPlaneListener.get
    assertEquals("localhost", controlEndpoint.host)
    assertEquals(5000, controlEndpoint.port)
    assertEquals(SecurityProtocol.SSL, controlEndpoint.securityProtocol)

    //advertised listener should contain control-plane listener
    val advertisedEndpoints = serverConfig.advertisedListeners
    assertFalse(advertisedEndpoints.filter { endpoint =>
      endpoint.securityProtocol == controlEndpoint.securityProtocol && endpoint.listenerName.value().equals(controlEndpoint.listenerName.value())
    }.isEmpty)

    // interBrokerListener name should be different from control-plane listener name
    val interBrokerListenerName = serverConfig.interBrokerListenerName
    assertFalse(interBrokerListenerName.value().equals(controlEndpoint.listenerName.value()))
  }

  @Test
  def testBadListenerProtocol() {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")
    props.put(KafkaConfig.ListenersProp, "BAD://localhost:9091")

    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testListenerNamesWithAdvertisedListenerUnset(): Unit = {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")

    props.put(KafkaConfig.ListenersProp, "CLIENT://localhost:9091,REPLICATION://localhost:9092,INTERNAL://localhost:9093")
    props.put(KafkaConfig.ListenerSecurityProtocolMapProp, "CLIENT:SSL,REPLICATION:SSL,INTERNAL:PLAINTEXT")
    props.put(KafkaConfig.InterBrokerListenerNameProp, "REPLICATION")
    val config = KafkaConfig.fromProps(props)
    val expectedListeners = Seq(
      EndPoint("localhost", 9091, new ListenerName("CLIENT"), SecurityProtocol.SSL),
      EndPoint("localhost", 9092, new ListenerName("REPLICATION"), SecurityProtocol.SSL),
      EndPoint("localhost", 9093, new ListenerName("INTERNAL"), SecurityProtocol.PLAINTEXT))
    assertEquals(expectedListeners, config.listeners)
    assertEquals(expectedListeners, config.advertisedListeners)
    val expectedSecurityProtocolMap = Map(
      new ListenerName("CLIENT") -> SecurityProtocol.SSL,
      new ListenerName("REPLICATION") -> SecurityProtocol.SSL,
      new ListenerName("INTERNAL") -> SecurityProtocol.PLAINTEXT
    )
    assertEquals(expectedSecurityProtocolMap, config.listenerSecurityProtocolMap)
  }

  @Test
  def testListenerAndAdvertisedListenerNames(): Unit = {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")

    props.put(KafkaConfig.ListenersProp, "EXTERNAL://localhost:9091,INTERNAL://localhost:9093")
    props.put(KafkaConfig.AdvertisedListenersProp, "EXTERNAL://lb1.example.com:9000,INTERNAL://host1:9093")
    props.put(KafkaConfig.ListenerSecurityProtocolMapProp, "EXTERNAL:SSL,INTERNAL:PLAINTEXT")
    props.put(KafkaConfig.InterBrokerListenerNameProp, "INTERNAL")
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
    assertEquals(expectedAdvertisedListeners, config.advertisedListeners)

    val expectedSecurityProtocolMap = Map(
      new ListenerName("EXTERNAL") -> SecurityProtocol.SSL,
      new ListenerName("INTERNAL") -> SecurityProtocol.PLAINTEXT
    )
    assertEquals(expectedSecurityProtocolMap, config.listenerSecurityProtocolMap)
  }

  @Test
  def testListenerNameMissingFromListenerSecurityProtocolMap(): Unit = {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")

    props.put(KafkaConfig.ListenersProp, "SSL://localhost:9091,REPLICATION://localhost:9092")
    props.put(KafkaConfig.InterBrokerListenerNameProp, "SSL")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testInterBrokerListenerNameMissingFromListenerSecurityProtocolMap(): Unit = {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")

    props.put(KafkaConfig.ListenersProp, "SSL://localhost:9091")
    props.put(KafkaConfig.InterBrokerListenerNameProp, "REPLICATION")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testInterBrokerListenerNameAndSecurityProtocolSet(): Unit = {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")

    props.put(KafkaConfig.ListenersProp, "SSL://localhost:9091")
    props.put(KafkaConfig.InterBrokerListenerNameProp, "SSL")
    props.put(KafkaConfig.InterBrokerSecurityProtocolProp, "SSL")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testCaseInsensitiveListenerProtocol() {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")
    props.put(KafkaConfig.ListenersProp, "plaintext://localhost:9091,SsL://localhost:9092")
    val config = KafkaConfig.fromProps(props)
    assertEquals(Some("SSL://localhost:9092"), config.listeners.find(_.listenerName.value == "SSL").map(_.connectionString))
    assertEquals(Some("PLAINTEXT://localhost:9091"), config.listeners.find(_.listenerName.value == "PLAINTEXT").map(_.connectionString))
  }

  def listenerListToEndPoints(listenerList: String,
                              securityProtocolMap: collection.Map[ListenerName, SecurityProtocol] = EndPoint.DefaultSecurityProtocolMap) =
    CoreUtils.listenerListToEndPoints(listenerList, securityProtocolMap)

  @Test
  def testListenerDefaults() {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")

    // configuration with host and port, but no listeners
    props.put(KafkaConfig.HostNameProp, "myhost")
    props.put(KafkaConfig.PortProp, "1111")

    val conf = KafkaConfig.fromProps(props)
    assertEquals(listenerListToEndPoints("PLAINTEXT://myhost:1111"), conf.listeners)

    // configuration with null host
    props.remove(KafkaConfig.HostNameProp)

    val conf2 = KafkaConfig.fromProps(props)
    assertEquals(listenerListToEndPoints("PLAINTEXT://:1111"), conf2.listeners)
    assertEquals(listenerListToEndPoints("PLAINTEXT://:1111"), conf2.advertisedListeners)
    assertEquals(null, conf2.listeners.find(_.securityProtocol == SecurityProtocol.PLAINTEXT).get.host)

    // configuration with advertised host and port, and no advertised listeners
    props.put(KafkaConfig.AdvertisedHostNameProp, "otherhost")
    props.put(KafkaConfig.AdvertisedPortProp, "2222")

    val conf3 = KafkaConfig.fromProps(props)
    assertEquals(conf3.advertisedListeners, listenerListToEndPoints("PLAINTEXT://otherhost:2222"))
  }

  @Test
  def testVersionConfiguration() {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, "1")
    props.put(KafkaConfig.ZkConnectProp, "localhost:2181")
    val conf = KafkaConfig.fromProps(props)
    assertEquals(ApiVersion.latestVersion, conf.interBrokerProtocolVersion)

    props.put(KafkaConfig.InterBrokerProtocolVersionProp, "0.8.2.0")
    // We need to set the message format version to make the configuration valid.
    props.put(KafkaConfig.LogMessageFormatVersionProp, "0.8.2.0")
    val conf2 = KafkaConfig.fromProps(props)
    assertEquals(KAFKA_0_8_2, conf2.interBrokerProtocolVersion)

    // check that 0.8.2.0 is the same as 0.8.2.1
    props.put(KafkaConfig.InterBrokerProtocolVersionProp, "0.8.2.1")
    // We need to set the message format version to make the configuration valid
    props.put(KafkaConfig.LogMessageFormatVersionProp, "0.8.2.1")
    val conf3 = KafkaConfig.fromProps(props)
    assertEquals(KAFKA_0_8_2, conf3.interBrokerProtocolVersion)

    //check that latest is newer than 0.8.2
    assertTrue(ApiVersion.latestVersion >= conf3.interBrokerProtocolVersion)
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
  def testUncleanLeaderElectionDefault() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, false)
  }

  @Test
  def testUncleanElectionDisabled() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.UncleanLeaderElectionEnableProp, String.valueOf(false))
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, false)
  }

  @Test
  def testUncleanElectionEnabled() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.UncleanLeaderElectionEnableProp, String.valueOf(true))
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, true)
  }

  @Test
  def testUncleanElectionInvalid() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.UncleanLeaderElectionEnableProp, "invalid")

    intercept[ConfigException] {
      KafkaConfig.fromProps(props)
    }
  }

  @Test
  def testLogRollTimeMsProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRollTimeMillisProp, "1800000")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRollTimeMillis)
  }

  @Test
  def testLogRollTimeBothMsAndHoursProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LogRollTimeMillisProp, "1800000")
    props.put(KafkaConfig.LogRollTimeHoursProp, "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRollTimeMillis)
  }

  @Test
  def testLogRollTimeNoConfigProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(24 * 7 * 60L * 60L * 1000L, cfg.logRollTimeMillis																									)
  }

  @Test
  def testDefaultCompressionType() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.compressionType, "producer")
  }

  @Test
  def testValidCompressionType() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put("compression.type", "gzip")
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.compressionType, "gzip")
  }

  @Test
  def testInvalidCompressionType() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.CompressionTypeProp, "abc")
    intercept[IllegalArgumentException] {
      KafkaConfig.fromProps(props)
    }
  }

  @Test
  def testInvalidInterBrokerSecurityProtocol() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.ListenersProp, "SSL://localhost:0")
    props.put(KafkaConfig.InterBrokerSecurityProtocolProp, SecurityProtocol.PLAINTEXT.toString)
    intercept[IllegalArgumentException] {
      KafkaConfig.fromProps(props)
    }
  }

  @Test
  def testEqualAdvertisedListenersProtocol() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092,SSL://localhost:9093")
    props.put(KafkaConfig.AdvertisedListenersProp, "PLAINTEXT://localhost:9092,SSL://localhost:9093")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testInvalidAdvertisedListenersProtocol() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.ListenersProp, "TRACE://localhost:9091,SSL://localhost:9093")
    props.put(KafkaConfig.AdvertisedListenersProp, "PLAINTEXT://localhost:9092")
    intercept[IllegalArgumentException] {
      KafkaConfig.fromProps(props)
    }
  }

  @Test
  def testInterBrokerVersionMessageFormatCompatibility(): Unit = {
    def buildConfig(interBrokerProtocol: ApiVersion, messageFormat: ApiVersion): KafkaConfig = {
      val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
      props.put(KafkaConfig.InterBrokerProtocolVersionProp, interBrokerProtocol.version)
      props.put(KafkaConfig.LogMessageFormatVersionProp, messageFormat.version)
      KafkaConfig.fromProps(props)
    }

    ApiVersion.allVersions.foreach { interBrokerVersion =>
      ApiVersion.allVersions.foreach { messageFormatVersion =>
        if (interBrokerVersion.recordVersion.value >= messageFormatVersion.recordVersion.value) {
          val config = buildConfig(interBrokerVersion, messageFormatVersion)
          assertEquals(messageFormatVersion, config.logMessageFormatVersion)
          assertEquals(interBrokerVersion, config.interBrokerProtocolVersion)
        } else {
          intercept[IllegalArgumentException] {
            buildConfig(interBrokerVersion, messageFormatVersion)
          }
        }
      }
    }
  }

  @Test
  def testFromPropsInvalid() {
    def getBaseProperties(): Properties = {
      val validRequiredProperties = new Properties()
      validRequiredProperties.put(KafkaConfig.ZkConnectProp, "127.0.0.1:2181")
      validRequiredProperties
    }
    // to ensure a basis is valid - bootstraps all needed validation
    KafkaConfig.fromProps(getBaseProperties())

    KafkaConfig.configNames().foreach(name => {
      name match {
        case KafkaConfig.ZkConnectProp => // ignore string
        case KafkaConfig.ZkSessionTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ZkConnectionTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ZkSyncTimeMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ZkEnableSecureAclsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_boolean")
        case KafkaConfig.ZkMaxInFlightRequestsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")

        case KafkaConfig.BrokerIdProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.NumNetworkThreadsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.NumIoThreadsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.BackgroundThreadsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.QueuedMaxRequestsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.NumReplicaAlterLogDirsThreadsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.QueuedMaxBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.RequestTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")

        case KafkaConfig.AuthorizerClassNameProp => //ignore string
        case KafkaConfig.CreateTopicPolicyClassNameProp => //ignore string

        case KafkaConfig.PortProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.HostNameProp => // ignore string
        case KafkaConfig.AdvertisedHostNameProp => //ignore string
        case KafkaConfig.AdvertisedPortProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.SocketSendBufferBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.SocketReceiveBufferBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.MaxConnectionsPerIpOverridesProp =>
          assertPropertyInvalid(getBaseProperties(), name, "127.0.0.1:not_a_number")
        case KafkaConfig.ConnectionsMaxIdleMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.FailedAuthenticationDelayMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "-1")

        case KafkaConfig.NumPartitionsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.LogDirsProp => // ignore string
        case KafkaConfig.LogDirProp => // ignore string
        case KafkaConfig.LogSegmentBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", Records.LOG_OVERHEAD - 1)

        case KafkaConfig.LogRollTimeMillisProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.LogRollTimeHoursProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")

        case KafkaConfig.LogRetentionTimeMillisProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.LogRetentionTimeMinutesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.LogRetentionTimeHoursProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")

        case KafkaConfig.LogRetentionBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogCleanupIntervalMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.LogCleanupPolicyProp => assertPropertyInvalid(getBaseProperties(), name, "unknown_policy", "0")
        case KafkaConfig.LogCleanerIoMaxBytesPerSecondProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogCleanerDedupeBufferSizeProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "1024")
        case KafkaConfig.LogCleanerDedupeBufferLoadFactorProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogCleanerEnableProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_boolean")
        case KafkaConfig.LogCleanerDeleteRetentionMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogCleanerMinCompactionLagMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogCleanerMaxCompactionLagMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogCleanerMinCleanRatioProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogIndexSizeMaxBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "3")
        case KafkaConfig.LogFlushIntervalMessagesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.LogFlushSchedulerIntervalMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogFlushIntervalMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogMessageTimestampDifferenceMaxMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LogFlushStartOffsetCheckpointIntervalMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.NumRecoveryThreadsPerDataDirProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.AutoCreateTopicsEnableProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_boolean", "0")
        case KafkaConfig.MinInSyncReplicasProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.ControllerSocketTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.DefaultReplicationFactorProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaLagTimeMaxMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaSocketTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "-2")
        case KafkaConfig.ReplicaSocketReceiveBufferBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaFetchMaxBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaFetchWaitMaxMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaFetchMinBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaFetchResponseMaxBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.NumReplicaFetchersProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.FetchPurgatoryPurgeIntervalRequestsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ProducerPurgatoryPurgeIntervalRequestsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.DeleteRecordsPurgatoryPurgeIntervalRequestsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.AutoLeaderRebalanceEnableProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_boolean", "0")
        case KafkaConfig.LeaderImbalancePerBrokerPercentageProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.UncleanLeaderElectionEnableProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_boolean", "0")
        case KafkaConfig.ControlledShutdownMaxRetriesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ControlledShutdownRetryBackoffMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.ControlledShutdownEnableProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_boolean", "0")
        case KafkaConfig.GroupMinSessionTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.GroupMaxSessionTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.GroupInitialRebalanceDelayMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.GroupMaxSizeProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0", "-1")
        case KafkaConfig.OffsetMetadataMaxSizeProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number")
        case KafkaConfig.OffsetsLoadBufferSizeProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetsTopicReplicationFactorProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetsTopicPartitionsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetsTopicSegmentBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetsTopicCompressionCodecProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "-1")
        case KafkaConfig.OffsetsRetentionMinutesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetsRetentionCheckIntervalMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetCommitTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.OffsetCommitRequiredAcksProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "-2")
        case KafkaConfig.TransactionalIdExpirationMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0", "-2")
        case KafkaConfig.TransactionsMaxTimeoutMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0", "-2")
        case KafkaConfig.TransactionsTopicMinISRProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0", "-2")
        case KafkaConfig.TransactionsLoadBufferSizeProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0", "-2")
        case KafkaConfig.TransactionsTopicPartitionsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0", "-2")
        case KafkaConfig.TransactionsTopicSegmentBytesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0", "-2")
        case KafkaConfig.TransactionsTopicReplicationFactorProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0", "-2")
        case KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.NumQuotaSamplesProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.QuotaWindowSizeSecondsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.DeleteTopicEnableProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_boolean", "0")

        case KafkaConfig.MetricNumSamplesProp => assertPropertyInvalid(getBaseProperties, name, "not_a_number", "-1", "0")
        case KafkaConfig.MetricSampleWindowMsProp => assertPropertyInvalid(getBaseProperties, name, "not_a_number", "-1", "0")
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
        case KafkaConfig.SslTruststoreTypeProp => // ignore string
        case KafkaConfig.SslTruststorePasswordProp => // ignore string
        case KafkaConfig.SslTruststoreLocationProp => // ignore string
        case KafkaConfig.SslKeyManagerAlgorithmProp =>
        case KafkaConfig.SslTrustManagerAlgorithmProp =>
        case KafkaConfig.SslClientAuthProp => // ignore string
        case KafkaConfig.SslEndpointIdentificationAlgorithmProp => // ignore string
        case KafkaConfig.SslSecureRandomImplementationProp => // ignore string
        case KafkaConfig.SslCipherSuitesProp => // ignore string
        case KafkaConfig.SslPrincipalMappingRulesProp => // ignore string

        //Sasl Configs
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

        // Password encoder configs
        case KafkaConfig.PasswordEncoderSecretProp =>
        case KafkaConfig.PasswordEncoderOldSecretProp =>
        case KafkaConfig.PasswordEncoderKeyFactoryAlgorithmProp =>
        case KafkaConfig.PasswordEncoderCipherAlgorithmProp =>
        case KafkaConfig.PasswordEncoderKeyLengthProp => assertPropertyInvalid(getBaseProperties, name, "not_a_number", "-1", "0")
        case KafkaConfig.PasswordEncoderIterationsProp => assertPropertyInvalid(getBaseProperties, name, "not_a_number", "-1", "0")

        //delegation token configs
        case KafkaConfig.DelegationTokenMasterKeyProp => // ignore
        case KafkaConfig.DelegationTokenMaxLifeTimeProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.DelegationTokenExpiryTimeMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")
        case KafkaConfig.DelegationTokenExpiryCheckIntervalMsProp => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "0")

        //Kafka Yammer metrics reporter configs
        case KafkaConfig.KafkaMetricsReporterClassesProp => // ignore
        case KafkaConfig.KafkaMetricsPollingIntervalSecondsProp => //ignore

        case _ => assertPropertyInvalid(getBaseProperties(), name, "not_a_number", "-1")
      }
    })
  }

  @Test
  def testSpecificProperties(): Unit = {
    val defaults = new Properties()
    defaults.put(KafkaConfig.ZkConnectProp, "127.0.0.1:2181")
    // For ZkConnectionTimeoutMs
    defaults.put(KafkaConfig.ZkSessionTimeoutMsProp, "1234")
    defaults.put(KafkaConfig.BrokerIdGenerationEnableProp, "false")
    defaults.put(KafkaConfig.MaxReservedBrokerIdProp, "1")
    defaults.put(KafkaConfig.BrokerIdProp, "1")
    defaults.put(KafkaConfig.HostNameProp, "127.0.0.1")
    defaults.put(KafkaConfig.PortProp, "1122")
    defaults.put(KafkaConfig.MaxConnectionsPerIpOverridesProp, "127.0.0.1:2, 127.0.0.2:3")
    defaults.put(KafkaConfig.LogDirProp, "/tmp1,/tmp2")
    defaults.put(KafkaConfig.LogRollTimeHoursProp, "12")
    defaults.put(KafkaConfig.LogRollTimeJitterHoursProp, "11")
    defaults.put(KafkaConfig.LogRetentionTimeHoursProp, "10")
    //For LogFlushIntervalMsProp
    defaults.put(KafkaConfig.LogFlushSchedulerIntervalMsProp, "123")
    defaults.put(KafkaConfig.OffsetsTopicCompressionCodecProp, SnappyCompressionCodec.codec.toString)
    // For MetricRecordingLevelProp
    defaults.put(KafkaConfig.MetricRecordingLevelProp, Sensor.RecordingLevel.DEBUG.toString)

    val config = KafkaConfig.fromProps(defaults)
    assertEquals("127.0.0.1:2181", config.zkConnect)
    assertEquals(1234, config.zkConnectionTimeoutMs)
    assertEquals(false, config.brokerIdGenerationEnable)
    assertEquals(1, config.maxReservedBrokerId)
    assertEquals(1, config.brokerId)
    assertEquals("127.0.0.1", config.hostName)
    assertEquals(1122, config.advertisedPort)
    assertEquals("127.0.0.1", config.advertisedHostName)
    assertEquals(Map("127.0.0.1" -> 2, "127.0.0.2" -> 3), config.maxConnectionsPerIpOverrides)
    assertEquals(List("/tmp1", "/tmp2"), config.logDirs)
    assertEquals(12 * 60L * 1000L * 60, config.logRollTimeMillis)
    assertEquals(11 * 60L * 1000L * 60, config.logRollTimeJitterMillis)
    assertEquals(10 * 60L * 1000L * 60, config.logRetentionTimeMillis)
    assertEquals(123L, config.logFlushIntervalMs)
    assertEquals(SnappyCompressionCodec, config.offsetsTopicCompressionCodec)
    assertEquals(Sensor.RecordingLevel.DEBUG.toString, config.metricRecordingLevel)
    assertEquals(false, config.tokenAuthEnabled)
    assertEquals(7 * 24 * 60L * 60L * 1000L, config.delegationTokenMaxLifeMs)
    assertEquals(24 * 60L * 60L * 1000L, config.delegationTokenExpiryTimeMs)
    assertEquals(1 * 60L * 1000L * 60, config.delegationTokenExpiryCheckIntervalMs)

    defaults.put(KafkaConfig.DelegationTokenMasterKeyProp, "1234567890")
    val config1 = KafkaConfig.fromProps(defaults)
    assertEquals(true, config1.tokenAuthEnabled)
  }

  @Test
  def testNonroutableAdvertisedListeners() {
    val props = new Properties()
    props.put(KafkaConfig.ZkConnectProp, "127.0.0.1:2181")
    props.put(KafkaConfig.ListenersProp, "PLAINTEXT://0.0.0.0:9092")
    assertFalse(isValidKafkaConfig(props))
  }

  @Test
  def testMaxConnectionsPerIpProp() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.MaxConnectionsPerIpProp, "0")
    assertFalse(isValidKafkaConfig(props))
    props.put(KafkaConfig.MaxConnectionsPerIpOverridesProp, "127.0.0.1:100")
    assertTrue(isValidKafkaConfig(props))
    props.put(KafkaConfig.MaxConnectionsPerIpOverridesProp, "127.0.0.0#:100")
    assertFalse(isValidKafkaConfig(props))
  }

  private def assertPropertyInvalid(validRequiredProps: => Properties, name: String, values: Any*) {
    values.foreach((value) => {
      val props = validRequiredProps
      props.setProperty(name, value.toString)
      intercept[Exception] {
        KafkaConfig.fromProps(props)
      }
    })
  }

}
