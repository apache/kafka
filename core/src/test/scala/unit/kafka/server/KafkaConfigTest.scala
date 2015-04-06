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

import junit.framework.Assert._
import kafka.api.{ApiVersion, KAFKA_082}
import kafka.utils.{TestUtils, CoreUtils}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.Test
import org.scalatest.junit.JUnit3Suite

class KafkaConfigTest extends JUnit3Suite {

  @Test
  def testLogRetentionTimeHoursProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put("log.retention.hours", "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(60L * 60L * 1000L, cfg.logRetentionTimeMillis)
  }
  
  @Test
  def testLogRetentionTimeMinutesProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put("log.retention.minutes", "30")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }
  
  @Test
  def testLogRetentionTimeMsProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put("log.retention.ms", "1800000")

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
    props.put("log.retention.minutes", "30")
    props.put("log.retention.hours", "1")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }
  
  @Test
  def testLogRetentionTimeBothMinutesAndMsProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put("log.retention.ms", "1800000")
    props.put("log.retention.minutes", "10")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRetentionTimeMillis)
  }

  @Test
  def testAdvertiseDefaults() {
    val port = "9999"
    val hostName = "fake-host"

    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.remove("listeners")
    props.put("host.name", hostName)
    props.put("port", port)
    val serverConfig = KafkaConfig.fromProps(props)
    val endpoints = serverConfig.advertisedListeners
    val endpoint = endpoints.get(SecurityProtocol.PLAINTEXT).get
    assertEquals(endpoint.host, hostName)
    assertEquals(endpoint.port, port.toInt)
  }

  @Test
  def testAdvertiseConfigured() {
    val port = "9999"
    val advertisedHostName = "routable-host"
    val advertisedPort = "1234"

    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put("advertised.host.name", advertisedHostName)
    props.put("advertised.port", advertisedPort.toString)

    val serverConfig = KafkaConfig.fromProps(props)
    val endpoints = serverConfig.advertisedListeners
    val endpoint = endpoints.get(SecurityProtocol.PLAINTEXT).get
    
    assertEquals(endpoint.host, advertisedHostName)
    assertEquals(endpoint.port, advertisedPort.toInt)
  }


  @Test
  def testDuplicateListeners() {
    val props = new Properties()
    props.put("broker.id", "1")
    props.put("zookeeper.connect", "localhost:2181")

    // listeners with duplicate port
    props.put("listeners", "PLAINTEXT://localhost:9091,TRACE://localhost:9091")
    assert(!isValidKafkaConfig(props))

    // listeners with duplicate protocol
    props.put("listeners", "PLAINTEXT://localhost:9091,PLAINTEXT://localhost:9092")
    assert(!isValidKafkaConfig(props))

    // advertised listeners with duplicate port
    props.put("advertised,listeners", "PLAINTEXT://localhost:9091,TRACE://localhost:9091")
    assert(!isValidKafkaConfig(props))
  }

  @Test
  def testBadListenerProtocol() {
    val props = new Properties()
    props.put("broker.id", "1")
    props.put("zookeeper.connect", "localhost:2181")
    props.put("listeners", "BAD://localhost:9091")

    assert(!isValidKafkaConfig(props))
  }

  @Test
  def testListenerDefaults() {
    val props = new Properties()
    props.put("broker.id", "1")
    props.put("zookeeper.connect", "localhost:2181")

    // configuration with host and port, but no listeners
    props.put("host.name", "myhost")
    props.put("port", "1111")

    val conf = KafkaConfig.fromProps(props)
    assertEquals(CoreUtils.listenerListToEndPoints("PLAINTEXT://myhost:1111"), conf.listeners)

    // configuration with null host
    props.remove("host.name")

    val conf2 = KafkaConfig.fromProps(props)
    assertEquals(CoreUtils.listenerListToEndPoints("PLAINTEXT://:1111"), conf2.listeners)
    assertEquals(CoreUtils.listenerListToEndPoints("PLAINTEXT://:1111"), conf2.advertisedListeners)
    assertEquals(null, conf2.listeners(SecurityProtocol.PLAINTEXT).host)

    // configuration with advertised host and port, and no advertised listeners
    props.put("advertised.host.name", "otherhost")
    props.put("advertised.port", "2222")

    val conf3 = KafkaConfig.fromProps(props)
    assertEquals(conf3.advertisedListeners, CoreUtils.listenerListToEndPoints("PLAINTEXT://otherhost:2222"))
  }

  @Test
  def testVersionConfiguration() {
    val props = new Properties()
    props.put("broker.id", "1")
    props.put("zookeeper.connect", "localhost:2181")
    val conf = KafkaConfig.fromProps(props)
    assertEquals(ApiVersion.latestVersion, conf.interBrokerProtocolVersion)

    props.put("inter.broker.protocol.version","0.8.2.0")
    val conf2 = KafkaConfig.fromProps(props)
    assertEquals(KAFKA_082, conf2.interBrokerProtocolVersion)

    // check that 0.8.2.0 is the same as 0.8.2.1
    props.put("inter.broker.protocol.version","0.8.2.1")
    val conf3 = KafkaConfig.fromProps(props)
    assertEquals(KAFKA_082, conf3.interBrokerProtocolVersion)

    //check that latest is newer than 0.8.2
    assert(ApiVersion.latestVersion.onOrAfter(conf3.interBrokerProtocolVersion))
  }

  private def isValidKafkaConfig(props: Properties): Boolean = {
    try {
      KafkaConfig.fromProps(props)
      true
    } catch {
      case e: IllegalArgumentException => false
    }
  }

  @Test
  def testUncleanLeaderElectionDefault() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, true)
  }

  @Test
  def testUncleanElectionDisabled() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put("unclean.leader.election.enable", String.valueOf(false))
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, false)
  }

  @Test
  def testUncleanElectionEnabled() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put("unclean.leader.election.enable", String.valueOf(true))
    val serverConfig = KafkaConfig.fromProps(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, true)
  }

  @Test
  def testUncleanElectionInvalid() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put("unclean.leader.election.enable", "invalid")

    intercept[ConfigException] {
      KafkaConfig.fromProps(props)
    }
  }
  
  @Test
  def testLogRollTimeMsProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put("log.roll.ms", "1800000")

    val cfg = KafkaConfig.fromProps(props)
    assertEquals(30 * 60L * 1000L, cfg.logRollTimeMillis)
  }
  
  @Test
  def testLogRollTimeBothMsAndHoursProvided() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put("log.roll.ms", "1800000")
    props.put("log.roll.hours", "1")

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
    props.put("compression.type", "abc")
    intercept[IllegalArgumentException] {
      KafkaConfig.fromProps(props)
    }
  }
}
