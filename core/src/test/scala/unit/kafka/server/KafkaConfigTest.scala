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

import org.junit.Test
import junit.framework.Assert._
import org.scalatest.junit.JUnit3Suite
import kafka.utils.TestUtils

class KafkaConfigTest extends JUnit3Suite {

  @Test
  def testLogRetentionTimeHoursProvided() {
    val props = TestUtils.createBrokerConfig(0, 8181)
    props.put("log.retention.hours", "1")

    val cfg = new KafkaConfig(props)
    assertEquals(60L * 60L * 1000L, cfg.logRetentionTimeMillis)

  }
  
  @Test
  def testLogRetentionTimeMinutesProvided() {
    val props = TestUtils.createBrokerConfig(0, 8181)
    props.put("log.retention.minutes", "30")

    val cfg = new KafkaConfig(props)
    assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis)

  }
  
  @Test
  def testLogRetentionTimeMsProvided() {
    val props = TestUtils.createBrokerConfig(0, 8181)
    props.put("log.retention.ms", "1800000")

    val cfg = new KafkaConfig(props)
    assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis)

  }
  
  @Test
  def testLogRetentionTimeNoConfigProvided() {
    val props = TestUtils.createBrokerConfig(0, 8181)

    val cfg = new KafkaConfig(props)
    assertEquals(24 * 7 * 60L * 60L * 1000L, cfg.logRetentionTimeMillis)

  }
  
  @Test
  def testLogRetentionTimeBothMinutesAndHoursProvided() {
    val props = TestUtils.createBrokerConfig(0, 8181)
    props.put("log.retention.minutes", "30")
    props.put("log.retention.hours", "1")

    val cfg = new KafkaConfig(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRetentionTimeMillis)

  }
  
  @Test
  def testLogRetentionTimeBothMinutesAndMsProvided() {
    val props = TestUtils.createBrokerConfig(0, 8181)
    props.put("log.retention.ms", "1800000")
    props.put("log.retention.minutes", "10")

    val cfg = new KafkaConfig(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRetentionTimeMillis)

  }

  @Test
  def testAdvertiseDefaults() {
    val port = 9999
    val hostName = "fake-host"
    
    val props = TestUtils.createBrokerConfig(0, port)
    props.put("host.name", hostName)
    
    val serverConfig = new KafkaConfig(props)
    
    assertEquals(serverConfig.advertisedHostName, hostName)
    assertEquals(serverConfig.advertisedPort, port)
  }

  @Test
  def testAdvertiseConfigured() {
    val port = 9999
    val advertisedHostName = "routable-host"
    val advertisedPort = 1234
    
    val props = TestUtils.createBrokerConfig(0, port)
    props.put("advertised.host.name", advertisedHostName)
    props.put("advertised.port", advertisedPort.toString)
    
    val serverConfig = new KafkaConfig(props)
    
    assertEquals(serverConfig.advertisedHostName, advertisedHostName)
    assertEquals(serverConfig.advertisedPort, advertisedPort)
  }

  @Test
  def testUncleanLeaderElectionDefault() {
    val props = TestUtils.createBrokerConfig(0, 8181)
    val serverConfig = new KafkaConfig(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, true)
  }

  @Test
  def testUncleanElectionDisabled() {
    val props = TestUtils.createBrokerConfig(0, 8181)
    props.put("unclean.leader.election.enable", String.valueOf(false))
    val serverConfig = new KafkaConfig(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, false)
  }

  @Test
  def testUncleanElectionEnabled() {
    val props = TestUtils.createBrokerConfig(0, 8181)
    props.put("unclean.leader.election.enable", String.valueOf(true))
    val serverConfig = new KafkaConfig(props)

    assertEquals(serverConfig.uncleanLeaderElectionEnable, true)
  }

  @Test
  def testUncleanElectionInvalid() {
    val props = TestUtils.createBrokerConfig(0, 8181)
    props.put("unclean.leader.election.enable", "invalid")

    intercept[IllegalArgumentException] {
      new KafkaConfig(props)
    }
  }
  
  @Test
  def testLogRollTimeMsProvided() {
    val props = TestUtils.createBrokerConfig(0, 8181)
    props.put("log.roll.ms", "1800000")

    val cfg = new KafkaConfig(props)
    assertEquals(30 * 60L * 1000L, cfg.logRollTimeMillis)

  }
  
  @Test
  def testLogRollTimeBothMsAndHoursProvided() {
    val props = TestUtils.createBrokerConfig(0, 8181)
    props.put("log.roll.ms", "1800000")
    props.put("log.roll.hours", "1")

    val cfg = new KafkaConfig(props)
    assertEquals( 30 * 60L * 1000L, cfg.logRollTimeMillis)

  }
    
  @Test
  def testLogRollTimeNoConfigProvided() {
    val props = TestUtils.createBrokerConfig(0, 8181)

    val cfg = new KafkaConfig(props)
    assertEquals(24 * 7 * 60L * 60L * 1000L, cfg.logRollTimeMillis																									)

  }
  

}
