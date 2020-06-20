/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.Properties

import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.zookeeper.client.ZKClientConfig
import org.junit.Test
import org.junit.Assert.assertEquals
import org.scalatest.Assertions.intercept

class KafkaServerTest extends ZooKeeperTestHarness {

  @Test
  def testAlreadyRegisteredAdvertisedListeners(): Unit = {
    //start a server with a advertised listener
    val server1 = createServer(1, "myhost", TestUtils.RandomPort)

    //start a server with same advertised listener
    intercept[IllegalArgumentException] {
      createServer(2, "myhost", TestUtils.boundPort(server1))
    }

    //start a server with same host but with different port
    val server2 = createServer(2, "myhost", TestUtils.RandomPort)

    TestUtils.shutdownServers(Seq(server1, server2))
  }

  @Test
  def testCreatesProperZkTlsConfigWhenDisabled(): Unit = {
    val props = new Properties
    props.put(KafkaConfig.ZkConnectProp, zkConnect) // required, otherwise we would leave it out
    props.put(KafkaConfig.ZkSslClientEnableProp, "false")
    assertEquals(None, KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props)))
  }

  @Test
  def testCreatesProperZkTlsConfigWithTrueValues(): Unit = {
    val props = new Properties
    props.put(KafkaConfig.ZkConnectProp, zkConnect) // required, otherwise we would leave it out
    // should get correct config for all properties if TLS is enabled
    val someValue = "some_value"
    def kafkaConfigValueToSet(kafkaProp: String) : String = kafkaProp match {
      case KafkaConfig.ZkSslClientEnableProp | KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp => "true"
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp => "HTTPS"
      case _ => someValue
    }
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(kafkaProp => props.put(kafkaProp, kafkaConfigValueToSet(kafkaProp)))
    val zkClientConfig: Option[ZKClientConfig] = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    // now check to make sure the values were set correctly
    def zkClientValueToExpect(kafkaProp: String) : String = kafkaProp match {
      case KafkaConfig.ZkSslClientEnableProp | KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp => "true"
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp => "true"
      case _ => someValue
    }
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(kafkaProp =>
      assertEquals(zkClientValueToExpect(kafkaProp), zkClientConfig.get.getProperty(KafkaConfig.ZkSslConfigToSystemPropertyMap(kafkaProp))))
  }

  @Test
  def testCreatesProperZkTlsConfigWithFalseAndListValues(): Unit = {
    val props = new Properties
    props.put(KafkaConfig.ZkConnectProp, zkConnect) // required, otherwise we would leave it out
    // should get correct config for all properties if TLS is enabled
    val someValue = "some_value"
    def kafkaConfigValueToSet(kafkaProp: String) : String = kafkaProp match {
      case KafkaConfig.ZkSslClientEnableProp => "true"
      case KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp => "false"
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp => ""
      case KafkaConfig.ZkSslEnabledProtocolsProp | KafkaConfig.ZkSslCipherSuitesProp => "A,B"
      case _ => someValue
    }
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(kafkaProp => props.put(kafkaProp, kafkaConfigValueToSet(kafkaProp)))
    val zkClientConfig: Option[ZKClientConfig] = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    // now check to make sure the values were set correctly
    def zkClientValueToExpect(kafkaProp: String) : String = kafkaProp match {
      case KafkaConfig.ZkSslClientEnableProp => "true"
      case KafkaConfig.ZkSslCrlEnableProp | KafkaConfig.ZkSslOcspEnableProp => "false"
      case KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp => "false"
      case KafkaConfig.ZkSslEnabledProtocolsProp | KafkaConfig.ZkSslCipherSuitesProp => "A,B"
      case _ => someValue
    }
    KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.foreach(kafkaProp =>
      assertEquals(zkClientValueToExpect(kafkaProp), zkClientConfig.get.getProperty(KafkaConfig.ZkSslConfigToSystemPropertyMap(kafkaProp))))
  }

  def createServer(nodeId: Int, hostName: String, port: Int): KafkaServer = {
    val props = TestUtils.createBrokerConfig(nodeId, zkConnect)
    props.put(KafkaConfig.AdvertisedListenersProp, s"PLAINTEXT://$hostName:$port")
    val kafkaConfig = KafkaConfig.fromProps(props)
    TestUtils.createServer(kafkaConfig)
  }

}
