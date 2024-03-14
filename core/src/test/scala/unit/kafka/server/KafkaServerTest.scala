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

import kafka.utils.TestUtils
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.server.config.KafkaConfig._
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertThrows, fail}
import org.junit.jupiter.api.Test
import java.util.Properties

import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig

class KafkaServerTest extends QuorumTestHarness {

  @Test
  def testAlreadyRegisteredAdvertisedListeners(): Unit = {
    //start a server with a advertised listener
    val server1 = createServer(1, "myhost", TestUtils.RandomPort)

    //start a server with same advertised listener
    assertThrows(classOf[IllegalArgumentException], () => createServer(2, "myhost", TestUtils.boundPort(server1)))

    //start a server with same host but with different port
    val server2 = createServer(2, "myhost", TestUtils.RandomPort)

    TestUtils.shutdownServers(Seq(server1, server2))
  }

  @Test
  def testCreatesProperZkConfigWhenSaslDisabled(): Unit = {
    val props = new Properties
    props.put(ZK_CONNECT_PROP, zkConnect) // required, otherwise we would leave it out
    val zkClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    assertEquals("false", zkClientConfig.getProperty(JaasUtils.ZK_SASL_CLIENT))
  }

  @Test
  def testCreatesProperZkTlsConfigWhenDisabled(): Unit = {
    val props = new Properties
    props.put(ZK_CONNECT_PROP, zkConnect) // required, otherwise we would leave it out
    props.put(ZK_SSL_CLIENT_ENABLE_PROP, "false")
    val zkClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    zkSslConfigToSystemPropertyMap.keySet().forEach { propName =>
      assertNull(zkClientConfig.getProperty(propName))
    }
  }

  @Test
  def testCreatesProperZkTlsConfigWithTrueValues(): Unit = {
    val props = new Properties
    props.put(ZK_CONNECT_PROP, zkConnect) // required, otherwise we would leave it out
    // should get correct config for all properties if TLS is enabled
    val someValue = "some_value"
    def kafkaConfigValueToSet(kafkaProp: String) : String = kafkaProp match {
      case ZK_SSL_CLIENT_ENABLE_PROP | ZK_SSL_CRL_ENABLE_PROP | ZK_SSL_OCSP_ENABLE_PROP => "true"
      case ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP => "HTTPS"
      case _ => someValue
    }
    zkSslConfigToSystemPropertyMap.keySet().forEach(kafkaProp => props.put(kafkaProp, kafkaConfigValueToSet(kafkaProp)))
    val zkClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    // now check to make sure the values were set correctly
    def zkClientValueToExpect(kafkaProp: String) : String = kafkaProp match {
      case ZK_SSL_CLIENT_ENABLE_PROP | ZK_SSL_CRL_ENABLE_PROP | ZK_SSL_OCSP_ENABLE_PROP => "true"
      case ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP => "true"
      case _ => someValue
    }
    zkSslConfigToSystemPropertyMap.keySet().forEach(kafkaProp =>
      assertEquals(zkClientValueToExpect(kafkaProp), zkClientConfig.getProperty(zkSslConfigToSystemPropertyMap.get(kafkaProp))))
  }

  @Test
  def testCreatesProperZkTlsConfigWithFalseAndListValues(): Unit = {
    val props = new Properties
    props.put(ZK_CONNECT_PROP, zkConnect) // required, otherwise we would leave it out
    // should get correct config for all properties if TLS is enabled
    val someValue = "some_value"
    def kafkaConfigValueToSet(kafkaProp: String) : String = kafkaProp match {
      case ZK_SSL_CLIENT_ENABLE_PROP => "true"
      case ZK_SSL_CRL_ENABLE_PROP | ZK_SSL_OCSP_ENABLE_PROP => "false"
      case ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP => ""
      case ZK_SSL_ENABLED_PROTOCOLS_PROP | ZK_SSL_CIPHER_SUITES_PROP => "A,B"
      case _ => someValue
    }
    zkSslConfigToSystemPropertyMap.keySet().forEach(kafkaProp => props.put(kafkaProp, kafkaConfigValueToSet(kafkaProp)))
    val zkClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    // now check to make sure the values were set correctly
    def zkClientValueToExpect(kafkaProp: String) : String = kafkaProp match {
      case ZK_SSL_CLIENT_ENABLE_PROP => "true"
      case ZK_SSL_CRL_ENABLE_PROP | ZK_SSL_OCSP_ENABLE_PROP => "false"
      case ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP => "false"
      case ZK_SSL_ENABLED_PROTOCOLS_PROP | ZK_SSL_CIPHER_SUITES_PROP => "A,B"
      case _ => someValue
    }
    zkSslConfigToSystemPropertyMap.keySet().forEach(kafkaProp =>
      assertEquals(zkClientValueToExpect(kafkaProp), zkClientConfig.getProperty(zkSslConfigToSystemPropertyMap.get(kafkaProp))))
  }

  @Test
  def testZkIsrManager(): Unit = {
    val props = TestUtils.createBrokerConfigs(1, zkConnect).head
    props.put(INTER_BROKER_PROTOCOL_VERSION_PROP, "2.7-IV1")

    val server = TestUtils.createServer(KafkaConfig.fromProps(props))
    server.replicaManager.alterPartitionManager match {
      case _: ZkAlterPartitionManager =>
      case _ => fail("Should use ZK for ISR manager in versions before 2.7-IV2")
    }
    server.shutdown()
  }

  @Test
  def testAlterIsrManager(): Unit = {
    val props = TestUtils.createBrokerConfigs(1, zkConnect).head
    props.put(INTER_BROKER_PROTOCOL_VERSION_PROP, MetadataVersion.latestTesting.toString)

    val server = TestUtils.createServer(KafkaConfig.fromProps(props))
    server.replicaManager.alterPartitionManager match {
      case _: DefaultAlterPartitionManager =>
      case _ => fail("Should use AlterIsr for ISR manager in versions after 2.7-IV2")
    }
    server.shutdown()
  }

  @Test
  def testRemoteLogManagerInstantiation(): Unit = {
    val props = TestUtils.createBrokerConfigs(1, zkConnect).head
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
      "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager")
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
      "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")

    val server = TestUtils.createServer(KafkaConfig.fromProps(props))
    server.remoteLogManagerOpt match {
      case Some(_) =>
      case None => fail("RemoteLogManager should be initialized")
    }
    server.shutdown()
  }

  def createServer(nodeId: Int, hostName: String, port: Int): KafkaServer = {
    val props = TestUtils.createBrokerConfig(nodeId, zkConnect)
    props.put(ADVERTISED_LISTENERS_PROP, s"PLAINTEXT://$hostName:$port")
    val kafkaConfig = KafkaConfig.fromProps(props)
    TestUtils.createServer(kafkaConfig)
  }

}
