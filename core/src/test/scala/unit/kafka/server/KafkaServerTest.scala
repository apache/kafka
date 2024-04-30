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

import kafka.utils.{CoreUtils, TestUtils}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.server.config.ReplicationConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertThrows, fail}
import org.junit.jupiter.api.Test

import java.util.Properties
import java.net.{InetAddress, ServerSocket}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.ZkConfigs
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig

import scala.jdk.CollectionConverters._

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
  def testListenerPortAlreadyInUse(): Unit = {
    val serverSocket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress)

    var kafkaServer : Option[KafkaServer] = None
    try {
      TestUtils.waitUntilTrue(() => serverSocket.isBound, "Server socket failed to bind.")
      // start a server with listener on the port already bound
      assertThrows(classOf[RuntimeException],
        () => kafkaServer = Option(createServerWithListenerOnPort(serverSocket.getLocalPort)),
        "Expected RuntimeException due to address already in use during KafkaServer startup"
      )
    } finally {
      CoreUtils.swallow(serverSocket.close(), this)
      TestUtils.shutdownServers(kafkaServer.toList)
    }
  }

  @Test
  def testCreatesProperZkConfigWhenSaslDisabled(): Unit = {
    val props = new Properties
    props.put(ZkConfigs.ZK_CONNECT_CONFIG, zkConnect) // required, otherwise we would leave it out
    val zkClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    assertEquals("false", zkClientConfig.getProperty(JaasUtils.ZK_SASL_CLIENT))
  }

  @Test
  def testCreatesProperZkTlsConfigWhenDisabled(): Unit = {
    val props = new Properties
    props.put(ZkConfigs.ZK_CONNECT_CONFIG, zkConnect) // required, otherwise we would leave it out
    props.put(ZkConfigs.ZK_SSL_CLIENT_ENABLE_CONFIG, "false")
    val zkClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    ZkConfigs.ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.asScala.keys.foreach { propName =>
      assertNull(zkClientConfig.getProperty(propName))
    }
  }

  @Test
  def testCreatesProperZkTlsConfigWithTrueValues(): Unit = {
    val props = new Properties
    props.put(ZkConfigs.ZK_CONNECT_CONFIG, zkConnect) // required, otherwise we would leave it out
    // should get correct config for all properties if TLS is enabled
    val someValue = "some_value"
    def kafkaConfigValueToSet(kafkaProp: String) : String = kafkaProp match {
      case ZkConfigs.ZK_SSL_CLIENT_ENABLE_CONFIG | ZkConfigs.ZK_SSL_CRL_ENABLE_CONFIG | ZkConfigs.ZK_SSL_OCSP_ENABLE_CONFIG => "true"
      case ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG => "HTTPS"
      case _ => someValue
    }
    ZkConfigs.ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.asScala.keys.foreach(kafkaProp => props.put(kafkaProp, kafkaConfigValueToSet(kafkaProp)))
    val zkClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    // now check to make sure the values were set correctly
    def zkClientValueToExpect(kafkaProp: String) : String = kafkaProp match {
      case ZkConfigs.ZK_SSL_CLIENT_ENABLE_CONFIG | ZkConfigs.ZK_SSL_CRL_ENABLE_CONFIG | ZkConfigs.ZK_SSL_OCSP_ENABLE_CONFIG => "true"
      case ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG => "true"
      case _ => someValue
    }
    ZkConfigs.ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.asScala.keys.foreach(kafkaProp =>
      assertEquals(zkClientValueToExpect(kafkaProp), zkClientConfig.getProperty(ZkConfigs.ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(kafkaProp))))
  }

  @Test
  def testCreatesProperZkTlsConfigWithFalseAndListValues(): Unit = {
    val props = new Properties
    props.put(ZkConfigs.ZK_CONNECT_CONFIG, zkConnect) // required, otherwise we would leave it out
    // should get correct config for all properties if TLS is enabled
    val someValue = "some_value"
    def kafkaConfigValueToSet(kafkaProp: String) : String = kafkaProp match {
      case ZkConfigs.ZK_SSL_CLIENT_ENABLE_CONFIG => "true"
      case ZkConfigs.ZK_SSL_CRL_ENABLE_CONFIG | ZkConfigs.ZK_SSL_OCSP_ENABLE_CONFIG => "false"
      case ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG => ""
      case ZkConfigs.ZK_SSL_ENABLED_PROTOCOLS_CONFIG | ZkConfigs.ZK_SSL_CIPHER_SUITES_CONFIG => "A,B"
      case _ => someValue
    }
    ZkConfigs.ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.asScala.keys.foreach(kafkaProp => props.put(kafkaProp, kafkaConfigValueToSet(kafkaProp)))
    val zkClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(KafkaConfig.fromProps(props))
    // now check to make sure the values were set correctly
    def zkClientValueToExpect(kafkaProp: String) : String = kafkaProp match {
      case ZkConfigs.ZK_SSL_CLIENT_ENABLE_CONFIG => "true"
      case ZkConfigs.ZK_SSL_CRL_ENABLE_CONFIG | ZkConfigs.ZK_SSL_OCSP_ENABLE_CONFIG => "false"
      case ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG => "false"
      case ZkConfigs.ZK_SSL_ENABLED_PROTOCOLS_CONFIG | ZkConfigs.ZK_SSL_CIPHER_SUITES_CONFIG => "A,B"
      case _ => someValue
    }
    ZkConfigs.ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.asScala.keys.foreach(kafkaProp =>
      assertEquals(zkClientValueToExpect(kafkaProp), zkClientConfig.getProperty(ZkConfigs.ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(kafkaProp))))
  }

  @Test
  def testZkIsrManager(): Unit = {
    val props = TestUtils.createBrokerConfigs(1, zkConnect).head
    props.put(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, "2.7-IV1")

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
    props.put(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, MetadataVersion.latestTesting.toString)

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
    props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, s"PLAINTEXT://$hostName:$port")
    val kafkaConfig = KafkaConfig.fromProps(props)
    TestUtils.createServer(kafkaConfig)
  }

  def createServerWithListenerOnPort(port: Int): KafkaServer = {
    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(SocketServerConfigs.LISTENERS_CONFIG, s"PLAINTEXT://localhost:$port")
    val kafkaConfig = KafkaConfig.fromProps(props)
    TestUtils.createServer(kafkaConfig)
  }

}
