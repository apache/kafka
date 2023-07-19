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

import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.log.remote.storage.{NoOpRemoteLogMetadataManager, NoOpRemoteStorageManager, RemoteLogManagerConfig}
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}

class ServerStartupTest extends QuorumTestHarness {

  private var server: KafkaServer = _

  @AfterEach
  override def tearDown(): Unit = {
    if (server != null)
      TestUtils.shutdownServers(Seq(server))
    super.tearDown()
  }

  @Test
  def testBrokerCreatesZKChroot(): Unit = {
    val brokerId = 0
    val zookeeperChroot = "/kafka-chroot-for-unittest"
    val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
    val zooKeeperConnect = props.get("zookeeper.connect")
    props.put("zookeeper.connect", zooKeeperConnect.toString + zookeeperChroot)
    server = TestUtils.createServer(KafkaConfig.fromProps(props))

    val pathExists = zkClient.pathExists(zookeeperChroot)
    assertTrue(pathExists)
  }

  @Test
  def testConflictBrokerStartupWithSamePort(): Unit = {
    // Create and start first broker
    val brokerId1 = 0
    val props1 = TestUtils.createBrokerConfig(brokerId1, zkConnect)
    server = TestUtils.createServer(KafkaConfig.fromProps(props1))
    val port = TestUtils.boundPort(server)

    // Create a second broker with same port
    val brokerId2 = 1
    val props2 = TestUtils.createBrokerConfig(brokerId2, zkConnect, port = port)
    assertThrows(classOf[IllegalArgumentException], () => TestUtils.createServer(KafkaConfig.fromProps(props2)))
  }

  @Test
  def testRemoteStorageEnabled(): Unit = {
    // Create and start first broker
    val brokerId1 = 0
    val props1 = TestUtils.createBrokerConfig(brokerId1, zkConnect)
    props1.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    props1.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteStorageManager].getName)
    props1.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteLogMetadataManager].getName)
    props1.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, "badListenerName")
    assertThrows(classOf[ConfigException], () => TestUtils.createServer(KafkaConfig.fromProps(props1)))
    // should not throw exception after adding a correct value for "remote.log.metadata.manager.listener.name"
    props1.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, "PLAINTEXT")
    server = TestUtils.createServer(KafkaConfig.fromProps(props1))
  }

  @Test
  def testRemoteStorageEnabledWithoutSettingListener(): Unit = {
    // Create and start first broker
    val brokerId1 = 0
    val props1 = TestUtils.createBrokerConfig(brokerId1, zkConnect)
    props1.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    props1.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteStorageManager].getName)
    props1.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteLogMetadataManager].getName)
    // should not throw exception if "remote.log.metadata.manager.listener.name" is unconfigured
    server = TestUtils.createServer(KafkaConfig.fromProps(props1))
  }

  @Test
  def testConflictBrokerRegistration(): Unit = {
    // Try starting a broker with the a conflicting broker id.
    // This shouldn't affect the existing broker registration.

    val brokerId = 0
    val props1 = TestUtils.createBrokerConfig(brokerId, zkConnect)
    server = TestUtils.createServer(KafkaConfig.fromProps(props1))
    val brokerRegistration = zkClient.getBroker(brokerId).getOrElse(fail("broker doesn't exists"))

    val props2 = TestUtils.createBrokerConfig(brokerId, zkConnect)
    assertThrows(classOf[NodeExistsException], () => TestUtils.createServer(KafkaConfig.fromProps(props2)))

    // broker registration shouldn't change
    assertEquals(brokerRegistration, zkClient.getBroker(brokerId).getOrElse(fail("broker doesn't exists")))
  }

  @Test
  def testBrokerSelfAware(): Unit = {
    val brokerId = 0
    val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
    server = TestUtils.createServer(KafkaConfig.fromProps(props))

    TestUtils.waitUntilTrue(() => server.metadataCache.getAliveBrokers().nonEmpty, "Wait for cache to update")
    assertEquals(1, server.metadataCache.getAliveBrokers().size)
    assertEquals(brokerId, server.metadataCache.getAliveBrokers().head.id)
  }

  @Test
  def testBrokerStateRunningAfterZK(): Unit = {
    val brokerId = 0

    val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
    server = new KafkaServer(KafkaConfig.fromProps(props))

    server.startup()
    TestUtils.waitUntilTrue(() => server.brokerState == BrokerState.RUNNING,
      "waiting for the broker state to become RUNNING")
    val brokers = zkClient.getAllBrokersInCluster
    assertEquals(1, brokers.size)
    assertEquals(brokerId, brokers.head.id)
  }
}
