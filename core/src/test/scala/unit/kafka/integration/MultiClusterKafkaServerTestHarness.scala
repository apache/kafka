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

package kafka.integration

import java.io.File
import java.util.Properties

import kafka.server._
import kafka.utils.TestUtils
import kafka.zk.MultiClusterZooKeeperTestHarness
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import scala.collection.Seq
import scala.collection.mutable.{ArrayBuffer, Buffer}

/**
 * A test harness that brings up some number of broker nodes
 */
abstract class MultiClusterKafkaServerTestHarness extends MultiClusterZooKeeperTestHarness {
  private val brokerLists: Buffer[String] = new ArrayBuffer(numClusters)
  private val instanceServers: Buffer[Buffer[KafkaServer]] = new ArrayBuffer(numClusters)
  private val instanceConfigs: Buffer[Seq[KafkaConfig]] = new ArrayBuffer(numClusters)

  val kafkaPrincipalType = KafkaPrincipal.USER_TYPE

  def brokerList(clusterIndex: Int): String = {
    brokerLists(clusterIndex)
  }

  // We avoid naming this method servers() since that conflicts with the existing indexed addressing of
  // Buffer[KafkaServer] (i.e., of specific brokers within a single cluster) used in our sub-sub-subclass
  // AbstractConsumerTest.
  def serversByCluster(clusterIndex: Int): Buffer[KafkaServer] = {
    instanceServers(clusterIndex)
  }

  private def configsByCluster(clusterIndex: Int): Seq[KafkaConfig] = {
    if (clusterIndex >= numClusters)
      throw new KafkaException("clusterIndex (" + clusterIndex + ") is out of range: must be less than " + numClusters)
    if (instanceConfigs.length < numClusters)
      (instanceConfigs.length until numClusters).foreach { _ => instanceConfigs += null }
    if (instanceConfigs(clusterIndex) == null)
      instanceConfigs(clusterIndex) = generateConfigsByCluster(clusterIndex)
    instanceConfigs(clusterIndex)
  }

  /**
   * Implementations must override this method to return a set of KafkaConfigs matching the number of brokers per
   * cluster. This method will be invoked for every test and should not reuse previous configurations unless they
   * select their ports randomly when servers are started.
   */
  def generateConfigsByCluster(clusterIndex: Int): Seq[KafkaConfig]

  /**
   * Override this in case ACLs or security credentials must be set before `servers` are started.
   *
   * This is required in some cases because of the topic creation in the setup of `IntegrationTestHarness`. If
   * the ACLs are only set later, tests may fail. The failure could manifest itself as a cluster action
   * authorization exception when processing an update metadata request (controller -> broker) or in more obscure
   * ways (e.g. `__consumer_offsets` topic replication fails because the metadata cache has no brokers as a previous
   * update metadata request failed due to an authorization exception).
   *
   * The default implementation of this method is a no-op.
   */
  def configureSecurityBeforeServersStart(clusterIndex: Int): Unit = {}

  /**
   * Override this in case Tokens or security credentials needs to be created after `servers` are started.
   * The default implementation of this method is a no-op.
   */
  def configureSecurityAfterServersStart(clusterIndex: Int): Unit = {}

  def boundPort(server: KafkaServer): Int = server.boundPort(listenerName)

  protected def securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT
  protected def listenerName: ListenerName = ListenerName.forSecurityProtocol(securityProtocol)
  protected def trustStoreFile: Option[File] = None
  protected def serverSaslProperties: Option[Properties] = None
  protected def clientSaslProperties: Option[Properties] = None
  protected def brokerTime(brokerId: Int): Time = Time.SYSTEM

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()

    (0 until numClusters).foreach { clusterIndex =>
      // apparently intended semantic is that each config in the Seq[KafkaConfig] corresponds to a single broker?
      if (configsByCluster(clusterIndex).isEmpty)
        throw new KafkaException(s"Must supply at least one server config for cluster #${clusterIndex}")

      instanceServers += new ArrayBuffer(configsByCluster(clusterIndex).length)

      // default implementation is a no-op, it is overridden by subclasses if required
      configureSecurityBeforeServersStart(clusterIndex)

      // Add each broker to `instanceServers` buffer as soon as it is created to ensure that brokers
      // are shut down cleanly in tearDown even if a subsequent broker fails to start
      for (config <- configsByCluster(clusterIndex))
        instanceServers(clusterIndex) += TestUtils.createServer(config, time = brokerTime(config.brokerId))
      brokerLists += TestUtils.bootstrapServers(instanceServers(clusterIndex), listenerName)

      // default implementation is a no-op, it is overridden by subclasses if required
      configureSecurityAfterServersStart(clusterIndex)
    }
  }

  @AfterEach
  override def tearDown(): Unit = {
    (0 until numClusters).foreach { clusterIndex =>
      if (serversByCluster(clusterIndex) != null) {
        TestUtils.shutdownServers(serversByCluster(clusterIndex))
      }
    }
    super.tearDown()
  }

  /**
   * Create a topic in ZooKeeper.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(topic: String, numPartitions: Int = 1, replicationFactor: Int = 1,
                  topicConfig: Properties = new Properties, clusterIndex: Int = 0):
                  scala.collection.immutable.Map[Int, Int] =
    TestUtils.createTopic(zkClient(clusterIndex), topic, numPartitions, replicationFactor,
      serversByCluster(clusterIndex), topicConfig)

  /**
   * Create a topic in ZooKeeper using a customized replica assignment.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(topic: String, partitionReplicaAssignment: collection.Map[Int, Seq[Int]], clusterIndex: Int):
                  scala.collection.immutable.Map[Int, Int] =
    TestUtils.createTopic(zkClient(clusterIndex), topic, partitionReplicaAssignment, serversByCluster(clusterIndex))

}
