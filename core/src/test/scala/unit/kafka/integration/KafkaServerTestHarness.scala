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
import java.util.Arrays

import kafka.server._
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import scala.collection.Seq
import scala.collection.mutable.{ArrayBuffer, Buffer}
import java.util.Properties

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.utils.Time

/**
 * A test harness that brings up some number of broker nodes
 */
abstract class KafkaServerTestHarness extends ZooKeeperTestHarness {
  var instanceConfigs: Seq[KafkaConfig] = null
  var servers: Buffer[KafkaServer] = new ArrayBuffer
  var brokerList: String = null
  var alive: Array[Boolean] = null

  /**
   * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
   * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
   */
  def generateConfigs: Seq[KafkaConfig]

  /**
   * Override this in case ACLs or security credentials must be set before `servers` are started.
   *
   * This is required in some cases because of the topic creation in the setup of `IntegrationTestHarness`. If the ACLs
   * are only set later, tests may fail. The failure could manifest itself as a cluster action
   * authorization exception when processing an update metadata request (controller -> broker) or in more obscure
   * ways (e.g. __consumer_offsets topic replication fails because the metadata cache has no brokers as a previous
   * update metadata request failed due to an authorization exception).
   *
   * The default implementation of this method is a no-op.
   */
  def configureSecurityBeforeServersStart(): Unit = {}

  /**
   * Override this in case Tokens or security credentials needs to be created after `servers` are started.
   * The default implementation of this method is a no-op.
   */
  def configureSecurityAfterServersStart(): Unit = {}

  def configs: Seq[KafkaConfig] = {
    if (instanceConfigs == null)
      instanceConfigs = generateConfigs
    instanceConfigs
  }

  def serverForId(id: Int): Option[KafkaServer] = servers.find(s => s.config.brokerId == id)

  def boundPort(server: KafkaServer): Int = server.boundPort(listenerName)

  protected def securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT
  protected def listenerName: ListenerName = ListenerName.forSecurityProtocol(securityProtocol)
  protected def trustStoreFile: Option[File] = None
  protected def serverSaslProperties: Option[Properties] = None
  protected def clientSaslProperties: Option[Properties] = None
  protected def brokerTime(brokerId: Int): Time = Time.SYSTEM
  protected def enableForwarding: Boolean = false

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()

    if (configs.isEmpty)
      throw new KafkaException("Must supply at least one server config.")

    // default implementation is a no-op, it is overridden by subclasses if required
    configureSecurityBeforeServersStart()

    // Add each broker to `servers` buffer as soon as it is created to ensure that brokers
    // are shutdown cleanly in tearDown even if a subsequent broker fails to start
    for (config <- configs) {
      servers += TestUtils.createServer(
        config,
        time = brokerTime(config.brokerId),
        threadNamePrefix = None,
        enableForwarding
      )
    }
    brokerList = TestUtils.bootstrapServers(servers, listenerName)
    alive = new Array[Boolean](servers.length)
    Arrays.fill(alive, true)

    // default implementation is a no-op, it is overridden by subclasses if required
    configureSecurityAfterServersStart()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (servers != null) {
      TestUtils.shutdownServers(servers)
    }
    super.tearDown()
  }

  /**
   * Create a topic in ZooKeeper.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(topic: String, numPartitions: Int = 1, replicationFactor: Int = 1,
                  topicConfig: Properties = new Properties): scala.collection.immutable.Map[Int, Int] =
    TestUtils.createTopic(zkClient, topic, numPartitions, replicationFactor, servers, topicConfig)

  /**
   * Create a topic in ZooKeeper using a customized replica assignment.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(topic: String, partitionReplicaAssignment: collection.Map[Int, Seq[Int]]): scala.collection.immutable.Map[Int, Int] =
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment, servers)

  /**
   * Pick a broker at random and kill it if it isn't already dead
   * Return the id of the broker killed
   */
  def killRandomBroker(): Int = {
    val index = TestUtils.random.nextInt(servers.length)
    killBroker(index)
    index
  }

  def killBroker(index: Int): Unit = {
    if(alive(index)) {
      servers(index).shutdown()
      servers(index).awaitShutdown()
      alive(index) = false
    }
  }

  /**
   * Restart any dead brokers
   */
  def restartDeadBrokers(): Unit = {
    for(i <- servers.indices if !alive(i)) {
      servers(i).startup()
      alive(i) = true
    }
  }

  def waitForUserScramCredentialToAppearOnAllBrokers(clientPrincipal: String, mechanismName: String): Unit = {
    servers.foreach { server =>
      val cache = server.credentialProvider.credentialCache.cache(mechanismName, classOf[ScramCredential])
      TestUtils.waitUntilTrue(() => cache.get(clientPrincipal) != null, s"SCRAM credentials not created for $clientPrincipal")
    }
  }

}
