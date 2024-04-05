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

import kafka.server._
import kafka.utils.TestUtils
import kafka.utils.TestUtils.{createAdminClient, resource}
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{KafkaException, Uuid}
import org.apache.kafka.controller.ControllerRequestContextUtil.ANONYMOUS_CONTEXT
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}

import java.io.File
import java.util
import java.util.{Arrays, Collections, Properties}
import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

/**
 * A test harness that brings up some number of broker nodes
 */
abstract class KafkaServerTestHarness extends QuorumTestHarness {
  var instanceConfigs: Seq[KafkaConfig] = _

  private val _brokers = new mutable.ArrayBuffer[KafkaBroker]

  /**
   * Get the list of brokers, which could be either BrokerServer objects or KafkaServer objects.
   */
  def brokers: mutable.Buffer[KafkaBroker] = _brokers

  /**
   * Get the list of brokers, as instances of KafkaServer.
   * This method should only be used when dealing with brokers that use ZooKeeper.
   */
  def servers: mutable.Buffer[KafkaServer] = {
    checkIsZKTest()
    _brokers.asInstanceOf[mutable.Buffer[KafkaServer]]
  }

  var alive: Array[Boolean] = _

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
  def configureSecurityBeforeServersStart(testInfo: TestInfo): Unit = {}

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

  def bootstrapServers(listenerName: ListenerName = listenerName): String = {
    TestUtils.bootstrapServers(_brokers, listenerName)
  }

  protected def securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT
  protected def listenerName: ListenerName = ListenerName.forSecurityProtocol(securityProtocol)
  protected def trustStoreFile: Option[File] = None
  protected def serverSaslProperties: Option[Properties] = None
  protected def clientSaslProperties: Option[Properties] = None
  protected def brokerTime(brokerId: Int): Time = Time.SYSTEM

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    if (configs.isEmpty)
      throw new KafkaException("Must supply at least one server config.")

    // default implementation is a no-op, it is overridden by subclasses if required
    configureSecurityBeforeServersStart(testInfo)

    createBrokers(startup = true)


    // default implementation is a no-op, it is overridden by subclasses if required
    configureSecurityAfterServersStart()
  }

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(_brokers)
    super.tearDown()
  }

  def recreateBrokers(reconfigure: Boolean = false, startup: Boolean = false): Unit = {
    // The test must be allowed to fail and be torn down if an exception is raised here.
    if (reconfigure) {
      instanceConfigs = null
    }
    if (configs.isEmpty)
      throw new KafkaException("Must supply at least one server config.")

    TestUtils.shutdownServers(_brokers, deleteLogDirs = false)
    _brokers.clear()
    Arrays.fill(alive, false)

    createBrokers(startup)
  }

  def createOffsetsTopic(
    listenerName: ListenerName = listenerName,
    adminClientConfig: Properties = new Properties
  ): Unit = {
    if (isKRaftTest()) {
      resource(createAdminClient(brokers, listenerName, adminClientConfig)) { admin =>
        TestUtils.createOffsetsTopicWithAdmin(admin, brokers, controllerServers)
      }
    } else {
      TestUtils.createOffsetsTopic(zkClient, servers)
    }
  }

  /**
   * Create a topic.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(
    topic: String,
    numPartitions: Int = 1,
    replicationFactor: Int = 1,
    topicConfig: Properties = new Properties,
    listenerName: ListenerName = listenerName,
    adminClientConfig: Properties = new Properties
  ): scala.collection.immutable.Map[Int, Int] = {
    if (isKRaftTest()) {
      resource(createAdminClient(brokers, listenerName, adminClientConfig)) { admin =>
        TestUtils.createTopicWithAdmin(
          admin = admin,
          topic = topic,
          brokers = brokers,
          controllers = controllerServers,
          numPartitions = numPartitions,
          replicationFactor = replicationFactor,
          topicConfig = topicConfig
        )
      }
    } else {
      TestUtils.createTopic(
        zkClient = zkClient,
        topic = topic,
        numPartitions = numPartitions,
        replicationFactor = replicationFactor,
        servers = servers,
        topicConfig = topicConfig
      )
    }
  }

  /**
   * Create a topic in ZooKeeper using a customized replica assignment.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopicWithAssignment(
    topic: String,
    partitionReplicaAssignment: collection.Map[Int, Seq[Int]],
    listenerName: ListenerName = listenerName
  ): scala.collection.immutable.Map[Int, Int] =
    if (isKRaftTest()) {
      resource(createAdminClient(brokers, listenerName)) { admin =>
        TestUtils.createTopicWithAdmin(
          admin = admin,
          topic = topic,
          replicaAssignment = partitionReplicaAssignment,
          brokers = brokers,
          controllers = controllerServers
        )
      }
    } else {
      TestUtils.createTopic(
        zkClient,
        topic,
        partitionReplicaAssignment,
        servers
      )
    }

  def deleteTopic(
    topic: String,
    listenerName: ListenerName = listenerName
  ): Unit = {
    if (isKRaftTest()) {
      resource(createAdminClient(brokers, listenerName)) { admin =>
        TestUtils.deleteTopicWithAdmin(
          admin = admin,
          topic = topic,
          brokers = aliveBrokers,
          controllers = controllerServers)
      }
    } else {
      adminZkClient.deleteTopic(topic)
    }
  }

  def addAndVerifyAcls(acls: Set[AccessControlEntry], resource: ResourcePattern): Unit = {
    TestUtils.addAndVerifyAcls(brokers, acls, resource, controllerServers)
  }

  def removeAndVerifyAcls(acls: Set[AccessControlEntry], resource: ResourcePattern): Unit = {
    TestUtils.removeAndVerifyAcls(brokers, acls, resource, controllerServers)
  }

  /**
   * Pick a broker at random and kill it if it isn't already dead
   * Return the id of the broker killed
   */
  def killRandomBroker(): Int = {
    val index = TestUtils.random.nextInt(_brokers.length)
    killBroker(index)
    index
  }

  def killBroker(index: Int): Unit = {
    if (alive(index)) {
      _brokers(index).shutdown()
      _brokers(index).awaitShutdown()
      alive(index) = false
    }
  }

  def startBroker(index: Int): Unit = {
    if (!alive(index)) {
      _brokers(index).startup()
      alive(index) = true
    }
  }

  /**
   * Restart any dead brokers
   */
  def restartDeadBrokers(reconfigure: Boolean = false): Unit = {
    if (reconfigure) {
      instanceConfigs = null
    }
    if (configs.isEmpty)
      throw new KafkaException("Must supply at least one server config.")
    for (i <- _brokers.indices if !alive(i)) {
      if (reconfigure) {
        _brokers(i) = createBrokerFromConfig(configs(i))
      }
      _brokers(i).startup()
      alive(i) = true
    }
  }

  def waitForUserScramCredentialToAppearOnAllBrokers(clientPrincipal: String, mechanismName: String): Unit = {
    _brokers.foreach { server =>
      val cache = server.credentialProvider.credentialCache.cache(mechanismName, classOf[ScramCredential])
      TestUtils.waitUntilTrue(() => cache.get(clientPrincipal) != null, s"SCRAM credentials not created for $clientPrincipal")
    }
  }

  def getController(): KafkaServer = {
    checkIsZKTest()
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    servers.filter(s => s.config.brokerId == controllerId).head
  }

  def getTopicIds(names: Seq[String]): Map[String, Uuid] = {
    val result = new util.HashMap[String, Uuid]()
    if (isKRaftTest()) {
      val topicIdsMap = controllerServer.controller.findTopicIds(ANONYMOUS_CONTEXT, names.asJava).get()
      names.foreach { name =>
        val response = topicIdsMap.get(name)
        result.put(name, response.result())
      }
    } else {
      val topicIdsMap = getController().kafkaController.controllerContext.topicIds.toMap
      names.foreach { name =>
        if (topicIdsMap.contains(name)) result.put(name, topicIdsMap.get(name).get)
      }
    }
    result.asScala.toMap
  }

  def getTopicIds(): Map[String, Uuid] = {
    if (isKRaftTest()) {
      controllerServer.controller.findAllTopicIds(ANONYMOUS_CONTEXT).get().asScala.toMap
    } else {
      getController().kafkaController.controllerContext.topicIds.toMap
    }
  }

  def getTopicNames(): Map[Uuid, String] = {
    if (isKRaftTest()) {
      val result = new util.HashMap[Uuid, String]()
      controllerServer.controller.findAllTopicIds(ANONYMOUS_CONTEXT).get().entrySet().forEach {
        e => result.put(e.getValue(), e.getKey())
      }
      result.asScala.toMap
    } else {
      getController().kafkaController.controllerContext.topicNames.toMap
    }
  }

  private def createBrokers(startup: Boolean): Unit = {
    // Add each broker to `brokers` buffer as soon as it is created to ensure that brokers
    // are shutdown cleanly in tearDown even if a subsequent broker fails to start
    val potentiallyRegeneratedConfigs = configs
    alive = new Array[Boolean](potentiallyRegeneratedConfigs.length)
    Arrays.fill(alive, false)
    for (config <- potentiallyRegeneratedConfigs) {
      val broker = createBrokerFromConfig(config)
      _brokers += broker
      if (startup) {
        broker.startup()
        alive(_brokers.length - 1) = true
      }
    }
  }

  private def createBrokerFromConfig(config: KafkaConfig): KafkaBroker = {
    if (isKRaftTest()) {
      createBroker(config, brokerTime(config.brokerId), startup = false)
    } else {
      TestUtils.createServer(
        config,
        time = brokerTime(config.brokerId),
        threadNamePrefix = None,
        startup = false,
        enableZkApiForwarding = isZkMigrationTest() || (config.migrationEnabled && config.interBrokerProtocolVersion.isApiForwardingEnabled)
      )
    }
  }

  def aliveBrokers: Seq[KafkaBroker] = {
    _brokers.filter(broker => alive(broker.config.brokerId)).toSeq
  }

  def ensureConsistentKRaftMetadata(): Unit = {
    if (isKRaftTest()) {
      TestUtils.ensureConsistentKRaftMetadata(
        aliveBrokers,
        controllerServer
      )
    }
  }

  def changeClientIdConfig(sanitizedClientId: String, configs: Properties): Unit = {
    if (isKRaftTest()) {
      resource(createAdminClient(brokers, listenerName)) {
        admin => {
          admin.alterClientQuotas(Collections.singleton(
            new ClientQuotaAlteration(
              new ClientQuotaEntity(Map(ClientQuotaEntity.CLIENT_ID -> (if (sanitizedClientId == "<default>") null else sanitizedClientId)).asJava),
              configs.asScala.map { case (key, value) => new ClientQuotaAlteration.Op(key, value.toDouble) }.toList.asJava))).all().get()
        }
      }
    }
    else {
      adminZkClient.changeClientIdConfig(sanitizedClientId, configs)
    }
  }
}
