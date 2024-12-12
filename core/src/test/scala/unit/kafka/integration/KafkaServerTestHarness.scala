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
import kafka.utils.TestUtils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter}
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.internals.Topic
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
import java.time.Duration
import java.util
import java.util.{Collections, Properties}
import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._
import scala.util.Using

/**
 * A test harness that brings up some number of broker nodes
 */
abstract class KafkaServerTestHarness extends QuorumTestHarness {
  var instanceConfigs: Seq[KafkaConfig] = _

  private val _brokers = new mutable.ArrayBuffer[KafkaBroker]

  /**
   * Get the list of brokers.
   */
  def brokers: mutable.Buffer[KafkaBroker] = _brokers

  /**
   * Get the list of brokers.
   */
  def servers: mutable.Buffer[KafkaBroker] = brokers

  def brokerServers: mutable.Buffer[BrokerServer] = {
    checkIsKRaftTest()
    _brokers.asInstanceOf[mutable.Buffer[BrokerServer]]
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

  def serverForId(id: Int): Option[KafkaBroker] = brokers.find(s => s.config.brokerId == id)

  def boundPort(server: KafkaBroker): Int = server.boundPort(listenerName)

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
    util.Arrays.fill(alive, false)

    createBrokers(startup)
  }

  def createOffsetsTopic(
    listenerName: ListenerName = listenerName,
    adminClientConfig: Properties = new Properties
  ): Unit = {
    if (isKRaftTest()) {
      Using.resource(createAdminClient(brokers, listenerName, adminClientConfig)) { admin =>
        TestUtils.createOffsetsTopicWithAdmin(admin, brokers, controllerServers)
      }
    } else {
      createOffsetsTopic(zkClient, servers)
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
      Using.resource(createAdminClient(brokers, listenerName, adminClientConfig)) { admin =>
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
      Using.resource(createAdminClient(brokers, listenerName)) { admin =>
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
      Using.resource(createAdminClient(brokers, listenerName)) { admin =>
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
    val authorizerForWrite = pickAuthorizerForWrite(brokers, controllerServers)
    val aclBindings = acls.map { acl => new AclBinding(resource, acl) }
    authorizerForWrite.createAcls(anonymousAuthorizableContext, aclBindings.toList.asJava).asScala
      .map(_.toCompletableFuture.get)
      .foreach { result =>
        result.exception.ifPresent { e => throw e }
      }
    val aclFilter = new AclBindingFilter(resource.toFilter, AccessControlEntryFilter.ANY)
    (brokers.map(_.authorizer.get) ++ controllerServers.map(_.authorizer.get)).foreach {
      authorizer => waitAndVerifyAcls(
        authorizer.acls(aclFilter).asScala.map(_.entry).toSet ++ acls,
        authorizer, resource)
    }
  }

  def removeAndVerifyAcls(acls: Set[AccessControlEntry], resource: ResourcePattern): Unit = {
    val authorizerForWrite = pickAuthorizerForWrite(brokers, controllerServers)
    val aclBindingFilters = acls.map { acl => new AclBindingFilter(resource.toFilter, acl.toFilter) }
    authorizerForWrite.deleteAcls(anonymousAuthorizableContext, aclBindingFilters.toList.asJava).asScala
      .map(_.toCompletableFuture.get)
      .foreach { result =>
        result.exception.ifPresent { e => throw e }
      }
    val aclFilter = new AclBindingFilter(resource.toFilter, AccessControlEntryFilter.ANY)
    (brokers.map(_.authorizer.get) ++ controllerServers.map(_.authorizer.get)).foreach {
      authorizer => waitAndVerifyAcls(
        authorizer.acls(aclFilter).asScala.map(_.entry).toSet -- acls,
        authorizer, resource)
    }
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

  /**
   * Kill the broker at the specified index.
   * A controlled shutdown is attempted, with a timeout of 5 minutes.
   */
  def killBroker(index: Int): Unit = {
    killBroker(index, Duration.ofMinutes(5))
  }

  /**
   * Kill the broker at the specified index.
   * A controlled shutdown is attempted, with the specified timeout.
   */
  def killBroker(index: Int, timeout: Duration): Unit = {
    if(alive(index)) {
      _brokers(index).shutdown(timeout)
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

  def getTopicIds(names: Seq[String]): Map[String, Uuid] = {
    val result = new util.HashMap[String, Uuid]()
    val topicIdsMap = controllerServer.controller.findTopicIds(ANONYMOUS_CONTEXT, names.asJava).get()
    names.foreach { name =>
      val response = topicIdsMap.get(name)
      result.put(name, response.result())
    }
    result.asScala.toMap
  }

  def getTopicIds(): Map[String, Uuid] = {
    controllerServer.controller.findAllTopicIds(ANONYMOUS_CONTEXT).get().asScala.toMap
  }

  def getTopicNames(): Map[Uuid, String] = {
    val result = new util.HashMap[Uuid, String]()
    controllerServer.controller.findAllTopicIds(ANONYMOUS_CONTEXT).get().forEach {
      (key, value) => result.put(value, key)
    }
    result.asScala.toMap
  }

  private def createBrokers(startup: Boolean): Unit = {
    // Add each broker to `brokers` buffer as soon as it is created to ensure that brokers
    // are shutdown cleanly in tearDown even if a subsequent broker fails to start
    val potentiallyRegeneratedConfigs = configs
    alive = new Array[Boolean](potentiallyRegeneratedConfigs.length)
    util.Arrays.fill(alive, false)
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
      TestUtils.createServer(config, time = brokerTime(config.brokerId), threadNamePrefix = None, startup = false)
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
      Using.resource(createAdminClient(brokers, listenerName)) {
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

  /**
   * Ensures that the consumer offsets/group metadata topic exists. If it does not, the topic is created and the method waits
   * until the leader is elected and metadata is propagated to all brokers. If it does, the method verifies that it has
   * the expected number of partition and replication factor however it does not guarantee that the topic is empty.
   */
  private def createOffsetsTopic(zkClient: KafkaZkClient, servers: Seq[KafkaBroker]): Unit = {
    val server = servers.head
    val numPartitions = server.config.groupCoordinatorConfig.offsetsTopicPartitions
    val replicationFactor = server.config.groupCoordinatorConfig.offsetsTopicReplicationFactor.toInt

    try {
       TestUtils.createTopic(
        zkClient,
        Topic.GROUP_METADATA_TOPIC_NAME,
        numPartitions,
        replicationFactor,
        servers,
        server.groupCoordinator.groupMetadataTopicConfigs
      )
    } catch {
      case ex: TopicExistsException =>
        val allPartitionsMetadata = waitForAllPartitionsMetadata(
          servers,
          Topic.GROUP_METADATA_TOPIC_NAME,
          numPartitions
        )

        // If the topic already exists, we ensure that it has the required
        // number of partitions and replication factor. If it has not, the
        // exception is thrown further.
        if (allPartitionsMetadata.size != numPartitions || allPartitionsMetadata.head._2.replicas.size != replicationFactor) {
          throw ex
        }
    }
  }
}
