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
package kafka.api

import java.io.File
import java.net.InetAddress
import java.lang.{Long => JLong}
import java.time.{Duration => JDuration}
import java.util.Arrays.asList
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, ExecutionException, TimeUnit}
import java.util.{Collections, Optional, Properties}
import java.{time, util}
import kafka.integration.KafkaServerTestHarness
import kafka.security.authorizer.AclEntry
import kafka.server.metadata.KRaftMetadataCache
import kafka.server.{DynamicConfig, KafkaConfig}
import kafka.utils.TestUtils._
import kafka.utils.{Log4jController, TestInfoUtils, TestUtils}
import org.apache.kafka.clients.HostResolver
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.config.{ConfigResource, LogLevelConfig, TopicConfig}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.requests.{DeleteRecordsRequest, MetadataResponse}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{ConsumerGroupState, ElectionType, TopicCollection, TopicPartition, TopicPartitionInfo, TopicPartitionReplica, Uuid}
import org.apache.kafka.controller.ControllerRequestContextUtil.ANONYMOUS_CONTEXT
import org.apache.kafka.server.config.Defaults
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Disabled, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.slf4j.LoggerFactory

import java.util.AbstractMap.SimpleImmutableEntry
import scala.annotation.nowarn
import scala.collection.Seq
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.util.Random

/**
 * An integration test of the KafkaAdminClient.
 *
 * Also see [[org.apache.kafka.clients.admin.KafkaAdminClientTest]] for unit tests of the admin client.
 */
class PlaintextAdminIntegrationTest extends BaseAdminIntegrationTest {
  import PlaintextAdminIntegrationTest._

  val topic = "topic"
  val partition = 0
  val topicPartition = new TopicPartition(topic, partition)

  private var brokerLoggerConfigResource: ConfigResource = _
  private val changedBrokerLoggers = scala.collection.mutable.Set[String]()

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    brokerLoggerConfigResource = new ConfigResource(
      ConfigResource.Type.BROKER_LOGGER, brokers.head.config.brokerId.toString)
  }

  @AfterEach
  override def tearDown(): Unit = {
    teardownBrokerLoggers()
    super.tearDown()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testClose(quorum: String): Unit = {
    val client = Admin.create(createConfig)
    client.close()
    client.close() // double close has no effect
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testListNodes(quorum: String): Unit = {
    client = Admin.create(createConfig)
    val brokerStrs = bootstrapServers().split(",").toList.sorted
    var nodeStrs: List[String] = null
    do {
      val nodes = client.describeCluster().nodes().get().asScala
      nodeStrs = nodes.map(node => s"${node.host}:${node.port}").toList.sorted
    } while (nodeStrs.size < brokerStrs.size)
    assertEquals(brokerStrs.mkString(","), nodeStrs.mkString(","))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testAdminClientHandlingBadIPWithoutTimeout(quorum: String): Unit = {
    val config = createConfig
    config.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "1000")
    val returnBadAddressFirst = new HostResolver {
      override def resolve(host: String): Array[InetAddress] = {
        Array[InetAddress](InetAddress.getByName("10.200.20.100"), InetAddress.getByName(host))
      }
    }
    client = AdminClientTestUtils.create(config, returnBadAddressFirst)
    // simply check that a call, e.g. describeCluster, returns normally
    client.describeCluster().nodes().get()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCreateExistingTopicsThrowTopicExistsException(quorum: String): Unit = {
    client = Admin.create(createConfig)
    val topic = "mytopic"
    val topics = Seq(topic)
    val newTopics = Seq(new NewTopic(topic, 1, 1.toShort))

    client.createTopics(newTopics.asJava).all.get()
    waitForTopics(client, topics, List())

    val newTopicsWithInvalidRF = Seq(new NewTopic(topic, 1, (brokers.size + 1).toShort))
    val e = assertThrows(classOf[ExecutionException],
      () => client.createTopics(newTopicsWithInvalidRF.asJava, new CreateTopicsOptions().validateOnly(true)).all.get())
    assertTrue(e.getCause.isInstanceOf[TopicExistsException])
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDeleteTopicsWithIds(quorum: String): Unit = {
    client = Admin.create(createConfig)
    val topics = Seq("mytopic", "mytopic2", "mytopic3")
    val newTopics = Seq(
      new NewTopic("mytopic", Map((0: Integer) -> Seq[Integer](1, 2).asJava, (1: Integer) -> Seq[Integer](2, 0).asJava).asJava),
      new NewTopic("mytopic2", 3, 3.toShort),
      new NewTopic("mytopic3", Option.empty[Integer].asJava, Option.empty[java.lang.Short].asJava)
    )
    val createResult = client.createTopics(newTopics.asJava)
    createResult.all.get()
    waitForTopics(client, topics, List())
    val topicIds = getTopicIds().values.toSet

    client.deleteTopics(TopicCollection.ofTopicIds(topicIds.asJava)).all.get()
    waitForTopics(client, List(), topics)
  }

  /**
    * describe should not auto create topics
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDescribeNonExistingTopic(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val existingTopic = "existing-topic"
    client.createTopics(Seq(existingTopic).map(new NewTopic(_, 1, 1.toShort)).asJava).all.get()
    waitForTopics(client, Seq(existingTopic), List())

    val nonExistingTopic = "non-existing"
    val results = client.describeTopics(Seq(nonExistingTopic, existingTopic).asJava).topicNameValues()
    assertEquals(existingTopic, results.get(existingTopic).get.name)
    assertFutureExceptionTypeEquals(results.get(nonExistingTopic), classOf[UnknownTopicOrPartitionException])
    if (!isKRaftTest()) {
      assertEquals(None, zkClient.getTopicPartitionCount(nonExistingTopic))
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDescribeTopicsWithIds(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val existingTopic = "existing-topic"
    client.createTopics(Seq(existingTopic).map(new NewTopic(_, 1, 1.toShort)).asJava).all.get()
    waitForTopics(client, Seq(existingTopic), List())
    ensureConsistentKRaftMetadata()

    val existingTopicId = brokers.head.metadataCache.getTopicId(existingTopic)

    val nonExistingTopicId = Uuid.randomUuid()

    val results = client.describeTopics(TopicCollection.ofTopicIds(Seq(existingTopicId, nonExistingTopicId).asJava)).topicIdValues()
    assertEquals(existingTopicId, results.get(existingTopicId).get.topicId())
    assertFutureExceptionTypeEquals(results.get(nonExistingTopicId), classOf[UnknownTopicIdException])
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDescribeCluster(quorum: String): Unit = {
    client = Admin.create(createConfig)
    val result = client.describeCluster
    val nodes = result.nodes.get()
    val clusterId = result.clusterId().get()
    assertEquals(brokers.head.dataPlaneRequestProcessor.clusterId, clusterId)
    val controller = result.controller().get()

    if (isKRaftTest()) {
      // In KRaft, we return a random brokerId as the current controller.
      val brokerIds = brokers.map(_.config.brokerId).toSet
      assertTrue(brokerIds.contains(controller.id))
    } else {
      assertEquals(brokers.head.dataPlaneRequestProcessor.metadataCache.getControllerId.map(_.id).
        getOrElse(MetadataResponse.NO_CONTROLLER_ID), controller.id)
    }

    val brokerEndpoints = bootstrapServers().split(",")
    assertEquals(brokerEndpoints.size, nodes.size)
    for (node <- nodes.asScala) {
      val hostStr = s"${node.host}:${node.port}"
      assertTrue(brokerEndpoints.contains(hostStr), s"Unknown host:port pair $hostStr in brokerVersionInfos")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDescribeLogDirs(quorum: String): Unit = {
    client = Admin.create(createConfig)
    val topic = "topic"
    val leaderByPartition = createTopic(topic, numPartitions = 10)
    val partitionsByBroker = leaderByPartition.groupBy { case (_, leaderId) => leaderId }.map { case (k, v) =>
      k -> v.keys.toSeq
    }
    ensureConsistentKRaftMetadata()
    val brokerIds = (0 until brokerCount).map(Integer.valueOf)
    val logDirInfosByBroker = client.describeLogDirs(brokerIds.asJava).allDescriptions.get

    (0 until brokerCount).foreach { brokerId =>
      val server = brokers.find(_.config.brokerId == brokerId).get
      val expectedPartitions = partitionsByBroker(brokerId)
      val logDirInfos = logDirInfosByBroker.get(brokerId)
      val replicaInfos = logDirInfos.asScala.flatMap { case (_, logDirInfo) =>
        logDirInfo.replicaInfos.asScala
      }.filter { case (k, _) => k.topic == topic }

      assertEquals(expectedPartitions.toSet, replicaInfos.keys.map(_.partition).toSet)
      logDirInfos.forEach { (logDir, logDirInfo) =>
        assertTrue(logDirInfo.totalBytes.isPresent)
        assertTrue(logDirInfo.usableBytes.isPresent)
        logDirInfo.replicaInfos.asScala.keys.foreach(tp =>
          assertEquals(server.logManager.getLog(tp).get.dir.getParent, logDir)
        )
      }
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDescribeReplicaLogDirs(quorum: String): Unit = {
    client = Admin.create(createConfig)
    val topic = "topic"
    val leaderByPartition = createTopic(topic, numPartitions = 10)
    val replicas = leaderByPartition.map { case (partition, brokerId) =>
      new TopicPartitionReplica(topic, partition, brokerId)
    }.toSeq
    ensureConsistentKRaftMetadata()

    val replicaDirInfos = client.describeReplicaLogDirs(replicas.asJavaCollection).all.get
    replicaDirInfos.forEach { (topicPartitionReplica, replicaDirInfo) =>
      val server = brokers.find(_.config.brokerId == topicPartitionReplica.brokerId()).get
      val tp = new TopicPartition(topicPartitionReplica.topic(), topicPartitionReplica.partition())
      assertEquals(server.logManager.getLog(tp).get.dir.getParent, replicaDirInfo.getCurrentReplicaLogDir)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testAlterReplicaLogDirs(quorum: String): Unit = {
    client = Admin.create(createConfig)
    val topic = "topic"
    val tp = new TopicPartition(topic, 0)
    val randomNums = brokers.map(server => server -> Random.nextInt(2)).toMap

    // Generate two mutually exclusive replicaAssignment
    val firstReplicaAssignment = brokers.map { server =>
      val logDir = new File(server.config.logDirs(randomNums(server))).getAbsolutePath
      new TopicPartitionReplica(topic, 0, server.config.brokerId) -> logDir
    }.toMap
    val secondReplicaAssignment = brokers.map { server =>
      val logDir = new File(server.config.logDirs(1 - randomNums(server))).getAbsolutePath
      new TopicPartitionReplica(topic, 0, server.config.brokerId) -> logDir
    }.toMap

    // Verify that replica can be created in the specified log directory
    val futures = client.alterReplicaLogDirs(firstReplicaAssignment.asJava,
      new AlterReplicaLogDirsOptions).values.asScala.values
    futures.foreach { future =>
      val exception = assertThrows(classOf[ExecutionException], () => future.get)
      assertTrue(exception.getCause.isInstanceOf[UnknownTopicOrPartitionException])
    }

    createTopic(topic, replicationFactor = brokerCount)
    ensureConsistentKRaftMetadata()
    brokers.foreach { server =>
      val logDir = server.logManager.getLog(tp).get.dir.getParent
      assertEquals(firstReplicaAssignment(new TopicPartitionReplica(topic, 0, server.config.brokerId)), logDir)
    }

    // Verify that replica can be moved to the specified log directory after the topic has been created
    client.alterReplicaLogDirs(secondReplicaAssignment.asJava, new AlterReplicaLogDirsOptions).all.get
    brokers.foreach { server =>
      TestUtils.waitUntilTrue(() => {
        val logDir = server.logManager.getLog(tp).get.dir.getParent
        secondReplicaAssignment(new TopicPartitionReplica(topic, 0, server.config.brokerId)) == logDir
      }, "timed out waiting for replica movement")
    }

    // Verify that replica can be moved to the specified log directory while the producer is sending messages
    val running = new AtomicBoolean(true)
    val numMessages = new AtomicInteger
    import scala.concurrent.ExecutionContext.Implicits._
    val producerFuture = Future {
      val producer = TestUtils.createProducer(
        bootstrapServers(),
        securityProtocol = securityProtocol,
        trustStoreFile = trustStoreFile,
        retries = 0, // Producer should not have to retry when broker is moving replica between log directories.
        requestTimeoutMs = 10000,
        acks = -1
      )
      try {
        while (running.get) {
          val future = producer.send(new ProducerRecord(topic, s"xxxxxxxxxxxxxxxxxxxx-$numMessages".getBytes))
          numMessages.incrementAndGet()
          future.get(10, TimeUnit.SECONDS)
        }
        numMessages.get
      } finally producer.close()
    }

    try {
      TestUtils.waitUntilTrue(() => numMessages.get > 10, s"only $numMessages messages are produced before timeout. Producer future ${producerFuture.value}")
      client.alterReplicaLogDirs(firstReplicaAssignment.asJava, new AlterReplicaLogDirsOptions).all.get
      brokers.foreach { server =>
        TestUtils.waitUntilTrue(() => {
          val logDir = server.logManager.getLog(tp).get.dir.getParent
          firstReplicaAssignment(new TopicPartitionReplica(topic, 0, server.config.brokerId)) == logDir
        }, s"timed out waiting for replica movement. Producer future ${producerFuture.value}")
      }

      val currentMessagesNum = numMessages.get
      TestUtils.waitUntilTrue(() => numMessages.get - currentMessagesNum > 10,
        s"only ${numMessages.get - currentMessagesNum} messages are produced within timeout after replica movement. Producer future ${producerFuture.value}")
    } finally running.set(false)

    val finalNumMessages = Await.result(producerFuture, Duration(20, TimeUnit.SECONDS))

    // Verify that all messages that are produced can be consumed
    val consumerRecords = TestUtils.consumeTopicRecords(brokers, topic, finalNumMessages,
      securityProtocol = securityProtocol, trustStoreFile = trustStoreFile)
    consumerRecords.zipWithIndex.foreach { case (consumerRecord, index) =>
      assertEquals(s"xxxxxxxxxxxxxxxxxxxx-$index", new String(consumerRecord.value))
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDescribeAndAlterConfigs(quorum: String): Unit = {
    client = Admin.create(createConfig)

    // Create topics
    val topic1 = "describe-alter-configs-topic-1"
    val topicResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    val topicConfig1 = new Properties
    topicConfig1.setProperty(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "500000")
    topicConfig1.setProperty(TopicConfig.RETENTION_MS_CONFIG, "60000000")
    createTopic(topic1, numPartitions = 1, replicationFactor = 1, topicConfig1)

    val topic2 = "describe-alter-configs-topic-2"
    val topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopic(topic2)

    // Describe topics and broker
    val brokerResource1 = new ConfigResource(ConfigResource.Type.BROKER, brokers(1).config.brokerId.toString)
    val brokerResource2 = new ConfigResource(ConfigResource.Type.BROKER, brokers(2).config.brokerId.toString)
    val configResources = Seq(topicResource1, topicResource2, brokerResource1, brokerResource2)
    val describeResult = client.describeConfigs(configResources.asJava)
    val configs = describeResult.all.get

    assertEquals(4, configs.size)

    val maxMessageBytes1 = configs.get(topicResource1).get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG)
    assertEquals(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, maxMessageBytes1.name)
    assertEquals(topicConfig1.get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG), maxMessageBytes1.value)
    assertFalse(maxMessageBytes1.isDefault)
    assertFalse(maxMessageBytes1.isSensitive)
    assertFalse(maxMessageBytes1.isReadOnly)

    assertEquals(topicConfig1.get(TopicConfig.RETENTION_MS_CONFIG),
      configs.get(topicResource1).get(TopicConfig.RETENTION_MS_CONFIG).value)

    val maxMessageBytes2 = configs.get(topicResource2).get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG)
    assertEquals(LogConfig.DEFAULT_MAX_MESSAGE_BYTES.toString, maxMessageBytes2.value)
    assertEquals(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, maxMessageBytes2.name)
    assertTrue(maxMessageBytes2.isDefault)
    assertFalse(maxMessageBytes2.isSensitive)
    assertFalse(maxMessageBytes2.isReadOnly)

    // Find the number of internal configs that we have explicitly set in the broker config.
    // These will appear when we describe the broker configuration. Other internal configs,
    // that we have not set, will not appear there.
    val numInternalConfigsSet = brokers.head.config.originals.keySet().asScala.count(k => {
      Option(KafkaConfig.configDef.configKeys().get(k)) match {
        case None => false
        case Some(configDef) => configDef.internalConfig
      }
    })
    assertEquals(brokers(1).config.nonInternalValues.size + numInternalConfigsSet,
      configs.get(brokerResource1).entries.size)
    assertEquals(brokers(1).config.brokerId.toString, configs.get(brokerResource1).get(KafkaConfig.BrokerIdProp).value)
    val listenerSecurityProtocolMap = configs.get(brokerResource1).get(KafkaConfig.ListenerSecurityProtocolMapProp)
    assertEquals(brokers(1).config.getString(KafkaConfig.ListenerSecurityProtocolMapProp), listenerSecurityProtocolMap.value)
    assertEquals(KafkaConfig.ListenerSecurityProtocolMapProp, listenerSecurityProtocolMap.name)
    assertFalse(listenerSecurityProtocolMap.isDefault)
    assertFalse(listenerSecurityProtocolMap.isSensitive)
    assertFalse(listenerSecurityProtocolMap.isReadOnly)
    val truststorePassword = configs.get(brokerResource1).get(KafkaConfig.SslTruststorePasswordProp)
    assertEquals(KafkaConfig.SslTruststorePasswordProp, truststorePassword.name)
    assertNull(truststorePassword.value)
    assertFalse(truststorePassword.isDefault)
    assertTrue(truststorePassword.isSensitive)
    assertFalse(truststorePassword.isReadOnly)
    val compressionType = configs.get(brokerResource1).get(KafkaConfig.CompressionTypeProp)
    assertEquals(brokers(1).config.compressionType, compressionType.value)
    assertEquals(KafkaConfig.CompressionTypeProp, compressionType.name)
    assertTrue(compressionType.isDefault)
    assertFalse(compressionType.isSensitive)
    assertFalse(compressionType.isReadOnly)

    assertEquals(brokers(2).config.nonInternalValues.size + numInternalConfigsSet,
      configs.get(brokerResource2).entries.size)
    assertEquals(brokers(2).config.brokerId.toString, configs.get(brokerResource2).get(KafkaConfig.BrokerIdProp).value)
    assertEquals(brokers(2).config.logCleanerThreads.toString,
      configs.get(brokerResource2).get(CleanerConfig.LOG_CLEANER_THREADS_PROP).value)

    checkValidAlterConfigs(client, this, topicResource1, topicResource2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCreatePartitions(quorum: String): Unit = {
    client = Admin.create(createConfig)

    // Create topics
    val topic1 = "create-partitions-topic-1"
    createTopic(topic1)

    val topic2 = "create-partitions-topic-2"
    createTopic(topic2, replicationFactor = 2)

    // assert that both the topics have 1 partition
    val topic1_metadata = getTopicMetadata(client, topic1)
    val topic2_metadata = getTopicMetadata(client, topic2)
    assertEquals(1, topic1_metadata.partitions.size)
    assertEquals(1, topic2_metadata.partitions.size)

    val validateOnly = new CreatePartitionsOptions().validateOnly(true)
    val actuallyDoIt = new CreatePartitionsOptions().validateOnly(false)

    def partitions(topic: String, expectedNumPartitionsOpt: Option[Int]): util.List[TopicPartitionInfo] = {
      getTopicMetadata(client, topic, expectedNumPartitionsOpt = expectedNumPartitionsOpt).partitions
    }

    def numPartitions(topic: String, expectedNumPartitionsOpt: Option[Int]): Int = partitions(topic, expectedNumPartitionsOpt).size

    // validateOnly: try creating a new partition (no assignments), to bring the total to 3 partitions
    var alterResult = client.createPartitions(Map(topic1 ->
      NewPartitions.increaseTo(3)).asJava, validateOnly)
    var altered = alterResult.values.get(topic1).get
    TestUtils.waitForAllPartitionsMetadata(brokers, topic1, expectedNumPartitions = 1)

    // try creating a new partition (no assignments), to bring the total to 3 partitions
    alterResult = client.createPartitions(Map(topic1 ->
      NewPartitions.increaseTo(3)).asJava, actuallyDoIt)
    altered = alterResult.values.get(topic1).get
    TestUtils.waitForAllPartitionsMetadata(brokers, topic1, expectedNumPartitions = 3)

    // validateOnly: now try creating a new partition (with assignments), to bring the total to 3 partitions
    val newPartition2Assignments = asList[util.List[Integer]](asList(0, 1), asList(1, 2))
    alterResult = client.createPartitions(Map(topic2 ->
      NewPartitions.increaseTo(3, newPartition2Assignments)).asJava, validateOnly)
    altered = alterResult.values.get(topic2).get
    TestUtils.waitForAllPartitionsMetadata(brokers, topic2, expectedNumPartitions = 1)

    // now try creating a new partition (with assignments), to bring the total to 3 partitions
    alterResult = client.createPartitions(Map(topic2 ->
      NewPartitions.increaseTo(3, newPartition2Assignments)).asJava, actuallyDoIt)
    altered = alterResult.values.get(topic2).get
    val actualPartitions2 = partitions(topic2, expectedNumPartitionsOpt = Some(3))
    assertEquals(3, actualPartitions2.size)
    assertEquals(Seq(0, 1), actualPartitions2.get(1).replicas.asScala.map(_.id).toList)
    assertEquals(Seq(1, 2), actualPartitions2.get(2).replicas.asScala.map(_.id).toList)

    // loop over error cases calling with+without validate-only
    for (option <- Seq(validateOnly, actuallyDoIt)) {
      val desc = if (option.validateOnly()) "validateOnly" else "validateOnly=false"

      // try a newCount which would be a decrease
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(1)).asJava, option)
      
      var e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidPartitionsException when newCount is a decrease")
      assertTrue(e.getCause.isInstanceOf[InvalidPartitionsException], desc)
      var exceptionMsgStr = if (isKRaftTest()) {
        "The topic create-partitions-topic-1 currently has 3 partition(s); 1 would not be an increase."
      } else {
        "Topic currently has 3 partitions, which is higher than the requested 1."
      }
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try a newCount which would be a noop (without assignment)
      alterResult = client.createPartitions(Map(topic2 ->
        NewPartitions.increaseTo(3)).asJava, option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic2).get,
        () => s"$desc: Expect InvalidPartitionsException when requesting a noop")
      assertTrue(e.getCause.isInstanceOf[InvalidPartitionsException], desc)
      exceptionMsgStr = if (isKRaftTest()) {
        "Topic already has 3 partition(s)."
      } else {
        "Topic already has 3 partitions."
      }
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic2, expectedNumPartitionsOpt = Some(3)), desc)

      // try a newCount which would be a noop (where the assignment matches current state)
      alterResult = client.createPartitions(Map(topic2 ->
        NewPartitions.increaseTo(3, newPartition2Assignments)).asJava, option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic2).get)
      assertTrue(e.getCause.isInstanceOf[InvalidPartitionsException], desc)
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic2, expectedNumPartitionsOpt = Some(3)), desc)

      // try a newCount which would be a noop (where the assignment doesn't match current state)
      alterResult = client.createPartitions(Map(topic2 ->
        NewPartitions.increaseTo(3, newPartition2Assignments.asScala.reverse.toList.asJava)).asJava, option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic2).get)
      assertTrue(e.getCause.isInstanceOf[InvalidPartitionsException], desc)
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic2, expectedNumPartitionsOpt = Some(3)), desc)

      // try a bad topic name
      val unknownTopic = "an-unknown-topic"
      alterResult = client.createPartitions(Map(unknownTopic ->
        NewPartitions.increaseTo(2)).asJava, option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(unknownTopic).get,
        () => s"$desc: Expect InvalidTopicException when using an unknown topic")
      assertTrue(e.getCause.isInstanceOf[UnknownTopicOrPartitionException], desc)
      exceptionMsgStr = if (isKRaftTest()) {
        "This server does not host this topic-partition."
      } else {
        "The topic 'an-unknown-topic' does not exist."
      }
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)

      // try an invalid newCount
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(-22)).asJava, option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidPartitionsException when newCount is invalid")
      assertTrue(e.getCause.isInstanceOf[InvalidPartitionsException], desc)
      exceptionMsgStr = if (isKRaftTest()) {
        "The topic create-partitions-topic-1 currently has 3 partition(s); -22 would not be an increase."
      } else {
        "Topic currently has 3 partitions, which is higher than the requested -22."
      }
      assertEquals(exceptionMsgStr, e.getCause.getMessage,
        desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try assignments where the number of brokers != replication factor
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(4, asList(asList(1, 2)))).asJava, option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidPartitionsException when #brokers != replication factor")
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      exceptionMsgStr = if (isKRaftTest()) {
        "The manual partition assignment includes a partition with 2 replica(s), but this is not " +
          "consistent with previous partitions, which have 1 replica(s)."
      } else {
        "Inconsistent replication factor between partitions, partition 0 has 1 while partitions [3] " +
          "have replication factors [2], respectively."
      }
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try #assignments < with the increase
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(6, asList(asList(1)))).asJava, option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidReplicaAssignmentException when #assignments != newCount - oldCount")
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      exceptionMsgStr = if (isKRaftTest()) {
        "Attempted to add 3 additional partition(s), but only 1 assignment(s) were specified."
      } else {
        "Increasing the number of partitions by 3 but 1 assignments provided."
      }
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try #assignments > with the increase
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(4, asList(asList(1), asList(2)))).asJava, option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidReplicaAssignmentException when #assignments != newCount - oldCount")
      exceptionMsgStr = if (isKRaftTest()) {
        "Attempted to add 1 additional partition(s), but only 2 assignment(s) were specified."
      } else {
        "Increasing the number of partitions by 1 but 2 assignments provided."
      }
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try with duplicate brokers in assignments
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(4, asList(asList(1, 1)))).asJava, option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidReplicaAssignmentException when assignments has duplicate brokers")
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      exceptionMsgStr = if (isKRaftTest()) {
        "The manual partition assignment includes the broker 1 more than once."
      } else {
        "Duplicate brokers not allowed in replica assignment: 1, 1 for partition id 3."
      }
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try assignments with differently sized inner lists
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(5, asList(asList(1), asList(1, 0)))).asJava, option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidReplicaAssignmentException when assignments have differently sized inner lists")
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      exceptionMsgStr = if (isKRaftTest()) {
        "The manual partition assignment includes a partition with 2 replica(s), but this is not " +
          "consistent with previous partitions, which have 1 replica(s)."
      } else {
        "Inconsistent replication factor between partitions, partition 0 has 1 " +
          "while partitions [4] have replication factors [2], respectively."
      }
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try assignments with unknown brokers
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(4, asList(asList(12)))).asJava, option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidReplicaAssignmentException when assignments contains an unknown broker")
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      exceptionMsgStr = if (isKRaftTest()) {
        "The manual partition assignment includes broker 12, but no such broker is registered."
      } else {
        "Unknown broker(s) in replica assignment: 12."
      }
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)

      // try with empty assignments
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(4, Collections.emptyList())).asJava, option)
      e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
        () => s"$desc: Expect InvalidReplicaAssignmentException when assignments is empty")
      assertTrue(e.getCause.isInstanceOf[InvalidReplicaAssignmentException], desc)
      exceptionMsgStr = if (isKRaftTest()) {
        "Attempted to add 1 additional partition(s), but only 0 assignment(s) were specified."
      } else {
        "Increasing the number of partitions by 1 but 0 assignments provided."
      }
      assertEquals(exceptionMsgStr, e.getCause.getMessage, desc)
      assertEquals(3, numPartitions(topic1, expectedNumPartitionsOpt = Some(3)), desc)
    }

    // a mixed success, failure response
    alterResult = client.createPartitions(Map(
      topic1 -> NewPartitions.increaseTo(4),
      topic2 -> NewPartitions.increaseTo(2)).asJava, actuallyDoIt)
    // assert that the topic1 now has 4 partitions
    altered = alterResult.values.get(topic1).get
    TestUtils.waitForAllPartitionsMetadata(brokers, topic1, expectedNumPartitions = 4)
    var e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic2).get)
    assertTrue(e.getCause.isInstanceOf[InvalidPartitionsException])
    val exceptionMsgStr = if (isKRaftTest()) {
      "The topic create-partitions-topic-2 currently has 3 partition(s); 2 would not be an increase."
    } else {
      "Topic currently has 3 partitions, which is higher than the requested 2."
    }
    assertEquals(exceptionMsgStr, e.getCause.getMessage)
    TestUtils.waitForAllPartitionsMetadata(brokers, topic2, expectedNumPartitions = 3)

    // Delete the topic. Verify addition of partitions to deleted topic is not possible. In
    // Zookeeper mode, the topic is queued for deletion. In KRaft, the deletion occurs
    // immediately and hence we have a different Exception thrown in the response.
    val deleteResult = client.deleteTopics(asList(topic1))
    deleteResult.topicNameValues.get(topic1).get
    alterResult = client.createPartitions(Map(topic1 ->
      NewPartitions.increaseTo(4)).asJava, validateOnly)
    e = assertThrows(classOf[ExecutionException], () => alterResult.values.get(topic1).get,
      () => "Expect InvalidTopicException or UnknownTopicOrPartitionException when the topic is queued for deletion")
    if (isKRaftTest()) {
      assertTrue(e.getCause.isInstanceOf[UnknownTopicOrPartitionException], e.toString)
      assertEquals("This server does not host this topic-partition.", e.getCause.getMessage)
    } else {
      assertTrue(e.getCause.isInstanceOf[InvalidTopicException], e.toString)
      assertEquals("The topic is queued for deletion.", e.getCause.getMessage)
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testSeekAfterDeleteRecords(quorum: String): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)

    client = Admin.create(createConfig)

    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)
    consumer.seekToBeginning(Collections.singleton(topicPartition))
    assertEquals(0L, consumer.position(topicPartition))

    val result = client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(5L)).asJava)
    val lowWatermark = result.lowWatermarks().get(topicPartition).get.lowWatermark
    assertEquals(5L, lowWatermark)

    consumer.seekToBeginning(Collections.singletonList(topicPartition))
    assertEquals(5L, consumer.position(topicPartition))

    consumer.seek(topicPartition, 7L)
    assertEquals(7L, consumer.position(topicPartition))

    client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(DeleteRecordsRequest.HIGH_WATERMARK)).asJava).all.get
    consumer.seekToBeginning(Collections.singletonList(topicPartition))
    assertEquals(10L, consumer.position(topicPartition))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testLogStartOffsetCheckpoint(quorum: String): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)

    client = Admin.create(createConfig)

    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)
    var result = client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(5L)).asJava)
    var lowWatermark: Option[Long] = Some(result.lowWatermarks.get(topicPartition).get.lowWatermark)
    assertEquals(Some(5), lowWatermark)

    for (i <- 0 until brokerCount) {
      killBroker(i)
    }
    restartDeadBrokers()

    client.close()
    client = Admin.create(createConfig)

    TestUtils.waitUntilTrue(() => {
      // Need to retry if leader is not available for the partition
      result = client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(0L)).asJava)

      lowWatermark = None
      val future = result.lowWatermarks().get(topicPartition)
      try {
        lowWatermark = Some(future.get.lowWatermark)
        lowWatermark.contains(5L)
      } catch {
        case e: ExecutionException if e.getCause.isInstanceOf[LeaderNotAvailableException] ||
          e.getCause.isInstanceOf[NotLeaderOrFollowerException] => false
        }
    }, s"Expected low watermark of the partition to be 5 but got ${lowWatermark.getOrElse("no response within the timeout")}")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testLogStartOffsetAfterDeleteRecords(quorum: String): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)

    client = Admin.create(createConfig)

    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)

    val result = client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(3L)).asJava)
    val lowWatermark = result.lowWatermarks.get(topicPartition).get.lowWatermark
    assertEquals(3L, lowWatermark)

    for (i <- 0 until brokerCount)
      assertEquals(3, brokers(i).replicaManager.localLog(topicPartition).get.logStartOffset)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testReplicaCanFetchFromLogStartOffsetAfterDeleteRecords(quorum: String): Unit = {
    val leaders = createTopic(topic, replicationFactor = brokerCount)
    val followerIndex = if (leaders(0) != brokers.head.config.brokerId) 0 else 1

    def waitForFollowerLog(expectedStartOffset: Long, expectedEndOffset: Long): Unit = {
      TestUtils.waitUntilTrue(() => brokers(followerIndex).replicaManager.localLog(topicPartition).isDefined,
                              "Expected follower to create replica for partition")

      // wait until the follower discovers that log start offset moved beyond its HW
      TestUtils.waitUntilTrue(() => {
        brokers(followerIndex).replicaManager.localLog(topicPartition).get.logStartOffset == expectedStartOffset
      }, s"Expected follower to discover new log start offset $expectedStartOffset")

      TestUtils.waitUntilTrue(() => {
        brokers(followerIndex).replicaManager.localLog(topicPartition).get.logEndOffset == expectedEndOffset
      }, s"Expected follower to catch up to log end offset $expectedEndOffset")
    }

    // we will produce to topic and delete records while one follower is down
    killBroker(followerIndex)

    client = Admin.create(createConfig)
    val producer = createProducer()
    sendRecords(producer, 100, topicPartition)

    val result = client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(3L)).asJava)
    result.all().get()

    // start the stopped broker to verify that it will be able to fetch from new log start offset
    restartDeadBrokers()

    waitForFollowerLog(expectedStartOffset=3L, expectedEndOffset=100L)

    // after the new replica caught up, all replicas should have same log start offset
    for (i <- 0 until brokerCount)
      assertEquals(3, brokers(i).replicaManager.localLog(topicPartition).get.logStartOffset)

    // kill the same follower again, produce more records, and delete records beyond follower's LOE
    killBroker(followerIndex)
    sendRecords(producer, 100, topicPartition)
    val result1 = client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(117L)).asJava)
    result1.all().get()
    restartDeadBrokers()
    TestUtils.waitForBrokersInIsr(client, topicPartition, Set(followerIndex))
    waitForFollowerLog(expectedStartOffset=117L, expectedEndOffset=200L)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testAlterLogDirsAfterDeleteRecords(quorum: String): Unit = {
    client = Admin.create(createConfig)
    createTopic(topic, replicationFactor = brokerCount)
    val expectedLEO = 100
    val producer = createProducer()
    sendRecords(producer, expectedLEO, topicPartition)

    // delete records to move log start offset
    val result = client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(3L)).asJava)
    result.all().get()
    // make sure we are in the expected state after delete records
    for (i <- 0 until brokerCount) {
      assertEquals(3, brokers(i).replicaManager.localLog(topicPartition).get.logStartOffset)
      assertEquals(expectedLEO, brokers(i).replicaManager.localLog(topicPartition).get.logEndOffset)
    }

    // we will create another dir just for one server
    val futureLogDir = brokers(0).config.logDirs(1)
    val futureReplica = new TopicPartitionReplica(topic, 0, brokers(0).config.brokerId)

    // Verify that replica can be moved to the specified log directory
    client.alterReplicaLogDirs(Map(futureReplica -> futureLogDir).asJava).all.get
    TestUtils.waitUntilTrue(() => {
      futureLogDir == brokers(0).logManager.getLog(topicPartition).get.dir.getParent
    }, "timed out waiting for replica movement")

    // once replica moved, its LSO and LEO should match other replicas
    assertEquals(3, brokers.head.replicaManager.localLog(topicPartition).get.logStartOffset)
    assertEquals(expectedLEO, brokers.head.replicaManager.localLog(topicPartition).get.logEndOffset)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testOffsetsForTimesAfterDeleteRecords(quorum: String): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)

    client = Admin.create(createConfig)

    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)
    assertEquals(0L, consumer.offsetsForTimes(Map(topicPartition -> JLong.valueOf(0L)).asJava).get(topicPartition).offset())

    var result = client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(5L)).asJava)
    result.all.get
    assertEquals(5L, consumer.offsetsForTimes(Map(topicPartition -> JLong.valueOf(0L)).asJava).get(topicPartition).offset())

    result = client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(DeleteRecordsRequest.HIGH_WATERMARK)).asJava)
    result.all.get
    assertNull(consumer.offsetsForTimes(Map(topicPartition -> JLong.valueOf(0L)).asJava).get(topicPartition))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testConsumeAfterDeleteRecords(quorum: String): Unit = {
    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    client = Admin.create(createConfig)

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)
    var messageCount = 0
    TestUtils.consumeRecords(consumer, 10)

    client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(3L)).asJava).all.get
    consumer.seek(topicPartition, 1)
    messageCount = 0
    TestUtils.consumeRecords(consumer, 7)

    client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(8L)).asJava).all.get
    consumer.seek(topicPartition, 1)
    messageCount = 0
    TestUtils.consumeRecords(consumer, 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDeleteRecordsWithException(quorum: String): Unit = {
    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    client = Admin.create(createConfig)

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)

    assertEquals(5L, client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(5L)).asJava)
      .lowWatermarks.get(topicPartition).get.lowWatermark)

    // OffsetOutOfRangeException if offset > high_watermark
    val cause = assertThrows(classOf[ExecutionException],
      () => client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(20L)).asJava).lowWatermarks.get(topicPartition).get).getCause
    assertEquals(classOf[OffsetOutOfRangeException], cause.getClass)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDescribeConfigsForTopic(quorum: String): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)
    client = Admin.create(createConfig)

    val existingTopic = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    client.describeConfigs(Collections.singletonList(existingTopic)).values.get(existingTopic).get()

    val nonExistentTopic = new ConfigResource(ConfigResource.Type.TOPIC, "unknown")
    val describeResult1 = client.describeConfigs(Collections.singletonList(nonExistentTopic))

    assertTrue(assertThrows(classOf[ExecutionException], () => describeResult1.values.get(nonExistentTopic).get).getCause.isInstanceOf[UnknownTopicOrPartitionException])

    val invalidTopic = new ConfigResource(ConfigResource.Type.TOPIC, "(invalid topic)")
    val describeResult2 = client.describeConfigs(Collections.singletonList(invalidTopic))

    assertTrue(assertThrows(classOf[ExecutionException], () => describeResult2.values.get(invalidTopic).get).getCause.isInstanceOf[InvalidTopicException])
  }

  private def subscribeAndWaitForAssignment(topic: String, consumer: Consumer[Array[Byte], Array[Byte]]): Unit = {
    consumer.subscribe(Collections.singletonList(topic))
    TestUtils.pollUntilTrue(consumer, () => !consumer.assignment.isEmpty, "Expected non-empty assignment")
  }

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]],
                          numRecords: Int,
                          topicPartition: TopicPartition): Unit = {
    val futures = (0 until numRecords).map( i => {
      val record = new ProducerRecord(topicPartition.topic, topicPartition.partition, s"$i".getBytes, s"$i".getBytes)
      debug(s"Sending this record: $record")
      producer.send(record)
    })

    futures.foreach(_.get)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testInvalidAlterConfigs(quorum: String): Unit = {
    client = Admin.create(createConfig)
    checkInvalidAlterConfigs(this, client)
  }

  /**
   * Test that ACL operations are not possible when the authorizer is disabled.
   * Also see [[kafka.api.SaslSslAdminIntegrationTest.testAclOperations()]] for tests of ACL operations
   * when the authorizer is enabled.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testAclOperations(quorum: String): Unit = {
    val acl = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))
    client = Admin.create(createConfig)
    assertFutureExceptionTypeEquals(client.describeAcls(AclBindingFilter.ANY).values(), classOf[SecurityDisabledException])
    assertFutureExceptionTypeEquals(client.createAcls(Collections.singleton(acl)).all(),
      classOf[SecurityDisabledException])
    assertFutureExceptionTypeEquals(client.deleteAcls(Collections.singleton(acl.toFilter())).all(),
      classOf[SecurityDisabledException])
  }

  /**
    * Test closing the AdminClient with a generous timeout.  Calls in progress should be completed,
    * since they can be done within the timeout.  New calls should receive exceptions.
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDelayedClose(quorum: String): Unit = {
    client = Admin.create(createConfig)
    val topics = Seq("mytopic", "mytopic2")
    val newTopics = topics.map(new NewTopic(_, 1, 1.toShort))
    val future = client.createTopics(newTopics.asJava, new CreateTopicsOptions().validateOnly(true)).all()
    client.close(time.Duration.ofHours(2))
    val future2 = client.createTopics(newTopics.asJava, new CreateTopicsOptions().validateOnly(true)).all()
    assertFutureExceptionTypeEquals(future2, classOf[IllegalStateException])
    future.get
    client.close(time.Duration.ofMinutes(30)) // multiple close-with-timeout should have no effect
  }

  /**
    * Test closing the AdminClient with a timeout of 0, when there are calls with extremely long
    * timeouts in progress.  The calls should be aborted after the hard shutdown timeout elapses.
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testForceClose(quorum: String): Unit = {
    val config = createConfig
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${TestUtils.IncorrectBrokerPort}")
    client = Admin.create(config)
    // Because the bootstrap servers are set up incorrectly, this call will not complete, but must be
    // cancelled by the close operation.
    val future = client.createTopics(Seq("mytopic", "mytopic2").map(new NewTopic(_, 1, 1.toShort)).asJava,
      new CreateTopicsOptions().timeoutMs(900000)).all()
    client.close(time.Duration.ZERO)
    assertFutureExceptionTypeEquals(future, classOf[TimeoutException])
  }

  /**
    * Check that a call with a timeout does not complete before the minimum timeout has elapsed,
    * even when the default request timeout is shorter.
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testMinimumRequestTimeouts(quorum: String): Unit = {
    val config = createConfig
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${TestUtils.IncorrectBrokerPort}")
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "0")
    client = Admin.create(config)
    val startTimeMs = Time.SYSTEM.milliseconds()
    val future = client.createTopics(Seq("mytopic", "mytopic2").map(new NewTopic(_, 1, 1.toShort)).asJava,
      new CreateTopicsOptions().timeoutMs(2)).all()
    assertFutureExceptionTypeEquals(future, classOf[TimeoutException])
    val endTimeMs = Time.SYSTEM.milliseconds()
    assertTrue(endTimeMs > startTimeMs, "Expected the timeout to take at least one millisecond.")
  }

  /**
    * Test injecting timeouts for calls that are in flight.
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCallInFlightTimeouts(quorum: String): Unit = {
    val config = createConfig
    config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "100000000")
    config.put(AdminClientConfig.RETRIES_CONFIG, "0")
    val factory = new KafkaAdminClientTest.FailureInjectingTimeoutProcessorFactory()
    client = KafkaAdminClientTest.createInternal(new AdminClientConfig(config), factory)
    val future = client.createTopics(Seq("mytopic", "mytopic2").map(new NewTopic(_, 1, 1.toShort)).asJava,
        new CreateTopicsOptions().validateOnly(true)).all()
    assertFutureExceptionTypeEquals(future, classOf[TimeoutException])
    val future2 = client.createTopics(Seq("mytopic3", "mytopic4").map(new NewTopic(_, 1, 1.toShort)).asJava,
      new CreateTopicsOptions().validateOnly(true)).all()
    future2.get
    assertEquals(1, factory.failuresInjected)
  }

  /**
   * Test the consumer group APIs.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testConsumerGroups(quorum: String): Unit = {
    val config = createConfig
    client = Admin.create(config)
    try {
      // Verify that initially there are no consumer groups to list.
      val list1 = client.listConsumerGroups()
      assertTrue(0 == list1.all().get().size())
      assertTrue(0 == list1.errors().get().size())
      assertTrue(0 == list1.valid().get().size())
      val testTopicName = "test_topic"
      val testTopicName1 = testTopicName + "1"
      val testTopicName2 = testTopicName + "2"
      val testNumPartitions = 2

      client.createTopics(util.Arrays.asList(
        new NewTopic(testTopicName, testNumPartitions, 1.toShort),
        new NewTopic(testTopicName1, testNumPartitions, 1.toShort),
        new NewTopic(testTopicName2, testNumPartitions, 1.toShort)
      )).all().get()
      waitForTopics(client, List(testTopicName, testTopicName1, testTopicName2), List())

      val producer = createProducer()
      try {
        producer.send(new ProducerRecord(testTopicName, 0, null, null)).get()
      } finally {
        Utils.closeQuietly(producer, "producer")
      }

      val EMPTY_GROUP_INSTANCE_ID = ""
      val testGroupId = "test_group_id"
      val testClientId = "test_client_id"
      val testInstanceId1 = "test_instance_id_1"
      val testInstanceId2 = "test_instance_id_2"
      val fakeGroupId = "fake_group_id"

      def createProperties(groupInstanceId: String): Properties = {
        val newConsumerConfig = new Properties(consumerConfig)
        // We need to disable the auto commit because after the members got removed from group, the offset commit
        // will cause the member rejoining and the test will be flaky (check ConsumerCoordinator#OffsetCommitResponseHandler)
        newConsumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        newConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
        newConsumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testClientId)
        if (groupInstanceId != EMPTY_GROUP_INSTANCE_ID) {
          newConsumerConfig.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId)
        }
        newConsumerConfig
      }

      // contains two static members and one dynamic member
      val groupInstanceSet = Set(testInstanceId1, testInstanceId2, EMPTY_GROUP_INSTANCE_ID)
      val consumerSet = groupInstanceSet.map { groupInstanceId => createConsumer(configOverrides = createProperties(groupInstanceId))}
      val topicSet = Set(testTopicName, testTopicName1, testTopicName2)

      val latch = new CountDownLatch(consumerSet.size)
      try {
        def createConsumerThread[K,V](consumer: Consumer[K,V], topic: String): Thread = {
          new Thread {
            override def run : Unit = {
              consumer.subscribe(Collections.singleton(topic))
              try {
                while (true) {
                  consumer.poll(JDuration.ofSeconds(5))
                  if (!consumer.assignment.isEmpty && latch.getCount > 0L)
                    latch.countDown()
                  consumer.commitSync()
                }
              } catch {
                case _: InterruptException => // Suppress the output to stderr
              }
            }
          }
        }

        // Start consumers in a thread that will subscribe to a new group.
        val consumerThreads = consumerSet.zip(topicSet).map(zipped => createConsumerThread(zipped._1, zipped._2))

        try {
          consumerThreads.foreach(_.start())
          assertTrue(latch.await(30000, TimeUnit.MILLISECONDS))
          // Test that we can list the new group.
          TestUtils.waitUntilTrue(() => {
            val matching = client.listConsumerGroups.all.get.asScala.filter(group =>
                group.groupId == testGroupId &&
                group.state.get == ConsumerGroupState.STABLE)
            matching.size == 1
          }, s"Expected to be able to list $testGroupId")

          TestUtils.waitUntilTrue(() => {
            val options = new ListConsumerGroupsOptions().inStates(Set(ConsumerGroupState.STABLE).asJava)
            val matching = client.listConsumerGroups(options).all.get.asScala.filter(group =>
                group.groupId == testGroupId &&
                group.state.get == ConsumerGroupState.STABLE)
            matching.size == 1
          }, s"Expected to be able to list $testGroupId in state Stable")

          TestUtils.waitUntilTrue(() => {
            val options = new ListConsumerGroupsOptions().inStates(Set(ConsumerGroupState.EMPTY).asJava)
            val matching = client.listConsumerGroups(options).all.get.asScala.filter(
                _.groupId == testGroupId)
            matching.isEmpty
          }, s"Expected to find zero groups")

          val describeWithFakeGroupResult = client.describeConsumerGroups(Seq(testGroupId, fakeGroupId).asJava,
            new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
          assertEquals(2, describeWithFakeGroupResult.describedGroups().size())

          // Test that we can get information about the test consumer group.
          assertTrue(describeWithFakeGroupResult.describedGroups().containsKey(testGroupId))
          var testGroupDescription = describeWithFakeGroupResult.describedGroups().get(testGroupId).get()

          assertEquals(testGroupId, testGroupDescription.groupId())
          assertFalse(testGroupDescription.isSimpleConsumerGroup)
          assertEquals(groupInstanceSet.size, testGroupDescription.members().size())
          val members = testGroupDescription.members()
          members.asScala.foreach(member => assertEquals(testClientId, member.clientId()))
          val topicPartitionsByTopic = members.asScala.flatMap(_.assignment().topicPartitions().asScala).groupBy(_.topic())
          topicSet.foreach { topic =>
            val topicPartitions = topicPartitionsByTopic.getOrElse(topic, List.empty)
            assertEquals(testNumPartitions, topicPartitions.size)
          }

          val expectedOperations = AclEntry.supportedOperations(ResourceType.GROUP).asJava
          assertEquals(expectedOperations, testGroupDescription.authorizedOperations())

          // Test that the fake group is listed as dead.
          assertTrue(describeWithFakeGroupResult.describedGroups().containsKey(fakeGroupId))
          val fakeGroupDescription = describeWithFakeGroupResult.describedGroups().get(fakeGroupId).get()

          assertEquals(fakeGroupId, fakeGroupDescription.groupId())
          assertEquals(0, fakeGroupDescription.members().size())
          assertEquals("", fakeGroupDescription.partitionAssignor())
          assertEquals(ConsumerGroupState.DEAD, fakeGroupDescription.state())
          assertEquals(expectedOperations, fakeGroupDescription.authorizedOperations())

          // Test that all() returns 2 results
          assertEquals(2, describeWithFakeGroupResult.all().get().size())

          // Test listConsumerGroupOffsets
          TestUtils.waitUntilTrue(() => {
            val parts = client.listConsumerGroupOffsets(testGroupId).partitionsToOffsetAndMetadata().get()
            val part = new TopicPartition(testTopicName, 0)
            parts.containsKey(part) && (parts.get(part).offset() == 1)
          }, s"Expected the offset for partition 0 to eventually become 1.")

          // Test delete non-exist consumer instance
          val invalidInstanceId = "invalid-instance-id"
          var removeMembersResult = client.removeMembersFromConsumerGroup(testGroupId, new RemoveMembersFromConsumerGroupOptions(
            Collections.singleton(new MemberToRemove(invalidInstanceId))
          ))

          TestUtils.assertFutureExceptionTypeEquals(removeMembersResult.all, classOf[UnknownMemberIdException])
          val firstMemberFuture = removeMembersResult.memberResult(new MemberToRemove(invalidInstanceId))
          TestUtils.assertFutureExceptionTypeEquals(firstMemberFuture, classOf[UnknownMemberIdException])

          // Test consumer group deletion
          var deleteResult = client.deleteConsumerGroups(Seq(testGroupId, fakeGroupId).asJava)
          assertEquals(2, deleteResult.deletedGroups().size())

          // Deleting the fake group ID should get GroupIdNotFoundException.
          assertTrue(deleteResult.deletedGroups().containsKey(fakeGroupId))
          assertFutureExceptionTypeEquals(deleteResult.deletedGroups().get(fakeGroupId),
            classOf[GroupIdNotFoundException])

          // Deleting the real group ID should get GroupNotEmptyException
          assertTrue(deleteResult.deletedGroups().containsKey(testGroupId))
          assertFutureExceptionTypeEquals(deleteResult.deletedGroups().get(testGroupId),
            classOf[GroupNotEmptyException])

          // Test delete one correct static member
          removeMembersResult = client.removeMembersFromConsumerGroup(testGroupId, new RemoveMembersFromConsumerGroupOptions(
            Collections.singleton(new MemberToRemove(testInstanceId1))
          ))

          assertNull(removeMembersResult.all().get())
          val validMemberFuture = removeMembersResult.memberResult(new MemberToRemove(testInstanceId1))
          assertNull(validMemberFuture.get())

          val describeTestGroupResult = client.describeConsumerGroups(Seq(testGroupId).asJava,
            new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
          assertEquals(1, describeTestGroupResult.describedGroups().size())

          testGroupDescription = describeTestGroupResult.describedGroups().get(testGroupId).get()

          assertEquals(testGroupId, testGroupDescription.groupId)
          assertFalse(testGroupDescription.isSimpleConsumerGroup)
          assertEquals(consumerSet.size - 1, testGroupDescription.members().size())

          // Delete all active members remaining (a static member + a dynamic member)
          removeMembersResult = client.removeMembersFromConsumerGroup(testGroupId, new RemoveMembersFromConsumerGroupOptions())
          assertNull(removeMembersResult.all().get())

          // The group should contain no members now.
          testGroupDescription = client.describeConsumerGroups(Seq(testGroupId).asJava,
            new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
              .describedGroups().get(testGroupId).get()
          assertTrue(testGroupDescription.members().isEmpty)

          // Consumer group deletion on empty group should succeed
          deleteResult = client.deleteConsumerGroups(Seq(testGroupId).asJava)
          assertEquals(1, deleteResult.deletedGroups().size())

          assertTrue(deleteResult.deletedGroups().containsKey(testGroupId))
          assertNull(deleteResult.deletedGroups().get(testGroupId).get())
      } finally {
        consumerThreads.foreach {
          case consumerThread =>
            consumerThread.interrupt()
            consumerThread.join()
        }
      }
      } finally {
        consumerSet.zip(groupInstanceSet).foreach(zipped => Utils.closeQuietly(zipped._1, zipped._2))
      }
    } finally {
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDeleteConsumerGroupOffsets(quorum: String): Unit = {
    val config = createConfig
    client = Admin.create(config)
    try {
      val testTopicName = "test_topic"
      val testGroupId = "test_group_id"
      val testClientId = "test_client_id"
      val fakeGroupId = "fake_group_id"

      val tp1 = new TopicPartition(testTopicName, 0)
      val tp2 = new TopicPartition("foo", 0)

      client.createTopics(Collections.singleton(
        new NewTopic(testTopicName, 1, 1.toShort))).all().get()
      waitForTopics(client, List(testTopicName), List())

      val producer = createProducer()
      try {
        producer.send(new ProducerRecord(testTopicName, 0, null, null)).get()
      } finally {
        Utils.closeQuietly(producer, "producer")
      }

      val newConsumerConfig = new Properties(consumerConfig)
      newConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
      newConsumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testClientId)
      // Increase timeouts to avoid having a rebalance during the test
      newConsumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.MAX_VALUE.toString)
      newConsumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Defaults.GROUP_MAX_SESSION_TIMEOUT_MS.toString)
      val consumer = createConsumer(configOverrides = newConsumerConfig)

      try {
        TestUtils.subscribeAndWaitForRecords(testTopicName, consumer)
        consumer.commitSync()

        // Test offset deletion while consuming
        val offsetDeleteResult = client.deleteConsumerGroupOffsets(testGroupId, Set(tp1, tp2).asJava)

        // Top level error will equal to the first partition level error
        assertFutureExceptionTypeEquals(offsetDeleteResult.all(), classOf[GroupSubscribedToTopicException])
        assertFutureExceptionTypeEquals(offsetDeleteResult.partitionResult(tp1),
          classOf[GroupSubscribedToTopicException])
        assertFutureExceptionTypeEquals(offsetDeleteResult.partitionResult(tp2),
          classOf[UnknownTopicOrPartitionException])

        // Test the fake group ID
        val fakeDeleteResult = client.deleteConsumerGroupOffsets(fakeGroupId, Set(tp1, tp2).asJava)

        assertFutureExceptionTypeEquals(fakeDeleteResult.all(), classOf[GroupIdNotFoundException])
        assertFutureExceptionTypeEquals(fakeDeleteResult.partitionResult(tp1),
          classOf[GroupIdNotFoundException])
        assertFutureExceptionTypeEquals(fakeDeleteResult.partitionResult(tp2),
          classOf[GroupIdNotFoundException])

      } finally {
        Utils.closeQuietly(consumer, "consumer")
      }

      // Test offset deletion when group is empty
      val offsetDeleteResult = client.deleteConsumerGroupOffsets(testGroupId, Set(tp1, tp2).asJava)

      assertFutureExceptionTypeEquals(offsetDeleteResult.all(),
        classOf[UnknownTopicOrPartitionException])
      assertNull(offsetDeleteResult.partitionResult(tp1).get())
      assertFutureExceptionTypeEquals(offsetDeleteResult.partitionResult(tp2),
        classOf[UnknownTopicOrPartitionException])
    } finally {
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testElectPreferredLeaders(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val prefer0 = Seq(0, 1, 2)
    val prefer1 = Seq(1, 2, 0)
    val prefer2 = Seq(2, 0, 1)

    val partition1 = new TopicPartition("elect-preferred-leaders-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> prefer0))

    val partition2 = new TopicPartition("elect-preferred-leaders-topic-2", 0)
    createTopicWithAssignment(partition2.topic, Map[Int, Seq[Int]](partition2.partition -> prefer0))

    def preferredLeader(topicPartition: TopicPartition): Int = {
      val partitionMetadata = getTopicMetadata(client, topicPartition.topic).partitions.get(topicPartition.partition)
      val preferredLeaderMetadata = partitionMetadata.replicas.get(0)
      preferredLeaderMetadata.id
    }

    /** Changes the <i>preferred</i> leader without changing the <i>current</i> leader. */
    def changePreferredLeader(newAssignment: Seq[Int]): Unit = {
      val preferred = newAssignment.head
      val prior1 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition1.topic, partition1.partition(), listenerName).get.id()
      val prior2 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition2.topic, partition2.partition(), listenerName).get.id()

      var m = Map.empty[TopicPartition, Optional[NewPartitionReassignment]]
      if (prior1 != preferred)
        m += partition1 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      if (prior2 != preferred)
        m += partition2 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      client.alterPartitionReassignments(m.asJava).all().get()

      TestUtils.waitUntilTrue(
        () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
        s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
        10000)
      // Check the leader hasn't moved
      TestUtils.assertLeader(client, partition1, prior1)
      TestUtils.assertLeader(client, partition2, prior2)
    }

    // Check current leaders are 0
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Noop election
    var electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    val exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[ElectionNotNeededException], exception.getClass)
    TestUtils.assertLeader(client, partition1, 0)

    // Noop election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertTrue(electResult.partitions.get.isEmpty)
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)

    // meaningful election
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)
    electResult.partitions.get.get(partition1)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition1"))
    TestUtils.assertLeader(client, partition1, 1)

    // topic 2 unchanged
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition2, 0)

    // meaningful election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertEquals(Set(partition2), electResult.partitions.get.keySet.asScala)
    electResult.partitions.get.get(partition2)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition2"))
    TestUtils.assertLeader(client, partition2, 1)

    def assertUnknownTopicOrPartition(
      topicPartition: TopicPartition,
      result: ElectLeadersResult
    ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
      if (isKRaftTest()) {
        assertEquals(s"No such topic as ${topicPartition.topic()}", exception.getMessage)
      } else {
        assertEquals("The partition does not exist.", exception.getMessage)
      }
    }

    // unknown topic
    val unknownPartition = new TopicPartition("topic-does-not-exist", 0)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition).asJava)
    assertEquals(Set(unknownPartition).asJava, electResult.partitions.get.keySet)
    assertUnknownTopicOrPartition(unknownPartition, electResult)
    TestUtils.assertLeader(client, partition1, 1)
    TestUtils.assertLeader(client, partition2, 1)

    // Now change the preferred leader to 2
    changePreferredLeader(prefer2)

    // mixed results
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition, partition1).asJava)
    assertEquals(Set(unknownPartition, partition1).asJava, electResult.partitions.get.keySet)
    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 1)
    assertUnknownTopicOrPartition(unknownPartition, electResult)

    // elect preferred leader for partition 2
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition2).asJava)
    assertEquals(Set(partition2).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 2)

    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)
    // but shut it down...
    killBroker(1)
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(1))

    def assertPreferredLeaderNotAvailable(
      topicPartition: TopicPartition,
      result: ElectLeadersResult
    ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
      if (isKRaftTest()) {
        assertTrue(exception.getMessage.contains(
          "The preferred leader was not available."),
          s"Unexpected message: ${exception.getMessage}")
      } else {
        assertTrue(exception.getMessage.contains(
          s"Failed to elect leader for partition $topicPartition under strategy PreferredReplicaPartitionLeaderElectionStrategy"),
          s"Unexpected message: ${exception.getMessage}")
      }
    }

    // ... now what happens if we try to elect the preferred leader and it's down?
    val shortTimeout = new ElectLeadersOptions().timeoutMs(10000)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava, shortTimeout)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    // preferred leader unavailable with null argument
    electResult = client.electLeaders(ElectionType.PREFERRED, null, shortTimeout)
    assertTrue(Set(partition1, partition2).subsetOf(electResult.partitions.get.keySet.asScala))

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    assertPreferredLeaderNotAvailable(partition2, electResult)
    TestUtils.assertLeader(client, partition2, 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testElectUncleanLeadersForOnePartition(quorum: String): Unit = {
    // Case: unclean leader election with one topic partition
    client = Admin.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)

    val partition1 = new TopicPartition("unclean-test-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> assignment1))

    TestUtils.assertLeader(client, partition1, broker1)

    killBroker(broker2)
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1), Set(broker2))
    killBroker(broker1)
    TestUtils.assertNoLeader(client, partition1)
    brokers(broker2).startup()
    TestUtils.waitForOnlineBroker(client, broker2)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, Set(partition1).asJava)
    electResult.partitions.get.get(partition1)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition1"))
    TestUtils.assertLeader(client, partition1, broker2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testElectUncleanLeadersForManyPartitions(quorum: String): Unit = {
    // Case: unclean leader election with many topic partitions
    client = Admin.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)
    val assignment2 = Seq(broker1, broker2)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)
    val partition2 = new TopicPartition(topic, 1)

    createTopicWithAssignment(
      topic,
      Map(partition1.partition -> assignment1, partition2.partition -> assignment2)
    )

    TestUtils.assertLeader(client, partition1, broker1)
    TestUtils.assertLeader(client, partition2, broker1)

    killBroker(broker2)
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(broker2))
    killBroker(broker1)
    TestUtils.assertNoLeader(client, partition1)
    TestUtils.assertNoLeader(client, partition2)
    brokers(broker2).startup()
    TestUtils.waitForOnlineBroker(client, broker2)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, Set(partition1, partition2).asJava)
    electResult.partitions.get.get(partition1)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition1"))
    electResult.partitions.get.get(partition2)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition2"))
    TestUtils.assertLeader(client, partition1, broker2)
    TestUtils.assertLeader(client, partition2, broker2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testElectUncleanLeadersForAllPartitions(quorum: String): Unit = {
    // Case: noop unclean leader election and valid unclean leader election for all partitions
    client = Admin.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val broker3 = 0
    val assignment1 = Seq(broker1, broker2)
    val assignment2 = Seq(broker1, broker3)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)
    val partition2 = new TopicPartition(topic, 1)

    createTopicWithAssignment(
      topic,
      Map(partition1.partition -> assignment1, partition2.partition -> assignment2)
    )

    TestUtils.assertLeader(client, partition1, broker1)
    TestUtils.assertLeader(client, partition2, broker1)

    killBroker(broker2)
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1), Set(broker2))
    killBroker(broker1)
    TestUtils.assertNoLeader(client, partition1)
    TestUtils.assertLeader(client, partition2, broker3)
    brokers(broker2).startup()
    TestUtils.waitForOnlineBroker(client, broker2)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, null)
    electResult.partitions.get.get(partition1)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition1"))
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition1, broker2)
    TestUtils.assertLeader(client, partition2, broker3)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testElectUncleanLeadersForUnknownPartitions(quorum: String): Unit = {
    // Case: unclean leader election for unknown topic
    client = Admin.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)

    val topic = "unclean-test-topic-1"
    val unknownPartition = new TopicPartition(topic, 1)
    val unknownTopic = new TopicPartition("unknown-topic", 0)

    createTopicWithAssignment(
      topic,
      Map(0 -> assignment1)
    )

    TestUtils.assertLeader(client, new TopicPartition(topic, 0), broker1)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, Set(unknownPartition, unknownTopic).asJava)
    assertTrue(electResult.partitions.get.get(unknownPartition).get.isInstanceOf[UnknownTopicOrPartitionException])
    assertTrue(electResult.partitions.get.get(unknownTopic).get.isInstanceOf[UnknownTopicOrPartitionException])
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testElectUncleanLeadersWhenNoLiveBrokers(quorum: String): Unit = {
    // Case: unclean leader election with no live brokers
    client = Admin.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)

    createTopicWithAssignment(
      topic,
      Map(partition1.partition -> assignment1)
    )

    TestUtils.assertLeader(client, partition1, broker1)

    killBroker(broker2)
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1), Set(broker2))
    killBroker(broker1)
    TestUtils.assertNoLeader(client, partition1)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, Set(partition1).asJava)
    assertTrue(electResult.partitions.get.get(partition1).get.isInstanceOf[EligibleLeadersNotAvailableException])
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testElectUncleanLeadersNoop(quorum: String): Unit = {
    // Case: noop unclean leader election with explicit topic partitions
    client = Admin.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)

    createTopicWithAssignment(
      topic,
      Map(partition1.partition -> assignment1)
    )

    TestUtils.assertLeader(client, partition1, broker1)

    killBroker(broker1)
    TestUtils.assertLeader(client, partition1, broker2)
    brokers(broker1).startup()

    val electResult = client.electLeaders(ElectionType.UNCLEAN, Set(partition1).asJava)
    assertTrue(electResult.partitions.get.get(partition1).get.isInstanceOf[ElectionNotNeededException])
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testElectUncleanLeadersAndNoop(quorum: String): Unit = {
    // Case: one noop unclean leader election and one valid unclean leader election
    client = Admin.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val broker3 = 0
    val assignment1 = Seq(broker1, broker2)
    val assignment2 = Seq(broker1, broker3)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)
    val partition2 = new TopicPartition(topic, 1)

    createTopicWithAssignment(
      topic,
      Map(partition1.partition -> assignment1, partition2.partition -> assignment2)
    )

    TestUtils.assertLeader(client, partition1, broker1)
    TestUtils.assertLeader(client, partition2, broker1)

    killBroker(broker2)
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1), Set(broker2))
    killBroker(broker1)
    TestUtils.assertNoLeader(client, partition1)
    TestUtils.assertLeader(client, partition2, broker3)
    brokers(broker2).startup()
    TestUtils.waitForOnlineBroker(client, broker2)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, Set(partition1, partition2).asJava)
    electResult.partitions.get.get(partition1)
      .ifPresent(t => fail(s"Unexpected exception during leader election: $t for partition $partition1"))
    assertTrue(electResult.partitions.get.get(partition2).get.isInstanceOf[ElectionNotNeededException])
    TestUtils.assertLeader(client, partition1, broker2)
    TestUtils.assertLeader(client, partition2, broker3)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testListReassignmentsDoesNotShowNonReassigningPartitions(quorum: String): Unit = {
    client = Admin.create(createConfig)

    // Create topics
    val topic = "list-reassignments-no-reassignments"
    createTopic(topic, replicationFactor = 3)
    val tp = new TopicPartition(topic, 0)

    val reassignmentsMap = client.listPartitionReassignments(Set(tp).asJava).reassignments().get()
    assertEquals(0, reassignmentsMap.size())

    val allReassignmentsMap = client.listPartitionReassignments().reassignments().get()
    assertEquals(0, allReassignmentsMap.size())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testListReassignmentsDoesNotShowDeletedPartitions(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val topic = "list-reassignments-no-reassignments"
    val tp = new TopicPartition(topic, 0)

    val reassignmentsMap = client.listPartitionReassignments(Set(tp).asJava).reassignments().get()
    assertEquals(0, reassignmentsMap.size())

    val allReassignmentsMap = client.listPartitionReassignments().reassignments().get()
    assertEquals(0, allReassignmentsMap.size())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testValidIncrementalAlterConfigs(quorum: String): Unit = {
    client = Admin.create(createConfig)

    // Create topics
    val topic1 = "incremental-alter-configs-topic-1"
    val topic1Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    val topic1CreateConfigs = new Properties
    topic1CreateConfigs.setProperty(TopicConfig.RETENTION_MS_CONFIG, "60000000")
    topic1CreateConfigs.setProperty(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
    createTopic(topic1, numPartitions = 1, replicationFactor = 1, topic1CreateConfigs)

    val topic2 = "incremental-alter-configs-topic-2"
    val topic2Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopic(topic2)

    // Alter topic configs
    var topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.FLUSH_MS_CONFIG, "1000"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE), AlterConfigOp.OpType.APPEND),
      new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, ""), AlterConfigOp.OpType.DELETE)
    ).asJavaCollection

    // Test SET and APPEND on previously unset properties
    var topic2AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.9"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), AlterConfigOp.OpType.APPEND)
    ).asJavaCollection

    var alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs,
      topic2Resource -> topic2AlterConfigs
    ).asJava)

    assertEquals(Set(topic1Resource, topic2Resource).asJava, alterResult.values.keySet)
    alterResult.all.get

    ensureConsistentKRaftMetadata()

    // Verify that topics were updated correctly
    var describeResult = client.describeConfigs(Seq(topic1Resource, topic2Resource).asJava)
    var configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals("1000", configs.get(topic1Resource).get(TopicConfig.FLUSH_MS_CONFIG).value)
    assertEquals("compact,delete", configs.get(topic1Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value)
    assertEquals(LogConfig.DEFAULT_RETENTION_MS.toString, configs.get(topic1Resource).get(TopicConfig.RETENTION_MS_CONFIG).value)

    assertEquals("0.9", configs.get(topic2Resource).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
    assertEquals("lz4", configs.get(topic2Resource).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)
    assertEquals("delete,compact", configs.get(topic2Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value)

    // verify subtract operation, including from an empty property
    topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), AlterConfigOp.OpType.SUBTRACT),
      new AlterConfigOp(new ConfigEntry(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "0"), AlterConfigOp.OpType.SUBTRACT)
    ).asJava

    // subtract all from this list property
    topic2AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE), AlterConfigOp.OpType.SUBTRACT)
    ).asJavaCollection

    alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs,
      topic2Resource -> topic2AlterConfigs
    ).asJava)
    assertEquals(Set(topic1Resource, topic2Resource).asJava, alterResult.values.keySet)
    alterResult.all.get

    ensureConsistentKRaftMetadata()

    // Verify that topics were updated correctly
    describeResult = client.describeConfigs(Seq(topic1Resource, topic2Resource).asJava)
    configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals("delete", configs.get(topic1Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value)
    assertEquals("1000", configs.get(topic1Resource).get(TopicConfig.FLUSH_MS_CONFIG).value) // verify previous change is still intact
    assertEquals("", configs.get(topic1Resource).get(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG).value)
    assertEquals("", configs.get(topic2Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value )

    // Alter topics with validateOnly=true
    topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), AlterConfigOp.OpType.APPEND)
    ).asJava

    alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs
    ).asJava, new AlterConfigsOptions().validateOnly(true))
    alterResult.all.get

    // Verify that topics were not updated due to validateOnly = true
    describeResult = client.describeConfigs(Seq(topic1Resource).asJava)
    configs = describeResult.all.get

    assertEquals("delete", configs.get(topic1Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value)

    // Alter topics with validateOnly=true with invalid configs
    topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "zip"), AlterConfigOp.OpType.SET)
    ).asJava

    alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs
    ).asJava, new AlterConfigsOptions().validateOnly(true))

    if (isKRaftTest()) {
      assertFutureExceptionTypeEquals(alterResult.values().get(topic1Resource), classOf[InvalidConfigurationException],
        Some("Invalid value zip for configuration compression.type"))
    } else {
      assertFutureExceptionTypeEquals(alterResult.values().get(topic1Resource), classOf[InvalidConfigurationException],
        Some("Invalid config value for resource"))
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testAppendAlreadyExistsConfigsAndSubtractNotExistsConfigs(quorum: String): Unit = {
    client = Admin.create(createConfig)

    // Create topics
    val topic = "incremental-alter-configs-topic"
    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)

    val appendValues = s"0:${brokers.head.config.brokerId}"
    val subtractValues = brokers.tail.map(broker => s"0:${broker.config.brokerId}").mkString(",")
    assertNotEquals("", subtractValues)

    val topicCreateConfigs = new Properties
    topicCreateConfigs.setProperty(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, appendValues)
    createTopic(topic, numPartitions = 1, replicationFactor = 1, topicCreateConfigs)

    // Append value that is already present
    val topicAppendConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, appendValues), AlterConfigOp.OpType.APPEND),
    ).asJavaCollection

    val appendResult = client.incrementalAlterConfigs(Map(topicResource -> topicAppendConfigs).asJava)
    appendResult.all.get

    // Subtract values that are not present
    val topicSubtractConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, subtractValues), AlterConfigOp.OpType.SUBTRACT)
    ).asJavaCollection
    val subtractResult = client.incrementalAlterConfigs(Map(topicResource -> topicSubtractConfigs).asJava)
    subtractResult.all.get

    ensureConsistentKRaftMetadata()

    // Verify that topics were updated correctly
    val describeResult = client.describeConfigs(Seq(topicResource).asJava)
    val configs = describeResult.all.get

    assertEquals(appendValues, configs.get(topicResource).get(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG).value)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testIncrementalAlterConfigsDeleteAndSetBrokerConfigs(quorum: String): Unit = {
    client = Admin.create(createConfig)
    val broker0Resource = new ConfigResource(ConfigResource.Type.BROKER, "0")
    client.incrementalAlterConfigs(Map(broker0Resource ->
      Seq(new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, "123"),
          AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.FollowerReplicationThrottledRateProp, "456"),
          AlterConfigOp.OpType.SET)
      ).asJavaCollection).asJava).all().get()
    TestUtils.waitUntilTrue(() => {
      val broker0Configs = client.describeConfigs(Seq(broker0Resource).asJava).
        all().get().get(broker0Resource).entries().asScala.map(entry => (entry.name, entry.value)).toMap
      ("123".equals(broker0Configs.getOrElse(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, "")) &&
        "456".equals(broker0Configs.getOrElse(DynamicConfig.Broker.FollowerReplicationThrottledRateProp, "")))
    }, "Expected to see the broker properties we just set", pause=25)
    client.incrementalAlterConfigs(Map(broker0Resource ->
      Seq(new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, ""),
        AlterConfigOp.OpType.DELETE),
        new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.FollowerReplicationThrottledRateProp, "654"),
          AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.ReplicaAlterLogDirsIoMaxBytesPerSecondProp, "987"),
          AlterConfigOp.OpType.SET)
      ).asJavaCollection).asJava).all().get()
    TestUtils.waitUntilTrue(() => {
      val broker0Configs = client.describeConfigs(Seq(broker0Resource).asJava).
        all().get().get(broker0Resource).entries().asScala.map(entry => (entry.name, entry.value)).toMap
      ("".equals(broker0Configs.getOrElse(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, "")) &&
        "654".equals(broker0Configs.getOrElse(DynamicConfig.Broker.FollowerReplicationThrottledRateProp, "")) &&
        "987".equals(broker0Configs.getOrElse(DynamicConfig.Broker.ReplicaAlterLogDirsIoMaxBytesPerSecondProp, "")))
    }, "Expected to see the broker properties we just modified", pause=25)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testIncrementalAlterConfigsDeleteBrokerConfigs(quorum: String): Unit = {
    client = Admin.create(createConfig)
    val broker0Resource = new ConfigResource(ConfigResource.Type.BROKER, "0")
    client.incrementalAlterConfigs(Map(broker0Resource ->
      Seq(new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, "123"),
        AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.FollowerReplicationThrottledRateProp, "456"),
          AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.ReplicaAlterLogDirsIoMaxBytesPerSecondProp, "789"),
          AlterConfigOp.OpType.SET)
      ).asJavaCollection).asJava).all().get()
    TestUtils.waitUntilTrue(() => {
      val broker0Configs = client.describeConfigs(Seq(broker0Resource).asJava).
        all().get().get(broker0Resource).entries().asScala.map(entry => (entry.name, entry.value)).toMap
      ("123".equals(broker0Configs.getOrElse(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, "")) &&
        "456".equals(broker0Configs.getOrElse(DynamicConfig.Broker.FollowerReplicationThrottledRateProp, "")) &&
        "789".equals(broker0Configs.getOrElse(DynamicConfig.Broker.ReplicaAlterLogDirsIoMaxBytesPerSecondProp, "")))
    }, "Expected to see the broker properties we just set", pause=25)
    client.incrementalAlterConfigs(Map(broker0Resource ->
      Seq(new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, ""),
        AlterConfigOp.OpType.DELETE),
        new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.FollowerReplicationThrottledRateProp, ""),
          AlterConfigOp.OpType.DELETE),
        new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.ReplicaAlterLogDirsIoMaxBytesPerSecondProp, ""),
          AlterConfigOp.OpType.DELETE)
      ).asJavaCollection).asJava).all().get()
    TestUtils.waitUntilTrue(() => {
      val broker0Configs = client.describeConfigs(Seq(broker0Resource).asJava).
        all().get().get(broker0Resource).entries().asScala.map(entry => (entry.name, entry.value)).toMap
      ("".equals(broker0Configs.getOrElse(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, "")) &&
        "".equals(broker0Configs.getOrElse(DynamicConfig.Broker.FollowerReplicationThrottledRateProp, "")) &&
        "".equals(broker0Configs.getOrElse(DynamicConfig.Broker.ReplicaAlterLogDirsIoMaxBytesPerSecondProp, "")))
    }, "Expected to see the broker properties we just removed to be deleted", pause=25)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testInvalidIncrementalAlterConfigs(quorum: String): Unit = {
    client = Admin.create(createConfig)

    // Create topics
    val topic1 = "incremental-alter-configs-topic-1"
    val topic1Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    createTopic(topic1)

    val topic2 = "incremental-alter-configs-topic-2"
    val topic2Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopic(topic2)

    // Add duplicate Keys for topic1
    var topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.75"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.65"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"), AlterConfigOp.OpType.SET) // valid entry
    ).asJavaCollection

    // Add valid config for topic2
    var topic2AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.9"), AlterConfigOp.OpType.SET)
    ).asJavaCollection

    var alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs,
      topic2Resource -> topic2AlterConfigs
    ).asJava)
    assertEquals(Set(topic1Resource, topic2Resource).asJava, alterResult.values.keySet)

    // InvalidRequestException error for topic1
    assertFutureExceptionTypeEquals(alterResult.values().get(topic1Resource), classOf[InvalidRequestException],
      Some("Error due to duplicate config keys"))

    // Operation should succeed for topic2
    alterResult.values().get(topic2Resource).get()
    ensureConsistentKRaftMetadata()

    // Verify that topic1 is not config not updated, and topic2 config is updated
    val describeResult = client.describeConfigs(Seq(topic1Resource, topic2Resource).asJava)
    val configs = describeResult.all.get
    assertEquals(2, configs.size)

    assertEquals(LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO.toString, configs.get(topic1Resource).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
    assertEquals(LogConfig.DEFAULT_COMPRESSION_TYPE, configs.get(topic1Resource).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)
    assertEquals("0.9", configs.get(topic2Resource).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)

    // Check invalid use of append/subtract operation types
    topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"), AlterConfigOp.OpType.APPEND)
    ).asJavaCollection

    topic2AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"), AlterConfigOp.OpType.SUBTRACT)
    ).asJavaCollection

    alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs,
      topic2Resource -> topic2AlterConfigs
    ).asJava)
    assertEquals(Set(topic1Resource, topic2Resource).asJava, alterResult.values.keySet)

    assertFutureExceptionTypeEquals(alterResult.values().get(topic1Resource), classOf[InvalidConfigurationException],
      if (isKRaftTest()) {
        Some("Can't APPEND to key compression.type because its type is not LIST.")
      } else {
        Some("Config value append is not allowed for config")
      })

    assertFutureExceptionTypeEquals(alterResult.values().get(topic2Resource), classOf[InvalidConfigurationException],
      if (isKRaftTest()) {
        Some("Can't SUBTRACT to key compression.type because its type is not LIST.")
      } else {
        Some("Config value subtract is not allowed for config")
      })

    // Try to add invalid config
    topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "1.1"), AlterConfigOp.OpType.SET)
    ).asJavaCollection

    alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs
    ).asJava)
    assertEquals(Set(topic1Resource).asJava, alterResult.values.keySet)

    assertFutureExceptionTypeEquals(alterResult.values().get(topic1Resource), classOf[InvalidConfigurationException],
      if (isKRaftTest()) {
        Some("Invalid value 1.1 for configuration min.cleanable.dirty.ratio: Value must be no more than 1")
      } else {
        Some("Invalid config value for resource")
      })
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testInvalidAlterPartitionReassignments(quorum: String): Unit = {
    client = Admin.create(createConfig)
    val topic = "alter-reassignments-topic-1"
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)
    val tp3 = new TopicPartition(topic, 2)
    createTopic(topic, numPartitions = 4)


    val validAssignment = Optional.of(new NewPartitionReassignment(
      (0 until brokerCount).map(_.asInstanceOf[Integer]).asJava
    ))

    val nonExistentTp1 = new TopicPartition("topicA", 0)
    val nonExistentTp2 = new TopicPartition(topic, 4)
    val nonExistentPartitionsResult = client.alterPartitionReassignments(Map(
      tp1 -> validAssignment,
      tp2 -> validAssignment,
      tp3 -> validAssignment,
      nonExistentTp1 -> validAssignment,
      nonExistentTp2 -> validAssignment
    ).asJava).values()
    assertFutureExceptionTypeEquals(nonExistentPartitionsResult.get(nonExistentTp1), classOf[UnknownTopicOrPartitionException])
    assertFutureExceptionTypeEquals(nonExistentPartitionsResult.get(nonExistentTp2), classOf[UnknownTopicOrPartitionException])

    val extraNonExistentReplica = Optional.of(new NewPartitionReassignment((0 until brokerCount + 1).map(_.asInstanceOf[Integer]).asJava))
    val negativeIdReplica = Optional.of(new NewPartitionReassignment(Seq(-3, -2, -1).map(_.asInstanceOf[Integer]).asJava))
    val duplicateReplica = Optional.of(new NewPartitionReassignment(Seq(0, 1, 1).map(_.asInstanceOf[Integer]).asJava))
    val invalidReplicaResult = client.alterPartitionReassignments(Map(
      tp1 -> extraNonExistentReplica,
      tp2 -> negativeIdReplica,
      tp3 -> duplicateReplica
    ).asJava).values()
    assertFutureExceptionTypeEquals(invalidReplicaResult.get(tp1), classOf[InvalidReplicaAssignmentException])
    assertFutureExceptionTypeEquals(invalidReplicaResult.get(tp2), classOf[InvalidReplicaAssignmentException])
    assertFutureExceptionTypeEquals(invalidReplicaResult.get(tp3), classOf[InvalidReplicaAssignmentException])
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testLongTopicNames(quorum: String): Unit = {
    val client = Admin.create(createConfig)
    val longTopicName = String.join("", Collections.nCopies(249, "x"));
    val invalidTopicName = String.join("", Collections.nCopies(250, "x"));
    val newTopics2 = Seq(new NewTopic(invalidTopicName, 3, 3.toShort),
                         new NewTopic(longTopicName, 3, 3.toShort))
    val results = client.createTopics(newTopics2.asJava).values()
    assertTrue(results.containsKey(longTopicName))
    results.get(longTopicName).get()
    assertTrue(results.containsKey(invalidTopicName))
    assertFutureExceptionTypeEquals(results.get(invalidTopicName), classOf[InvalidTopicException])
    assertFutureExceptionTypeEquals(client.alterReplicaLogDirs(
      Map(new TopicPartitionReplica(longTopicName, 0, 0) -> brokers(0).config.logDirs(0)).asJava).all(),
      classOf[InvalidTopicException])
    client.close()
  }

  // Verify that createTopics and alterConfigs fail with null values
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testNullConfigs(quorum: String): Unit = {

    def validateLogConfig(compressionType: String): Unit = {
      val logConfig = if (isKRaftTest()) {
        ensureConsistentKRaftMetadata()
        val topicProps = brokers.head.metadataCache.asInstanceOf[KRaftMetadataCache].topicConfig(topic)
        LogConfig.fromProps(Collections.emptyMap[String, AnyRef], topicProps)
      } else {
        zkClient.getLogConfigs(Set(topic), Collections.emptyMap[String, AnyRef])._1(topic)
      }

      assertEquals(compressionType, logConfig.originals.get(TopicConfig.COMPRESSION_TYPE_CONFIG))
      assertNull(logConfig.originals.get(TopicConfig.RETENTION_BYTES_CONFIG))
      assertEquals(LogConfig.DEFAULT_RETENTION_BYTES, logConfig.retentionSize)
    }

    client = Admin.create(createConfig)
    val invalidConfigs = Map[String, String](
      TopicConfig.RETENTION_BYTES_CONFIG -> null,
      TopicConfig.COMPRESSION_TYPE_CONFIG -> "producer"
    ).asJava
    val newTopic = new NewTopic(topic, 2, brokerCount.toShort)
    assertFutureExceptionTypeEquals(
      client.createTopics(Collections.singletonList(newTopic.configs(invalidConfigs))).all,
      classOf[InvalidConfigurationException],
      Some("Null value not supported for topic configs: retention.bytes")
    )

    val validConfigs = Map[String, String](TopicConfig.COMPRESSION_TYPE_CONFIG -> "producer").asJava
    client.createTopics(Collections.singletonList(newTopic.configs(validConfigs))).all.get()
    waitForTopics(client, expectedPresent = Seq(topic), expectedMissing = List())
    validateLogConfig(compressionType = "producer")

    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    val alterOps = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_BYTES_CONFIG, null), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"), AlterConfigOp.OpType.SET)
    )
    assertFutureExceptionTypeEquals(
      client.incrementalAlterConfigs(Map(topicResource -> alterOps.asJavaCollection).asJava).all,
      classOf[InvalidRequestException],
      Some("Null value not supported for : retention.bytes")
    )
    validateLogConfig(compressionType = "producer")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDescribeConfigsForLog4jLogLevels(quorum: String): Unit = {
    client = Admin.create(createConfig)
    LoggerFactory.getLogger("kafka.cluster.Replica").trace("Message to create the logger")
    val loggerConfig = describeBrokerLoggers()
    val kafkaLogLevel = loggerConfig.get("kafka").value()
    val logCleanerLogLevelConfig = loggerConfig.get("kafka.cluster.Replica")
    // we expect the log level to be inherited from the first ancestor with a level configured
    assertEquals(kafkaLogLevel, logCleanerLogLevelConfig.value())
    assertEquals("kafka.cluster.Replica", logCleanerLogLevelConfig.name())
    assertEquals(ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG, logCleanerLogLevelConfig.source())
    assertEquals(false, logCleanerLogLevelConfig.isReadOnly)
    assertEquals(false, logCleanerLogLevelConfig.isSensitive)
    assertTrue(logCleanerLogLevelConfig.synonyms().isEmpty)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  @Disabled // To be re-enabled once KAFKA-8779 is resolved
  def testIncrementalAlterConfigsForLog4jLogLevels(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val initialLoggerConfig = describeBrokerLoggers()
    val initialRootLogLevel = initialLoggerConfig.get(Log4jController.ROOT_LOGGER).value()
    assertEquals(initialRootLogLevel, initialLoggerConfig.get("kafka.controller.KafkaController").value())
    assertEquals(initialRootLogLevel, initialLoggerConfig.get("kafka.log.LogCleaner").value())
    assertEquals(initialRootLogLevel, initialLoggerConfig.get("kafka.server.ReplicaManager").value())

    val newRootLogLevel = LogLevelConfig.DEBUG_LOG_LEVEL
    val alterRootLoggerEntry = Seq(
      new AlterConfigOp(new ConfigEntry(Log4jController.ROOT_LOGGER, newRootLogLevel), AlterConfigOp.OpType.SET)
    ).asJavaCollection
    // Test validateOnly does not change anything
    alterBrokerLoggers(alterRootLoggerEntry, validateOnly = true)
    val validatedLoggerConfig = describeBrokerLoggers()
    assertEquals(initialRootLogLevel, validatedLoggerConfig.get(Log4jController.ROOT_LOGGER).value())
    assertEquals(initialRootLogLevel, validatedLoggerConfig.get("kafka.controller.KafkaController").value())
    assertEquals(initialRootLogLevel, validatedLoggerConfig.get("kafka.log.LogCleaner").value())
    assertEquals(initialRootLogLevel, validatedLoggerConfig.get("kafka.server.ReplicaManager").value())
    assertEquals(initialRootLogLevel, validatedLoggerConfig.get("kafka.zookeeper.ZooKeeperClient").value())

    // test that we can change them and unset loggers still use the root's log level
    alterBrokerLoggers(alterRootLoggerEntry)
    val changedRootLoggerConfig = describeBrokerLoggers()
    assertEquals(newRootLogLevel, changedRootLoggerConfig.get(Log4jController.ROOT_LOGGER).value())
    assertEquals(newRootLogLevel, changedRootLoggerConfig.get("kafka.controller.KafkaController").value())
    assertEquals(newRootLogLevel, changedRootLoggerConfig.get("kafka.log.LogCleaner").value())
    assertEquals(newRootLogLevel, changedRootLoggerConfig.get("kafka.server.ReplicaManager").value())
    assertEquals(newRootLogLevel, changedRootLoggerConfig.get("kafka.zookeeper.ZooKeeperClient").value())

    // alter the ZK client's logger so we can later test resetting it
    val alterZKLoggerEntry = Seq(
      new AlterConfigOp(new ConfigEntry("kafka.zookeeper.ZooKeeperClient", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SET)
    ).asJavaCollection
    alterBrokerLoggers(alterZKLoggerEntry)
    val changedZKLoggerConfig = describeBrokerLoggers()
    assertEquals(LogLevelConfig.ERROR_LOG_LEVEL, changedZKLoggerConfig.get("kafka.zookeeper.ZooKeeperClient").value())

    // properly test various set operations and one delete
    val alterLogLevelsEntries = Seq(
      new AlterConfigOp(new ConfigEntry("kafka.controller.KafkaController", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry("kafka.log.LogCleaner", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry("kafka.server.ReplicaManager", LogLevelConfig.TRACE_LOG_LEVEL), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry("kafka.zookeeper.ZooKeeperClient", ""), AlterConfigOp.OpType.DELETE) // should reset to the root logger level
    ).asJavaCollection
    alterBrokerLoggers(alterLogLevelsEntries)
    val alteredLoggerConfig = describeBrokerLoggers()
    assertEquals(newRootLogLevel, alteredLoggerConfig.get(Log4jController.ROOT_LOGGER).value())
    assertEquals(LogLevelConfig.INFO_LOG_LEVEL, alteredLoggerConfig.get("kafka.controller.KafkaController").value())
    assertEquals(LogLevelConfig.ERROR_LOG_LEVEL, alteredLoggerConfig.get("kafka.log.LogCleaner").value())
    assertEquals(LogLevelConfig.TRACE_LOG_LEVEL, alteredLoggerConfig.get("kafka.server.ReplicaManager").value())
    assertEquals(newRootLogLevel, alteredLoggerConfig.get("kafka.zookeeper.ZooKeeperClient").value())
  }

  /**
    * 1. Assume ROOT logger == TRACE
    * 2. Change kafka.controller.KafkaController logger to INFO
    * 3. Unset kafka.controller.KafkaController via AlterConfigOp.OpType.DELETE (resets it to the root logger - TRACE)
    * 4. Change ROOT logger to ERROR
    * 5. Ensure the kafka.controller.KafkaController logger's level is ERROR (the current root logger level)
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  @Disabled // To be re-enabled once KAFKA-8779 is resolved
  def testIncrementalAlterConfigsForLog4jLogLevelsCanResetLoggerToCurrentRoot(quorum: String): Unit = {
    client = Admin.create(createConfig)
    // step 1 - configure root logger
    val initialRootLogLevel = LogLevelConfig.TRACE_LOG_LEVEL
    val alterRootLoggerEntry = Seq(
      new AlterConfigOp(new ConfigEntry(Log4jController.ROOT_LOGGER, initialRootLogLevel), AlterConfigOp.OpType.SET)
    ).asJavaCollection
    alterBrokerLoggers(alterRootLoggerEntry)
    val initialLoggerConfig = describeBrokerLoggers()
    assertEquals(initialRootLogLevel, initialLoggerConfig.get(Log4jController.ROOT_LOGGER).value())
    assertEquals(initialRootLogLevel, initialLoggerConfig.get("kafka.controller.KafkaController").value())

    // step 2 - change KafkaController logger to INFO
    val alterControllerLoggerEntry = Seq(
      new AlterConfigOp(new ConfigEntry("kafka.controller.KafkaController", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET)
    ).asJavaCollection
    alterBrokerLoggers(alterControllerLoggerEntry)
    val changedControllerLoggerConfig = describeBrokerLoggers()
    assertEquals(initialRootLogLevel, changedControllerLoggerConfig.get(Log4jController.ROOT_LOGGER).value())
    assertEquals(LogLevelConfig.INFO_LOG_LEVEL, changedControllerLoggerConfig.get("kafka.controller.KafkaController").value())

    // step 3 - unset KafkaController logger
    val deleteControllerLoggerEntry = Seq(
      new AlterConfigOp(new ConfigEntry("kafka.controller.KafkaController", ""), AlterConfigOp.OpType.DELETE)
    ).asJavaCollection
    alterBrokerLoggers(deleteControllerLoggerEntry)
    val deletedControllerLoggerConfig = describeBrokerLoggers()
    assertEquals(initialRootLogLevel, deletedControllerLoggerConfig.get(Log4jController.ROOT_LOGGER).value())
    assertEquals(initialRootLogLevel, deletedControllerLoggerConfig.get("kafka.controller.KafkaController").value())

    val newRootLogLevel = LogLevelConfig.ERROR_LOG_LEVEL
    val newAlterRootLoggerEntry = Seq(
      new AlterConfigOp(new ConfigEntry(Log4jController.ROOT_LOGGER, newRootLogLevel), AlterConfigOp.OpType.SET)
    ).asJavaCollection
    alterBrokerLoggers(newAlterRootLoggerEntry)
    val newRootLoggerConfig = describeBrokerLoggers()
    assertEquals(newRootLogLevel, newRootLoggerConfig.get(Log4jController.ROOT_LOGGER).value())
    assertEquals(newRootLogLevel, newRootLoggerConfig.get("kafka.controller.KafkaController").value())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  @Disabled // Zk to be re-enabled once KAFKA-8779 is resolved
  def testIncrementalAlterConfigsForLog4jLogLevelsCannotResetRootLogger(quorum: String): Unit = {
    client = Admin.create(createConfig)
    val deleteRootLoggerEntry = Seq(
      new AlterConfigOp(new ConfigEntry(Log4jController.ROOT_LOGGER, ""), AlterConfigOp.OpType.DELETE)
    ).asJavaCollection

    assertTrue(assertThrows(classOf[ExecutionException], () => alterBrokerLoggers(deleteRootLoggerEntry)).getCause.isInstanceOf[InvalidRequestException])
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  @Disabled // To be re-enabled once KAFKA-8779 is resolved
  def testIncrementalAlterConfigsForLog4jLogLevelsDoesNotWorkWithInvalidConfigs(quorum: String): Unit = {
    client = Admin.create(createConfig)
    val validLoggerName = "kafka.server.KafkaRequestHandler"
    val expectedValidLoggerLogLevel = describeBrokerLoggers().get(validLoggerName)
    def assertLogLevelDidNotChange(): Unit = {
      assertEquals(expectedValidLoggerLogLevel, describeBrokerLoggers().get(validLoggerName))
    }

    val appendLogLevelEntries = Seq(
      new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
      new AlterConfigOp(new ConfigEntry("kafka.network.SocketServer", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.APPEND) // append is not supported
    ).asJavaCollection
    assertTrue(assertThrows(classOf[ExecutionException],
      () => alterBrokerLoggers(appendLogLevelEntries)).getCause.isInstanceOf[InvalidRequestException])
    assertLogLevelDidNotChange()

    val subtractLogLevelEntries = Seq(
      new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
      new AlterConfigOp(new ConfigEntry("kafka.network.SocketServer", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SUBTRACT) // subtract is not supported
    ).asJavaCollection
    assertTrue(assertThrows(classOf[ExecutionException], () => alterBrokerLoggers(subtractLogLevelEntries)).getCause.isInstanceOf[InvalidRequestException])
    assertLogLevelDidNotChange()

    val invalidLogLevelLogLevelEntries = Seq(
      new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
      new AlterConfigOp(new ConfigEntry("kafka.network.SocketServer", "OFF"), AlterConfigOp.OpType.SET) // OFF is not a valid log level
    ).asJavaCollection
    assertTrue(assertThrows(classOf[ExecutionException], () => alterBrokerLoggers(invalidLogLevelLogLevelEntries)).getCause.isInstanceOf[InvalidRequestException])
    assertLogLevelDidNotChange()

    val invalidLoggerNameLogLevelEntries = Seq(
      new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
      new AlterConfigOp(new ConfigEntry("Some Other LogCleaner", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SET) // invalid logger name is not supported
    ).asJavaCollection
    assertTrue(assertThrows(classOf[ExecutionException], () => alterBrokerLoggers(invalidLoggerNameLogLevelEntries)).getCause.isInstanceOf[InvalidRequestException])
    assertLogLevelDidNotChange()
  }

  /**
    * The AlterConfigs API is deprecated and should not support altering log levels
    */
  @nowarn("cat=deprecation")
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft")) // Zk to be re-enabled once KAFKA-8779 is resolved
  def testAlterConfigsForLog4jLogLevelsDoesNotWork(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val alterLogLevelsEntries = Seq(
      new ConfigEntry("kafka.controller.KafkaController", LogLevelConfig.INFO_LOG_LEVEL)
    ).asJavaCollection
    val alterResult = client.alterConfigs(Map(brokerLoggerConfigResource -> new Config(alterLogLevelsEntries)).asJava)
    assertTrue(assertThrows(classOf[ExecutionException], () => alterResult.values.get(brokerLoggerConfigResource).get).getCause.isInstanceOf[InvalidRequestException])
  }

  def alterBrokerLoggers(entries: util.Collection[AlterConfigOp], validateOnly: Boolean = false): Unit = {
    if (!validateOnly) {
      for (entry <- entries.asScala)
        changedBrokerLoggers.add(entry.configEntry().name())
    }

    client.incrementalAlterConfigs(Map(brokerLoggerConfigResource -> entries).asJava, new AlterConfigsOptions().validateOnly(validateOnly))
      .values.get(brokerLoggerConfigResource).get()
  }

  def describeBrokerLoggers(): Config =
    client.describeConfigs(Collections.singletonList(brokerLoggerConfigResource)).values.get(brokerLoggerConfigResource).get()

  /**
   * Due to the fact that log4j is not re-initialized across tests, changing a logger's log level persists across test classes.
   * We need to clean up the changes done while testing.
   */
  private def teardownBrokerLoggers(): Unit = {
    if (changedBrokerLoggers.nonEmpty) {
      val validLoggers = describeBrokerLoggers().entries().asScala.filterNot(_.name.equals(Log4jController.ROOT_LOGGER)).map(_.name).toSet
      val unsetBrokerLoggersEntries = changedBrokerLoggers
        .intersect(validLoggers)
        .map { logger => new AlterConfigOp(new ConfigEntry(logger, ""), AlterConfigOp.OpType.DELETE) }
        .asJavaCollection

      // ensure that we first reset the root logger to an arbitrary log level. Note that we cannot reset it to its original value
      alterBrokerLoggers(List(
        new AlterConfigOp(new ConfigEntry(Log4jController.ROOT_LOGGER, LogLevelConfig.FATAL_LOG_LEVEL), AlterConfigOp.OpType.SET)
      ).asJavaCollection)
      alterBrokerLoggers(unsetBrokerLoggersEntries)

      changedBrokerLoggers.clear()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testAppendConfigToEmptyDefaultValue(ignored: String): Unit = {
    testAppendConfig(new Properties(), "0:0", "0:0")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testAppendConfigToExistentValue(ignored: String): Unit = {
    val props = new Properties();
    props.setProperty(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:1")
    testAppendConfig(props, "0:0", "1:1,0:0")
  }

  private def testAppendConfig(props: Properties, append: String, expected: String): Unit = {
    client = Admin.create(createConfig)
    createTopic(topic, topicConfig = props)
    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    val topicAlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, append), AlterConfigOp.OpType.APPEND),
    ).asJavaCollection

    val alterResult = client.incrementalAlterConfigs(Map(
      topicResource -> topicAlterConfigs
    ).asJava)
    alterResult.all().get(15, TimeUnit.SECONDS)

    ensureConsistentKRaftMetadata()
    val config = client.describeConfigs(List(topicResource).asJava).all().get().get(topicResource).get(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
    assertEquals(expected, config.value())
  }

  /**
   * Test that createTopics returns the dynamic configurations of the topics that were created.
   *
   * Note: this test requires some custom static broker and controller configurations, which are set up in
   * BaseAdminIntegrationTest.modifyConfigs.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testCreateTopicsReturnsConfigs(quorum: String): Unit = {
    client = Admin.create(super.createConfig)

    val newLogRetentionProperties = new Properties
    newLogRetentionProperties.put(KafkaConfig.LogRetentionTimeMillisProp, "10800000")
    TestUtils.incrementalAlterConfigs(null, client, newLogRetentionProperties, perBrokerConfig = false)
      .all().get(15, TimeUnit.SECONDS)

    val newLogCleanerDeleteRetention = new Properties
    newLogCleanerDeleteRetention.put(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP, "34")
    TestUtils.incrementalAlterConfigs(brokers, client, newLogCleanerDeleteRetention, perBrokerConfig = true)
      .all().get(15, TimeUnit.SECONDS)

    if (isKRaftTest()) {
      // In KRaft mode, we don't yet support altering configs on controller nodes, except by setting
      // default node configs. Therefore, we have to set the dynamic config directly to test this.
      val controllerNodeResource = new ConfigResource(ConfigResource.Type.BROKER,
        controllerServer.config.nodeId.toString)
      controllerServer.controller.incrementalAlterConfigs(ANONYMOUS_CONTEXT,
        Collections.singletonMap(controllerNodeResource,
          Collections.singletonMap(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP,
            new SimpleImmutableEntry(AlterConfigOp.OpType.SET, "34"))), false).get()
      ensureConsistentKRaftMetadata()
    }

    waitUntilTrue(() => brokers.forall(_.config.originals.getOrDefault(
      CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP, "").toString.equals("34")),
      s"Timed out waiting for change to ${CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP}",
      waitTimeMs = 60000L)

    waitUntilTrue(() => brokers.forall(_.config.originals.getOrDefault(
      KafkaConfig.LogRetentionTimeMillisProp, "").toString.equals("10800000")),
      s"Timed out waiting for change to ${KafkaConfig.LogRetentionTimeMillisProp}",
      waitTimeMs = 60000L)

    val newTopics = Seq(new NewTopic("foo", Map((0: Integer) -> Seq[Integer](1, 2).asJava,
      (1: Integer) -> Seq[Integer](2, 0).asJava).asJava).
      configs(Collections.singletonMap(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "9999999")),
      new NewTopic("bar", 3, 3.toShort),
      new NewTopic("baz", Option.empty[Integer].asJava, Option.empty[java.lang.Short].asJava)
    )
    val result = client.createTopics(newTopics.asJava)
    result.all.get()
    waitForTopics(client, newTopics.map(_.name()).toList, List())

    assertEquals(2, result.numPartitions("foo").get())
    assertEquals(2, result.replicationFactor("foo").get())
    assertEquals(3, result.numPartitions("bar").get())
    assertEquals(3, result.replicationFactor("bar").get())
    assertEquals(configs.head.numPartitions, result.numPartitions("baz").get())
    assertEquals(configs.head.defaultReplicationFactor, result.replicationFactor("baz").get())

    val topicConfigs = result.config("foo").get()

    // From the topic configuration defaults.
    assertEquals(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "delete",
      ConfigSource.DEFAULT_CONFIG, false, false, Collections.emptyList(), null, null),
      topicConfigs.get(TopicConfig.CLEANUP_POLICY_CONFIG))

    // From dynamic cluster config via the synonym LogRetentionTimeHoursProp.
    assertEquals(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "10800000",
      ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG, false, false, Collections.emptyList(), null, null),
      topicConfigs.get(TopicConfig.RETENTION_MS_CONFIG))

    // From dynamic broker config via LogCleanerDeleteRetentionMsProp.
    assertEquals(new ConfigEntry(TopicConfig.DELETE_RETENTION_MS_CONFIG, "34",
      ConfigSource.DYNAMIC_BROKER_CONFIG, false, false, Collections.emptyList(), null, null),
      topicConfigs.get(TopicConfig.DELETE_RETENTION_MS_CONFIG))

    // From static broker config by SegmentJitterMsProp.
    assertEquals(new ConfigEntry(TopicConfig.SEGMENT_JITTER_MS_CONFIG, "123",
      ConfigSource.STATIC_BROKER_CONFIG, false, false, Collections.emptyList(), null, null),
      topicConfigs.get(TopicConfig.SEGMENT_JITTER_MS_CONFIG))

    // From static broker config by the synonym LogRollTimeHoursProp.
    val segmentMsPropType = if (isKRaftTest()) {
      ConfigSource.STATIC_BROKER_CONFIG
    } else {
      ConfigSource.DEFAULT_CONFIG
    }
    assertEquals(new ConfigEntry(TopicConfig.SEGMENT_MS_CONFIG, "7200000",
      segmentMsPropType, false, false, Collections.emptyList(), null, null),
      topicConfigs.get(TopicConfig.SEGMENT_MS_CONFIG))

    // From the dynamic topic config.
    assertEquals(new ConfigEntry(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "9999999",
      ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false, Collections.emptyList(), null, null),
      topicConfigs.get(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG))
  }
}

object PlaintextAdminIntegrationTest {

  @nowarn("cat=deprecation")
  def checkValidAlterConfigs(
    admin: Admin,
    test: KafkaServerTestHarness,
    topicResource1: ConfigResource,
    topicResource2: ConfigResource
  ): Unit = {
    // Alter topics
    var topicConfigEntries1 = Seq(
      new ConfigEntry(TopicConfig.FLUSH_MS_CONFIG, "1000")
    ).asJava

    var topicConfigEntries2 = Seq(
      new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.9"),
      new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    ).asJava

    var alterResult = admin.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2)
    ).asJava)

    assertEquals(Set(topicResource1, topicResource2).asJava, alterResult.values.keySet)
    alterResult.all.get

    // Verify that topics were updated correctly
    test.ensureConsistentKRaftMetadata()
    var describeResult = admin.describeConfigs(Seq(topicResource1, topicResource2).asJava)
    var configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals("1000", configs.get(topicResource1).get(TopicConfig.FLUSH_MS_CONFIG).value)
    assertEquals(LogConfig.DEFAULT_MAX_MESSAGE_BYTES.toString,
      configs.get(topicResource1).get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG).value)
    assertEquals(LogConfig.DEFAULT_RETENTION_MS.toString, configs.get(topicResource1).get(TopicConfig.RETENTION_MS_CONFIG).value)

    assertEquals("0.9", configs.get(topicResource2).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
    assertEquals("lz4", configs.get(topicResource2).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)

    // Alter topics with validateOnly=true
    topicConfigEntries1 = Seq(
      new ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "10")
    ).asJava

    topicConfigEntries2 = Seq(
      new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.3")
    ).asJava

    alterResult = admin.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2)
    ).asJava, new AlterConfigsOptions().validateOnly(true))

    assertEquals(Set(topicResource1, topicResource2).asJava, alterResult.values.keySet)
    alterResult.all.get

    // Verify that topics were not updated due to validateOnly = true
    test.ensureConsistentKRaftMetadata()
    describeResult = admin.describeConfigs(Seq(topicResource1, topicResource2).asJava)
    configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals(LogConfig.DEFAULT_MAX_MESSAGE_BYTES.toString,
      configs.get(topicResource1).get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG).value)
    assertEquals("0.9", configs.get(topicResource2).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
  }

  @nowarn("cat=deprecation")
  def checkInvalidAlterConfigs(
    test: KafkaServerTestHarness,
    admin: Admin
  ): Unit = {
    // Create topics
    val topic1 = "invalid-alter-configs-topic-1"
    val topicResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    createTopicWithAdmin(admin, topic1, test.brokers, test.controllerServers, numPartitions = 1, replicationFactor = 1)

    val topic2 = "invalid-alter-configs-topic-2"
    val topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopicWithAdmin(admin, topic2, test.brokers, test.controllerServers, numPartitions = 1, replicationFactor = 1)

    val topicConfigEntries1 = Seq(
      new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "1.1"), // this value is invalid as it's above 1.0
      new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    ).asJava

    var topicConfigEntries2 = Seq(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")).asJava

    val brokerResource = new ConfigResource(ConfigResource.Type.BROKER, test.brokers.head.config.brokerId.toString)
    val brokerConfigEntries = Seq(new ConfigEntry(KafkaConfig.ZkConnectProp, "localhost:2181")).asJava

    // Alter configs: first and third are invalid, second is valid
    var alterResult = admin.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2),
      brokerResource -> new Config(brokerConfigEntries)
    ).asJava)

    assertEquals(Set(topicResource1, topicResource2, brokerResource).asJava, alterResult.values.keySet)
    assertFutureExceptionTypeEquals(alterResult.values.get(topicResource1), classOf[InvalidConfigurationException])
    alterResult.values.get(topicResource2).get
    assertFutureExceptionTypeEquals(alterResult.values.get(brokerResource), classOf[InvalidRequestException])

    // Verify that first and third resources were not updated and second was updated
    test.ensureConsistentKRaftMetadata()
    var describeResult = admin.describeConfigs(Seq(topicResource1, topicResource2, brokerResource).asJava)
    var configs = describeResult.all.get
    assertEquals(3, configs.size)

    assertEquals(LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO.toString,
      configs.get(topicResource1).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
    assertEquals(LogConfig.DEFAULT_COMPRESSION_TYPE,
      configs.get(topicResource1).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)

    assertEquals("snappy", configs.get(topicResource2).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)

    assertEquals(LogConfig.DEFAULT_COMPRESSION_TYPE, configs.get(brokerResource).get(KafkaConfig.CompressionTypeProp).value)

    // Alter configs with validateOnly = true: first and third are invalid, second is valid
    topicConfigEntries2 = Seq(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip")).asJava

    alterResult = admin.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2),
      brokerResource -> new Config(brokerConfigEntries)
    ).asJava, new AlterConfigsOptions().validateOnly(true))

    assertEquals(Set(topicResource1, topicResource2, brokerResource).asJava, alterResult.values.keySet)
    assertFutureExceptionTypeEquals(alterResult.values.get(topicResource1), classOf[InvalidConfigurationException])
    alterResult.values.get(topicResource2).get
    assertFutureExceptionTypeEquals(alterResult.values.get(brokerResource), classOf[InvalidRequestException])

    // Verify that no resources are updated since validate_only = true
    test.ensureConsistentKRaftMetadata()
    describeResult = admin.describeConfigs(Seq(topicResource1, topicResource2, brokerResource).asJava)
    configs = describeResult.all.get
    assertEquals(3, configs.size)

    assertEquals(LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO.toString,
      configs.get(topicResource1).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value)
    assertEquals(LogConfig.DEFAULT_COMPRESSION_TYPE,
      configs.get(topicResource1).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)

    assertEquals("snappy", configs.get(topicResource2).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value)

    assertEquals(LogConfig.DEFAULT_COMPRESSION_TYPE, configs.get(brokerResource).get(KafkaConfig.CompressionTypeProp).value)
  }
}
