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
import java.lang.{Long => JLong}
import java.time.{Duration => JDuration}
import java.util.Arrays.asList
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, ExecutionException, TimeUnit}
import java.util.{Collections, Properties}
import java.{time, util}

import kafka.log.LogConfig
import kafka.security.auth.{Cluster, Group, Topic}
import kafka.server.{Defaults, KafkaConfig, KafkaServer}
import kafka.utils.Implicits._
import kafka.utils.TestUtils._
import kafka.utils.{Log4jController, Logging, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{ConsumerGroupState, ElectionType, TopicPartition, TopicPartitionInfo, TopicPartitionReplica}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.config.{ConfigResource, LogLevelConfig}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{DeleteRecordsRequest, MetadataResponse}
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
import org.apache.kafka.common.utils.{Time, Utils}
import org.junit.Assert._
import org.junit.rules.Timeout
import org.junit.{After, Before, Ignore, Rule, Test}
import org.scalatest.Assertions.intercept

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random
/**
 * An integration test of the KafkaAdminClient.
 *
 * Also see {@link org.apache.kafka.clients.admin.KafkaAdminClientTest} for a unit test of the admin client.
 */
class AdminClientIntegrationTest extends IntegrationTestHarness with Logging {

  import AdminClientIntegrationTest._

  @Rule
  def globalTimeout = Timeout.millis(120000)

  var client: Admin = null
  var brokerLoggerConfigResource: ConfigResource = null
  var changedBrokerLoggers = scala.collection.mutable.Set[String]()

  val topic = "topic"
  val partition = 0
  val topicPartition = new TopicPartition(topic, partition)
  val clusterResourcePattern = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)


  @Before
  override def setUp(): Unit = {
    super.setUp
    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
    brokerLoggerConfigResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, servers.head.config.brokerId.toString)
  }

  @After
  override def tearDown(): Unit = {
    teardownBrokerLoggers()
    if (client != null)
      Utils.closeQuietly(client, "AdminClient")
    super.tearDown()
  }

  val brokerCount = 3
  val consumerCount = 1
  val producerCount = 1

  override def generateConfigs = {
    val cfgs = TestUtils.createBrokerConfigs(brokerCount, zkConnect, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties, logDirCount = 2)
    cfgs.foreach { config =>
      config.setProperty(KafkaConfig.ListenersProp, s"${listenerName.value}://localhost:${TestUtils.RandomPort}")
      config.remove(KafkaConfig.InterBrokerSecurityProtocolProp)
      config.setProperty(KafkaConfig.InterBrokerListenerNameProp, listenerName.value)
      config.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, s"${listenerName.value}:${securityProtocol.name}")
      config.setProperty(KafkaConfig.DeleteTopicEnableProp, "true")
      config.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
      config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, "false")
      config.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false")
      // We set this in order to test that we don't expose sensitive data via describe configs. This will already be
      // set for subclasses with security enabled and we don't want to overwrite it.
      if (!config.containsKey(KafkaConfig.SslTruststorePasswordProp))
        config.setProperty(KafkaConfig.SslTruststorePasswordProp, "some.invalid.pass")
    }
    cfgs.foreach(_ ++= serverConfig)
    cfgs.map(KafkaConfig.fromProps)
  }

  def createConfig(): util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.asScala.foreach { case (key, value) => config.put(key.asInstanceOf[String], value) }
    config
  }

  def waitForTopics(client: Admin, expectedPresent: Seq[String], expectedMissing: Seq[String]): Unit = {
    TestUtils.waitUntilTrue(() => {
        val topics = client.listTopics.names.get()
        expectedPresent.forall(topicName => topics.contains(topicName)) &&
          expectedMissing.forall(topicName => !topics.contains(topicName))
      }, "timed out waiting for topics")
  }

  @Test
  def testClose(): Unit = {
    val client = AdminClient.create(createConfig())
    client.close()
    client.close() // double close has no effect
  }

  @Test
  def testListNodes(): Unit = {
    client = AdminClient.create(createConfig())
    val brokerStrs = brokerList.split(",").toList.sorted
    var nodeStrs: List[String] = null
    do {
      val nodes = client.describeCluster().nodes().get().asScala
      nodeStrs = nodes.map ( node => s"${node.host}:${node.port}" ).toList.sorted
    } while (nodeStrs.size < brokerStrs.size)
    assertEquals(brokerStrs.mkString(","), nodeStrs.mkString(","))
  }

  @Test
  def testCreateDeleteTopics(): Unit = {
    client = AdminClient.create(createConfig())
    val topics = Seq("mytopic", "mytopic2", "mytopic3")
    val newTopics = Seq(
      new NewTopic("mytopic", Map((0: Integer) -> Seq[Integer](1, 2).asJava, (1: Integer) -> Seq[Integer](2, 0).asJava).asJava),
      new NewTopic("mytopic2", 3, 3.toShort),
      new NewTopic("mytopic3", Option.empty[Integer].asJava, Option.empty[java.lang.Short].asJava)
    )
    val validateResult = client.createTopics(newTopics.asJava, new CreateTopicsOptions().validateOnly(true))
    validateResult.all.get()
    waitForTopics(client, List(), topics)

    def validateMetadataAndConfigs(result: CreateTopicsResult): Unit = {
      assertEquals(2, result.numPartitions("mytopic").get())
      assertEquals(2, result.replicationFactor("mytopic").get())
      assertEquals(3, result.numPartitions("mytopic2").get())
      assertEquals(3, result.replicationFactor("mytopic2").get())
      assertEquals(configs.head.numPartitions, result.numPartitions("mytopic3").get())
      assertEquals(configs.head.defaultReplicationFactor, result.replicationFactor("mytopic3").get())
      assertFalse(result.config("mytopic").get().entries.isEmpty)
    }
    validateMetadataAndConfigs(validateResult)

    val createResult = client.createTopics(newTopics.asJava)
    createResult.all.get()
    waitForTopics(client, topics, List())
    validateMetadataAndConfigs(createResult)

    val failedCreateResult = client.createTopics(newTopics.asJava)
    val results = failedCreateResult.values()
    assertTrue(results.containsKey("mytopic"))
    assertFutureExceptionTypeEquals(results.get("mytopic"), classOf[TopicExistsException])
    assertTrue(results.containsKey("mytopic2"))
    assertFutureExceptionTypeEquals(results.get("mytopic2"), classOf[TopicExistsException])
    assertTrue(results.containsKey("mytopic3"))
    assertFutureExceptionTypeEquals(results.get("mytopic3"), classOf[TopicExistsException])
    assertFutureExceptionTypeEquals(failedCreateResult.numPartitions("mytopic3"), classOf[TopicExistsException])
    assertFutureExceptionTypeEquals(failedCreateResult.replicationFactor("mytopic3"), classOf[TopicExistsException])
    assertFutureExceptionTypeEquals(failedCreateResult.config("mytopic3"), classOf[TopicExistsException])

    val topicToDescription = client.describeTopics(topics.asJava).all.get()
    assertEquals(topics.toSet, topicToDescription.keySet.asScala)

    val topic0 = topicToDescription.get("mytopic")
    assertEquals(false, topic0.isInternal)
    assertEquals("mytopic", topic0.name)
    assertEquals(2, topic0.partitions.size)
    val topic0Partition0 = topic0.partitions.get(0)
    assertEquals(1, topic0Partition0.leader.id)
    assertEquals(0, topic0Partition0.partition)
    assertEquals(Seq(1, 2), topic0Partition0.isr.asScala.map(_.id))
    assertEquals(Seq(1, 2), topic0Partition0.replicas.asScala.map(_.id))
    val topic0Partition1 = topic0.partitions.get(1)
    assertEquals(2, topic0Partition1.leader.id)
    assertEquals(1, topic0Partition1.partition)
    assertEquals(Seq(2, 0), topic0Partition1.isr.asScala.map(_.id))
    assertEquals(Seq(2, 0), topic0Partition1.replicas.asScala.map(_.id))

    val topic1 = topicToDescription.get("mytopic2")
    assertEquals(false, topic1.isInternal)
    assertEquals("mytopic2", topic1.name)
    assertEquals(3, topic1.partitions.size)
    for (partitionId <- 0 until 3) {
      val partition = topic1.partitions.get(partitionId)
      assertEquals(partitionId, partition.partition)
      assertEquals(3, partition.replicas.size)
      partition.replicas.asScala.foreach { replica =>
        assertTrue(replica.id >= 0)
        assertTrue(replica.id < brokerCount)
      }
      assertEquals("No duplicate replica ids", partition.replicas.size, partition.replicas.asScala.map(_.id).distinct.size)

      assertEquals(3, partition.isr.size)
      assertEquals(partition.replicas, partition.isr)
      assertTrue(partition.replicas.contains(partition.leader))
    }

    val topic3 = topicToDescription.get("mytopic3")
    assertEquals("mytopic3", topic3.name)
    assertEquals(configs.head.numPartitions, topic3.partitions.size)
    assertEquals(configs.head.defaultReplicationFactor, topic3.partitions.get(0).replicas().size())

    client.deleteTopics(topics.asJava).all.get()
    waitForTopics(client, List(), topics)
  }

  @Test
  def testCreateExistingTopicsThrowTopicExistsException(): Unit = {
    client = AdminClient.create(createConfig())
    val topic = "mytopic"
    val topics = Seq(topic)
    val newTopics = Seq(new NewTopic(topic, 1, 1.toShort))

    client.createTopics(newTopics.asJava).all.get()
    waitForTopics(client, topics, List())

    val newTopicsWithInvalidRF = Seq(new NewTopic(topic, 1, (servers.size + 1).toShort))
    val e = intercept[ExecutionException] {
      client.createTopics(newTopicsWithInvalidRF.asJava, new CreateTopicsOptions().validateOnly(true)).all.get()
    }
    assertTrue(e.getCause.isInstanceOf[TopicExistsException])
  }

  @Test
  def testMetadataRefresh(): Unit = {
    client = AdminClient.create(createConfig())
    val topics = Seq("mytopic")
    val newTopics = Seq(new NewTopic("mytopic", 3, 3.toShort))
    client.createTopics(newTopics.asJava).all.get()
    waitForTopics(client, expectedPresent = topics, expectedMissing = List())

    val controller = servers.find(_.config.brokerId == TestUtils.waitUntilControllerElected(zkClient)).get
    controller.shutdown()
    controller.awaitShutdown()
    val topicDesc = client.describeTopics(topics.asJava).all.get()
    assertEquals(topics.toSet, topicDesc.keySet.asScala)
  }

  @Test
  def testAuthorizedOperations(): Unit = {
    client = AdminClient.create(createConfig())

    // without includeAuthorizedOperations flag
    var result = client.describeCluster
    assertEquals(Set().asJava, result.authorizedOperations().get())

    //with includeAuthorizedOperations flag
    result = client.describeCluster(new DescribeClusterOptions().includeAuthorizedOperations(true))
    var expectedOperations = configuredClusterPermissions.asJava
    assertEquals(expectedOperations, result.authorizedOperations().get())

    val topic = "mytopic"
    val newTopics = Seq(new NewTopic(topic, 3, 3.toShort))
    client.createTopics(newTopics.asJava).all.get()
    waitForTopics(client, expectedPresent = Seq(topic), expectedMissing = List())

    // without includeAuthorizedOperations flag
    var topicResult = getTopicMetadata(client, topic)
    assertEquals(Set().asJava, topicResult.authorizedOperations)

    //with includeAuthorizedOperations flag
    topicResult = getTopicMetadata(client, topic, new DescribeTopicsOptions().includeAuthorizedOperations(true))
    expectedOperations = Topic.supportedOperations
      .map(operation => operation.toJava).asJava
    assertEquals(expectedOperations, topicResult.authorizedOperations)
  }

  def configuredClusterPermissions() : Set[AclOperation] = {
    Cluster.supportedOperations.map(operation => operation.toJava)
  }

  /**
    * describe should not auto create topics
    */
  @Test
  def testDescribeNonExistingTopic(): Unit = {
    client = AdminClient.create(createConfig())

    val existingTopic = "existing-topic"
    client.createTopics(Seq(existingTopic).map(new NewTopic(_, 1, 1.toShort)).asJava).all.get()
    waitForTopics(client, Seq(existingTopic), List())

    val nonExistingTopic = "non-existing"
    val results = client.describeTopics(Seq(nonExistingTopic, existingTopic).asJava).values
    assertEquals(existingTopic, results.get(existingTopic).get.name)
    intercept[ExecutionException](results.get(nonExistingTopic).get).getCause.isInstanceOf[UnknownTopicOrPartitionException]
    assertEquals(None, zkClient.getTopicPartitionCount(nonExistingTopic))
  }

  @Test
  def testDescribeCluster(): Unit = {
    client = AdminClient.create(createConfig())
    val result = client.describeCluster
    val nodes = result.nodes.get()
    val clusterId = result.clusterId().get()
    assertEquals(servers.head.dataPlaneRequestProcessor.clusterId, clusterId)
    val controller = result.controller().get()
    assertEquals(servers.head.dataPlaneRequestProcessor.metadataCache.getControllerId.
      getOrElse(MetadataResponse.NO_CONTROLLER_ID), controller.id())
    val brokers = brokerList.split(",")
    assertEquals(brokers.size, nodes.size)
    for (node <- nodes.asScala) {
      val hostStr = s"${node.host}:${node.port}"
      assertTrue(s"Unknown host:port pair $hostStr in brokerVersionInfos", brokers.contains(hostStr))
    }
  }

  @Test
  def testDescribeLogDirs(): Unit = {
    client = AdminClient.create(createConfig())
    val topic = "topic"
    val leaderByPartition = createTopic(topic, numPartitions = 10, replicationFactor = 1)
    val partitionsByBroker = leaderByPartition.groupBy { case (partitionId, leaderId) => leaderId }.mapValues(_.keys.toSeq)
    val brokers = (0 until brokerCount).map(Integer.valueOf)
    val logDirInfosByBroker = client.describeLogDirs(brokers.asJava).all.get

    (0 until brokerCount).foreach { brokerId =>
      val server = servers.find(_.config.brokerId == brokerId).get
      val expectedPartitions = partitionsByBroker(brokerId)
      val logDirInfos = logDirInfosByBroker.get(brokerId)
      val replicaInfos = logDirInfos.asScala.flatMap { case (logDir, logDirInfo) => logDirInfo.replicaInfos.asScala }.filterKeys(_.topic == topic)

      assertEquals(expectedPartitions.toSet, replicaInfos.keys.map(_.partition).toSet)
      logDirInfos.asScala.foreach { case (logDir, logDirInfo) =>
        logDirInfo.replicaInfos.asScala.keys.foreach(tp =>
          assertEquals(server.logManager.getLog(tp).get.dir.getParent, logDir)
        )
      }
    }
  }

  @Test
  def testDescribeReplicaLogDirs(): Unit = {
    client = AdminClient.create(createConfig())
    val topic = "topic"
    val leaderByPartition = createTopic(topic, numPartitions = 10, replicationFactor = 1)
    val replicas = leaderByPartition.map { case (partition, brokerId) =>
      new TopicPartitionReplica(topic, partition, brokerId)
    }.toSeq

    val replicaDirInfos = client.describeReplicaLogDirs(replicas.asJavaCollection).all.get
    replicaDirInfos.asScala.foreach { case (topicPartitionReplica, replicaDirInfo) =>
      val server = servers.find(_.config.brokerId == topicPartitionReplica.brokerId()).get
      val tp = new TopicPartition(topicPartitionReplica.topic(), topicPartitionReplica.partition())
      assertEquals(server.logManager.getLog(tp).get.dir.getParent, replicaDirInfo.getCurrentReplicaLogDir)
    }
  }

  @Test
  def testAlterReplicaLogDirs(): Unit = {
    client = AdminClient.create(createConfig())
    val topic = "topic"
    val tp = new TopicPartition(topic, 0)
    val randomNums = servers.map(server => server -> Random.nextInt(2)).toMap

    // Generate two mutually exclusive replicaAssignment
    val firstReplicaAssignment = servers.map { server =>
      val logDir = new File(server.config.logDirs(randomNums(server))).getAbsolutePath
      new TopicPartitionReplica(topic, 0, server.config.brokerId) -> logDir
    }.toMap
    val secondReplicaAssignment = servers.map { server =>
      val logDir = new File(server.config.logDirs(1 - randomNums(server))).getAbsolutePath
      new TopicPartitionReplica(topic, 0, server.config.brokerId) -> logDir
    }.toMap

    // Verify that replica can be created in the specified log directory
    val futures = client.alterReplicaLogDirs(firstReplicaAssignment.asJava,
      new AlterReplicaLogDirsOptions).values.asScala.values
    futures.foreach { future =>
      val exception = intercept[ExecutionException](future.get)
      assertTrue(exception.getCause.isInstanceOf[UnknownTopicOrPartitionException])
    }

    createTopic(topic, numPartitions = 1, replicationFactor = brokerCount)
    servers.foreach { server =>
      val logDir = server.logManager.getLog(tp).get.dir.getParent
      assertEquals(firstReplicaAssignment(new TopicPartitionReplica(topic, 0, server.config.brokerId)), logDir)
    }

    // Verify that replica can be moved to the specified log directory after the topic has been created
    client.alterReplicaLogDirs(secondReplicaAssignment.asJava, new AlterReplicaLogDirsOptions).all.get
    servers.foreach { server =>
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
        TestUtils.getBrokerListStrFromServers(servers, protocol = securityProtocol),
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
      servers.foreach { server =>
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
    val consumerRecords = TestUtils.consumeTopicRecords(servers, topic, finalNumMessages,
      securityProtocol = securityProtocol, trustStoreFile = trustStoreFile)
    consumerRecords.zipWithIndex.foreach { case (consumerRecord, index) =>
      assertEquals(s"xxxxxxxxxxxxxxxxxxxx-$index", new String(consumerRecord.value))
    }
  }

  @Test
  def testDescribeAndAlterConfigs(): Unit = {
    client = AdminClient.create(createConfig)

    // Create topics
    val topic1 = "describe-alter-configs-topic-1"
    val topicResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    val topicConfig1 = new Properties
    topicConfig1.setProperty(LogConfig.MaxMessageBytesProp, "500000")
    topicConfig1.setProperty(LogConfig.RetentionMsProp, "60000000")
    createTopic(topic1, numPartitions = 1, replicationFactor = 1, topicConfig1)

    val topic2 = "describe-alter-configs-topic-2"
    val topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopic(topic2, numPartitions = 1, replicationFactor = 1)

    // Describe topics and broker
    val brokerResource1 = new ConfigResource(ConfigResource.Type.BROKER, servers(1).config.brokerId.toString)
    val brokerResource2 = new ConfigResource(ConfigResource.Type.BROKER, servers(2).config.brokerId.toString)
    val configResources = Seq(topicResource1, topicResource2, brokerResource1, brokerResource2)
    val describeResult = client.describeConfigs(configResources.asJava)
    val configs = describeResult.all.get

    assertEquals(4, configs.size)

    val maxMessageBytes1 = configs.get(topicResource1).get(LogConfig.MaxMessageBytesProp)
    assertEquals(LogConfig.MaxMessageBytesProp, maxMessageBytes1.name)
    assertEquals(topicConfig1.get(LogConfig.MaxMessageBytesProp), maxMessageBytes1.value)
    assertFalse(maxMessageBytes1.isDefault)
    assertFalse(maxMessageBytes1.isSensitive)
    assertFalse(maxMessageBytes1.isReadOnly)

    assertEquals(topicConfig1.get(LogConfig.RetentionMsProp),
      configs.get(topicResource1).get(LogConfig.RetentionMsProp).value)

    val maxMessageBytes2 = configs.get(topicResource2).get(LogConfig.MaxMessageBytesProp)
    assertEquals(Defaults.MessageMaxBytes.toString, maxMessageBytes2.value)
    assertEquals(LogConfig.MaxMessageBytesProp, maxMessageBytes2.name)
    assertTrue(maxMessageBytes2.isDefault)
    assertFalse(maxMessageBytes2.isSensitive)
    assertFalse(maxMessageBytes2.isReadOnly)

    assertEquals(servers(1).config.values.size, configs.get(brokerResource1).entries.size)
    assertEquals(servers(1).config.brokerId.toString, configs.get(brokerResource1).get(KafkaConfig.BrokerIdProp).value)
    val listenerSecurityProtocolMap = configs.get(brokerResource1).get(KafkaConfig.ListenerSecurityProtocolMapProp)
    assertEquals(servers(1).config.getString(KafkaConfig.ListenerSecurityProtocolMapProp), listenerSecurityProtocolMap.value)
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
    assertEquals(servers(1).config.compressionType.toString, compressionType.value)
    assertEquals(KafkaConfig.CompressionTypeProp, compressionType.name)
    assertTrue(compressionType.isDefault)
    assertFalse(compressionType.isSensitive)
    assertFalse(compressionType.isReadOnly)

    assertEquals(servers(2).config.values.size, configs.get(brokerResource2).entries.size)
    assertEquals(servers(2).config.brokerId.toString, configs.get(brokerResource2).get(KafkaConfig.BrokerIdProp).value)
    assertEquals(servers(2).config.logCleanerThreads.toString,
      configs.get(brokerResource2).get(KafkaConfig.LogCleanerThreadsProp).value)

    checkValidAlterConfigs(client, topicResource1, topicResource2)
  }

  @Test
  def testCreatePartitions(): Unit = {
    client = AdminClient.create(createConfig)

    // Create topics
    val topic1 = "create-partitions-topic-1"
    createTopic(topic1, numPartitions = 1, replicationFactor = 1)

    val topic2 = "create-partitions-topic-2"
    createTopic(topic2, numPartitions = 1, replicationFactor = 2)

    // assert that both the topics have 1 partition
    val topic1_metadata = getTopicMetadata(client, topic1)
    val topic2_metadata = getTopicMetadata(client, topic2)
    assertEquals(1, topic1_metadata.partitions.size)
    assertEquals(1, topic2_metadata.partitions.size)

    val validateOnly = new CreatePartitionsOptions().validateOnly(true)
    val actuallyDoIt = new CreatePartitionsOptions().validateOnly(false)

    def partitions(topic: String, expectedNumPartitionsOpt: Option[Int] = None): util.List[TopicPartitionInfo] = {
      getTopicMetadata(client, topic, expectedNumPartitionsOpt = expectedNumPartitionsOpt).partitions
    }

    def numPartitions(topic: String): Int = partitions(topic).size

    // validateOnly: try creating a new partition (no assignments), to bring the total to 3 partitions
    var alterResult = client.createPartitions(Map(topic1 ->
      NewPartitions.increaseTo(3)).asJava, validateOnly)
    var altered = alterResult.values.get(topic1).get
    assertEquals(1, numPartitions(topic1))

    // try creating a new partition (no assignments), to bring the total to 3 partitions
    alterResult = client.createPartitions(Map(topic1 ->
      NewPartitions.increaseTo(3)).asJava, actuallyDoIt)
    altered = alterResult.values.get(topic1).get
    TestUtils.waitUntilTrue(() => numPartitions(topic1) == 3, "Timed out waiting for new partitions to appear")

    // validateOnly: now try creating a new partition (with assignments), to bring the total to 3 partitions
    val newPartition2Assignments = asList[util.List[Integer]](asList(0, 1), asList(1, 2))
    alterResult = client.createPartitions(Map(topic2 ->
      NewPartitions.increaseTo(3, newPartition2Assignments)).asJava, validateOnly)
    altered = alterResult.values.get(topic2).get
    assertEquals(1, numPartitions(topic2))

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
      try {
        alterResult.values.get(topic1).get
        fail(s"$desc: Expect InvalidPartitionsException when newCount is a decrease")
      } catch {
        case e: ExecutionException =>
          assertTrue(desc, e.getCause.isInstanceOf[InvalidPartitionsException])
          assertEquals(desc, "Topic currently has 3 partitions, which is higher than the requested 1.", e.getCause.getMessage)
          assertEquals(desc, 3, numPartitions(topic1))
      }

      // try a newCount which would be a noop (without assignment)
      alterResult = client.createPartitions(Map(topic2 ->
        NewPartitions.increaseTo(3)).asJava, option)
      try {
        alterResult.values.get(topic2).get
        fail(s"$desc: Expect InvalidPartitionsException when requesting a noop")
      } catch {
        case e: ExecutionException =>
          assertTrue(desc, e.getCause.isInstanceOf[InvalidPartitionsException])
          assertEquals(desc, "Topic already has 3 partitions.", e.getCause.getMessage)
          assertEquals(desc, 3, numPartitions(topic2))
      }

      // try a newCount which would be a noop (where the assignment matches current state)
      alterResult = client.createPartitions(Map(topic2 ->
        NewPartitions.increaseTo(3, newPartition2Assignments)).asJava, option)
      try {
        alterResult.values.get(topic2).get
      } catch {
        case e: ExecutionException =>
          assertTrue(desc, e.getCause.isInstanceOf[InvalidPartitionsException])
          assertEquals(desc, "Topic already has 3 partitions.", e.getCause.getMessage)
          assertEquals(desc, 3, numPartitions(topic2))
      }

      // try a newCount which would be a noop (where the assignment doesn't match current state)
      alterResult = client.createPartitions(Map(topic2 ->
        NewPartitions.increaseTo(3, newPartition2Assignments.asScala.reverse.toList.asJava)).asJava, option)
      try {
        alterResult.values.get(topic2).get
      } catch {
        case e: ExecutionException =>
          assertTrue(desc, e.getCause.isInstanceOf[InvalidPartitionsException])
          assertEquals(desc, "Topic already has 3 partitions.", e.getCause.getMessage)
          assertEquals(desc, 3, numPartitions(topic2))
      }

      // try a bad topic name
      val unknownTopic = "an-unknown-topic"
      alterResult = client.createPartitions(Map(unknownTopic ->
        NewPartitions.increaseTo(2)).asJava, option)
      try {
        alterResult.values.get(unknownTopic).get
        fail(s"$desc: Expect InvalidTopicException when using an unknown topic")
      } catch {
        case e: ExecutionException =>
          assertTrue(desc, e.getCause.isInstanceOf[UnknownTopicOrPartitionException])
          assertEquals(desc, "The topic 'an-unknown-topic' does not exist.", e.getCause.getMessage)
      }

      // try an invalid newCount
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(-22)).asJava, option)
      try {
        altered = alterResult.values.get(topic1).get
        fail(s"$desc: Expect InvalidPartitionsException when newCount is invalid")
      } catch {
        case e: ExecutionException =>
          assertTrue(desc, e.getCause.isInstanceOf[InvalidPartitionsException])
          assertEquals(desc, "Topic currently has 3 partitions, which is higher than the requested -22.",
            e.getCause.getMessage)
          assertEquals(desc, 3, numPartitions(topic1))
      }

      // try assignments where the number of brokers != replication factor
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(4, asList(asList(1, 2)))).asJava, option)
      try {
        altered = alterResult.values.get(topic1).get
        fail(s"$desc: Expect InvalidPartitionsException when #brokers != replication factor")
      } catch {
        case e: ExecutionException =>
          assertTrue(desc, e.getCause.isInstanceOf[InvalidReplicaAssignmentException])
          assertEquals(desc, "Inconsistent replication factor between partitions, partition 0 has 1 " +
            "while partitions [3] have replication factors [2], respectively.",
            e.getCause.getMessage)
          assertEquals(desc, 3, numPartitions(topic1))
      }

      // try #assignments < with the increase
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(6, asList(asList(1)))).asJava, option)
      try {
        altered = alterResult.values.get(topic1).get
        fail(s"$desc: Expect InvalidReplicaAssignmentException when #assignments != newCount - oldCount")
      } catch {
        case e: ExecutionException =>
          assertTrue(desc, e.getCause.isInstanceOf[InvalidReplicaAssignmentException])
          assertEquals(desc, "Increasing the number of partitions by 3 but 1 assignments provided.", e.getCause.getMessage)
          assertEquals(desc, 3, numPartitions(topic1))
      }

      // try #assignments > with the increase
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(4, asList(asList(1), asList(2)))).asJava, option)
      try {
        altered = alterResult.values.get(topic1).get
        fail(s"$desc: Expect InvalidReplicaAssignmentException when #assignments != newCount - oldCount")
      } catch {
        case e: ExecutionException =>
          assertTrue(desc, e.getCause.isInstanceOf[InvalidReplicaAssignmentException])
          assertEquals(desc, "Increasing the number of partitions by 1 but 2 assignments provided.", e.getCause.getMessage)
          assertEquals(desc, 3, numPartitions(topic1))
      }

      // try with duplicate brokers in assignments
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(4, asList(asList(1, 1)))).asJava, option)
      try {
        altered = alterResult.values.get(topic1).get
        fail(s"$desc: Expect InvalidReplicaAssignmentException when assignments has duplicate brokers")
      } catch {
        case e: ExecutionException =>
          assertTrue(desc, e.getCause.isInstanceOf[InvalidReplicaAssignmentException])
          assertEquals(desc, "Duplicate brokers not allowed in replica assignment: 1, 1 for partition id 3.",
            e.getCause.getMessage)
          assertEquals(desc, 3, numPartitions(topic1))
      }

      // try assignments with differently sized inner lists
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(5, asList(asList(1), asList(1, 0)))).asJava, option)
      try {
        altered = alterResult.values.get(topic1).get
        fail(s"$desc: Expect InvalidReplicaAssignmentException when assignments have differently sized inner lists")
      } catch {
        case e: ExecutionException =>
          assertTrue(desc, e.getCause.isInstanceOf[InvalidReplicaAssignmentException])
          assertEquals(desc, "Inconsistent replication factor between partitions, partition 0 has 1 " +
            "while partitions [4] have replication factors [2], respectively.", e.getCause.getMessage)
          assertEquals(desc, 3, numPartitions(topic1))
      }

      // try assignments with unknown brokers
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(4, asList(asList(12)))).asJava, option)
      try {
        altered = alterResult.values.get(topic1).get
        fail(s"$desc: Expect InvalidReplicaAssignmentException when assignments contains an unknown broker")
      } catch {
        case e: ExecutionException =>
          assertTrue(desc, e.getCause.isInstanceOf[InvalidReplicaAssignmentException])
          assertEquals(desc, "Unknown broker(s) in replica assignment: 12.", e.getCause.getMessage)
          assertEquals(desc, 3, numPartitions(topic1))
      }

      // try with empty assignments
      alterResult = client.createPartitions(Map(topic1 ->
        NewPartitions.increaseTo(4, Collections.emptyList())).asJava, option)
      try {
        altered = alterResult.values.get(topic1).get
        fail(s"$desc: Expect InvalidReplicaAssignmentException when assignments is empty")
      } catch {
        case e: ExecutionException =>
          assertTrue(desc, e.getCause.isInstanceOf[InvalidReplicaAssignmentException])
          assertEquals(desc, "Increasing the number of partitions by 1 but 0 assignments provided.", e.getCause.getMessage)
          assertEquals(desc, 3, numPartitions(topic1))
      }
    }

    // a mixed success, failure response
    alterResult = client.createPartitions(Map(
      topic1 -> NewPartitions.increaseTo(4),
      topic2 -> NewPartitions.increaseTo(2)).asJava, actuallyDoIt)
    // assert that the topic1 now has 4 partitions
    altered = alterResult.values.get(topic1).get
    TestUtils.waitUntilTrue(() => numPartitions(topic1) == 4, "Timed out waiting for new partitions to appear")
    try {
      altered = alterResult.values.get(topic2).get
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[InvalidPartitionsException])
        assertEquals("Topic currently has 3 partitions, which is higher than the requested 2.", e.getCause.getMessage)
        // assert that the topic2 still has 3 partitions
        assertEquals(3, numPartitions(topic2))
    }

    // finally, try to add partitions to a topic queued for deletion
    val deleteResult = client.deleteTopics(asList(topic1))
    deleteResult.values.get(topic1).get
    alterResult = client.createPartitions(Map(topic1 ->
      NewPartitions.increaseTo(4)).asJava, validateOnly)
    try {
      altered = alterResult.values.get(topic1).get
      fail("Expect InvalidTopicException when the topic is queued for deletion")
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[InvalidTopicException])
        assertEquals("The topic is queued for deletion.", e.getCause.getMessage)
    }
  }

  @Test
  def testSeekAfterDeleteRecords(): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)

    client = AdminClient.create(createConfig)

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

  @Test
  def testLogStartOffsetCheckpoint(): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)

    client = AdminClient.create(createConfig)

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
    brokerList = TestUtils.bootstrapServers(servers, listenerName)
    client = AdminClient.create(createConfig)

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
          e.getCause.isInstanceOf[NotLeaderForPartitionException] => false
        }
    }, s"Expected low watermark of the partition to be 5 but got ${lowWatermark.getOrElse("no response within the timeout")}")
  }

  @Test
  def testLogStartOffsetAfterDeleteRecords(): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)

    client = AdminClient.create(createConfig)

    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)

    val result = client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(3L)).asJava)
    val lowWatermark = result.lowWatermarks.get(topicPartition).get.lowWatermark
    assertEquals(3L, lowWatermark)

    for (i <- 0 until brokerCount)
      assertEquals(3, servers(i).replicaManager.localLog(topicPartition).get.logStartOffset)
  }

  @Test
  def testReplicaCanFetchFromLogStartOffsetAfterDeleteRecords(): Unit = {
    val leaders = createTopic(topic, numPartitions = 1, replicationFactor = brokerCount)
    val followerIndex = if (leaders(0) != servers(0).config.brokerId) 0 else 1

    def waitForFollowerLog(expectedStartOffset: Long, expectedEndOffset: Long): Unit = {
      TestUtils.waitUntilTrue(() => servers(followerIndex).replicaManager.localLog(topicPartition) != None,
                              "Expected follower to create replica for partition")

      // wait until the follower discovers that log start offset moved beyond its HW
      TestUtils.waitUntilTrue(() => {
        servers(followerIndex).replicaManager.localLog(topicPartition).get.logStartOffset == expectedStartOffset
      }, s"Expected follower to discover new log start offset $expectedStartOffset")

      TestUtils.waitUntilTrue(() => {
        servers(followerIndex).replicaManager.localLog(topicPartition).get.logEndOffset == expectedEndOffset
      }, s"Expected follower to catch up to log end offset $expectedEndOffset")
    }

    // we will produce to topic and delete records while one follower is down
    killBroker(followerIndex)

    client = AdminClient.create(createConfig)
    val producer = createProducer()
    sendRecords(producer, 100, topicPartition)

    val result = client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(3L)).asJava)
    result.all().get()

    // start the stopped broker to verify that it will be able to fetch from new log start offset
    restartDeadBrokers()

    waitForFollowerLog(expectedStartOffset=3L, expectedEndOffset=100L)

    // after the new replica caught up, all replicas should have same log start offset
    for (i <- 0 until brokerCount)
      assertEquals(3, servers(i).replicaManager.localLog(topicPartition).get.logStartOffset)

    // kill the same follower again, produce more records, and delete records beyond follower's LOE
    killBroker(followerIndex)
    sendRecords(producer, 100, topicPartition)
    val result1 = client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(117L)).asJava)
    result1.all().get()
    restartDeadBrokers()
    waitForFollowerLog(expectedStartOffset=117L, expectedEndOffset=200L)
  }

  @Test
  def testAlterLogDirsAfterDeleteRecords(): Unit = {
    client = AdminClient.create(createConfig)
    createTopic(topic, numPartitions = 1, replicationFactor = brokerCount)
    val expectedLEO = 100
    val producer = createProducer()
    sendRecords(producer, expectedLEO, topicPartition)

    // delete records to move log start offset
    val result = client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(3L)).asJava)
    result.all().get()
    // make sure we are in the expected state after delete records
    for (i <- 0 until brokerCount) {
      assertEquals(3, servers(i).replicaManager.localLog(topicPartition).get.logStartOffset)
      assertEquals(expectedLEO, servers(i).replicaManager.localLog(topicPartition).get.logEndOffset)
    }

    // we will create another dir just for one server
    val futureLogDir = servers(0).config.logDirs(1)
    val futureReplica = new TopicPartitionReplica(topic, 0, servers(0).config.brokerId)

    // Verify that replica can be moved to the specified log directory
    client.alterReplicaLogDirs(Map(futureReplica -> futureLogDir).asJava).all.get
    TestUtils.waitUntilTrue(() => {
      futureLogDir == servers(0).logManager.getLog(topicPartition).get.dir.getParent
    }, "timed out waiting for replica movement")

    // once replica moved, its LSO and LEO should match other replicas
    assertEquals(3, servers.head.replicaManager.localLog(topicPartition).get.logStartOffset)
    assertEquals(expectedLEO, servers.head.replicaManager.localLog(topicPartition).get.logEndOffset)
  }

  @Test
  def testOffsetsForTimesAfterDeleteRecords(): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)

    client = AdminClient.create(createConfig)

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

  @Test
  def testConsumeAfterDeleteRecords(): Unit = {
    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    client = AdminClient.create(createConfig)

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

  @Test
  def testDeleteRecordsWithException(): Unit = {
    val consumer = createConsumer()
    subscribeAndWaitForAssignment(topic, consumer)

    client = AdminClient.create(createConfig)

    val producer = createProducer()
    sendRecords(producer, 10, topicPartition)

    assertEquals(5L, client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(5L)).asJava)
      .lowWatermarks.get(topicPartition).get.lowWatermark)

    // OffsetOutOfRangeException if offset > high_watermark
    var cause = intercept[ExecutionException] {
      client.deleteRecords(Map(topicPartition -> RecordsToDelete.beforeOffset(20L)).asJava).lowWatermarks.get(topicPartition).get
    }.getCause
    assertEquals(classOf[OffsetOutOfRangeException], cause.getClass)

    val nonExistPartition = new TopicPartition(topic, 3)
    // LeaderNotAvailableException if non existent partition
    cause = intercept[ExecutionException] {
      client.deleteRecords(Map(nonExistPartition -> RecordsToDelete.beforeOffset(20L)).asJava).lowWatermarks.get(nonExistPartition).get
    }.getCause
    assertEquals(classOf[LeaderNotAvailableException], cause.getClass)
  }

  @Test
  def testDescribeConfigsForTopic(): Unit = {
    createTopic(topic, numPartitions = 2, replicationFactor = brokerCount)
    client = AdminClient.create(createConfig)

    val existingTopic = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    client.describeConfigs(Collections.singletonList(existingTopic)).values.get(existingTopic).get()

    val nonExistentTopic = new ConfigResource(ConfigResource.Type.TOPIC, "unknown")
    val describeResult1 = client.describeConfigs(Collections.singletonList(nonExistentTopic))

    assertTrue(intercept[ExecutionException](describeResult1.values.get(nonExistentTopic).get).getCause.isInstanceOf[UnknownTopicOrPartitionException])

    val invalidTopic = new ConfigResource(ConfigResource.Type.TOPIC, "(invalid topic)")
    val describeResult2 = client.describeConfigs(Collections.singletonList(invalidTopic))

    assertTrue(intercept[ExecutionException](describeResult2.values.get(invalidTopic).get).getCause.isInstanceOf[InvalidTopicException])
  }

  private def subscribeAndWaitForAssignment(topic: String, consumer: KafkaConsumer[Array[Byte], Array[Byte]]): Unit = {
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

  @Test
  def testInvalidAlterConfigs(): Unit = {
    client = AdminClient.create(createConfig)
    checkInvalidAlterConfigs(zkClient, servers, client)
  }

  val ACL1 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
      new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))

  /**
   * Test that ACL operations are not possible when the authorizer is disabled.
   * Also see {@link kafka.api.SaslSslAdminClientIntegrationTest} for tests of ACL operations
   * when the authorizer is enabled.
   */
  @Test
  def testAclOperations(): Unit = {
    client = AdminClient.create(createConfig())
    assertFutureExceptionTypeEquals(client.describeAcls(AclBindingFilter.ANY).values(), classOf[SecurityDisabledException])
    assertFutureExceptionTypeEquals(client.createAcls(Collections.singleton(ACL1)).all(),
        classOf[SecurityDisabledException])
    assertFutureExceptionTypeEquals(client.deleteAcls(Collections.singleton(ACL1.toFilter())).all(),
      classOf[SecurityDisabledException])
  }

  /**
    * Test closing the AdminClient with a generous timeout.  Calls in progress should be completed,
    * since they can be done within the timeout.  New calls should receive timeouts.
    */
  @Test
  def testDelayedClose(): Unit = {
    client = AdminClient.create(createConfig())
    val topics = Seq("mytopic", "mytopic2")
    val newTopics = topics.map(new NewTopic(_, 1, 1.toShort))
    val future = client.createTopics(newTopics.asJava, new CreateTopicsOptions().validateOnly(true)).all()
    client.close(time.Duration.ofHours(2))
    val future2 = client.createTopics(newTopics.asJava, new CreateTopicsOptions().validateOnly(true)).all()
    assertFutureExceptionTypeEquals(future2, classOf[TimeoutException])
    future.get
    client.close(time.Duration.ofMinutes(30)) // multiple close-with-timeout should have no effect
  }

  /**
    * Test closing the AdminClient with a timeout of 0, when there are calls with extremely long
    * timeouts in progress.  The calls should be aborted after the hard shutdown timeout elapses.
    */
  @Test
  def testForceClose(): Unit = {
    val config = createConfig()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${TestUtils.IncorrectBrokerPort}")
    client = AdminClient.create(config)
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
  @Test
  def testMinimumRequestTimeouts(): Unit = {
    val config = createConfig()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${TestUtils.IncorrectBrokerPort}")
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "0")
    client = AdminClient.create(config)
    val startTimeMs = Time.SYSTEM.milliseconds()
    val future = client.createTopics(Seq("mytopic", "mytopic2").map(new NewTopic(_, 1, 1.toShort)).asJava,
      new CreateTopicsOptions().timeoutMs(2)).all()
    assertFutureExceptionTypeEquals(future, classOf[TimeoutException])
    val endTimeMs = Time.SYSTEM.milliseconds()
    assertTrue("Expected the timeout to take at least one millisecond.", endTimeMs > startTimeMs)
  }

  /**
    * Test injecting timeouts for calls that are in flight.
    */
  @Test
  def testCallInFlightTimeouts(): Unit = {
    val config = createConfig()
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "100000000")
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
  @Test
  def testConsumerGroups(): Unit = {
    val config = createConfig()
    client = AdminClient.create(config)
    try {
      // Verify that initially there are no consumer groups to list.
      val list1 = client.listConsumerGroups()
      assertTrue(0 == list1.all().get().size())
      assertTrue(0 == list1.errors().get().size())
      assertTrue(0 == list1.valid().get().size())
      val testTopicName = "test_topic"
      val testNumPartitions = 2
      client.createTopics(Collections.singleton(
        new NewTopic(testTopicName, testNumPartitions, 1.toShort))).all().get()
      waitForTopics(client, List(testTopicName), List())

      val producer = createProducer()
      try {
        producer.send(new ProducerRecord(testTopicName, 0, null, null)).get()
      } finally {
        Utils.closeQuietly(producer, "producer")
      }
      val testGroupId = "test_group_id"
      val testClientId = "test_client_id"
      val testInstanceId = "test_instance_id"
      val fakeGroupId = "fake_group_id"
      val newConsumerConfig = new Properties(consumerConfig)
      newConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId)
      newConsumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testClientId)
      newConsumerConfig.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, testInstanceId)
      val consumer = createConsumer(configOverrides = newConsumerConfig)
      val latch = new CountDownLatch(1)
      try {
        // Start a consumer in a thread that will subscribe to a new group.
        val consumerThread = new Thread {
          override def run : Unit = {
            consumer.subscribe(Collections.singleton(testTopicName))

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
        try {
          consumerThread.start
          assertTrue(latch.await(30000, TimeUnit.MILLISECONDS))
          // Test that we can list the new group.
          TestUtils.waitUntilTrue(() => {
            val matching = client.listConsumerGroups.all.get().asScala.filter(_.groupId == testGroupId)
            !matching.isEmpty
          }, s"Expected to be able to list $testGroupId")

          val describeWithFakeGroupResult = client.describeConsumerGroups(Seq(testGroupId, fakeGroupId).asJava,
            new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
          assertEquals(2, describeWithFakeGroupResult.describedGroups().size())

          // Test that we can get information about the test consumer group.
          assertTrue(describeWithFakeGroupResult.describedGroups().containsKey(testGroupId))
          var testGroupDescription = describeWithFakeGroupResult.describedGroups().get(testGroupId).get()

          assertEquals(testGroupId, testGroupDescription.groupId())
          assertFalse(testGroupDescription.isSimpleConsumerGroup())
          assertEquals(1, testGroupDescription.members().size())
          val member = testGroupDescription.members().iterator().next()
          assertEquals(testClientId, member.clientId())
          val topicPartitions = member.assignment().topicPartitions()
          assertEquals(testNumPartitions, topicPartitions.size())
          assertEquals(testNumPartitions, topicPartitions.asScala.
            count(tp => tp.topic().equals(testTopicName)))
          val expectedOperations = Group.supportedOperations
            .map(operation => operation.toJava).asJava
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
          var removeMemberResult = client.removeMemberFromConsumerGroup(testGroupId, new RemoveMemberFromConsumerGroupOptions(
            Collections.singletonList(invalidInstanceId)
          )).all()

          assertTrue(removeMemberResult.hasError)
          assertEquals(Errors.NONE, removeMemberResult.topLevelError)

          val firstMemberFutures = removeMemberResult.memberFutures()
          assertEquals(1, firstMemberFutures.size)
          firstMemberFutures.values.asScala foreach { case value =>
            try {
              value.get()
            } catch {
              case e: ExecutionException =>
                assertTrue(e.getCause.isInstanceOf[UnknownMemberIdException])
              case t: Throwable =>
                fail(s"Should have caught exception in getting member future: $t")
            }
          }

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

          // Test delete correct member
          removeMemberResult = client.removeMemberFromConsumerGroup(testGroupId, new RemoveMemberFromConsumerGroupOptions(
            Collections.singletonList(testInstanceId)
          )).all()

          assertFalse(removeMemberResult.hasError)
          assertEquals(Errors.NONE, removeMemberResult.topLevelError)

          val deletedMemberFutures = removeMemberResult.memberFutures()
          assertEquals(1, firstMemberFutures.size)
          deletedMemberFutures.values.asScala foreach { case value =>
            try {
              value.get()
            } catch {
              case e: ExecutionException =>
                assertTrue(e.getCause.isInstanceOf[UnknownMemberIdException])
              case t: Throwable =>
                fail(s"Should have caught exception in getting member future: $t")
            }
          }

          // The group should contain no member now.
          val describeTestGroupResult = client.describeConsumerGroups(Seq(testGroupId).asJava,
            new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
          assertEquals(1, describeTestGroupResult.describedGroups().size())

          testGroupDescription = describeTestGroupResult.describedGroups().get(testGroupId).get()

          assertEquals(testGroupId, testGroupDescription.groupId)
          assertFalse(testGroupDescription.isSimpleConsumerGroup)
          assertTrue(testGroupDescription.members().isEmpty)

          // Consumer group deletion on empty group should succeed
          deleteResult = client.deleteConsumerGroups(Seq(testGroupId).asJava)
          assertEquals(1, deleteResult.deletedGroups().size())

          assertTrue(deleteResult.deletedGroups().containsKey(testGroupId))
          assertNull(deleteResult.deletedGroups().get(testGroupId).get())
        } finally {
          consumerThread.interrupt()
          consumerThread.join()
        }
      } finally {
        Utils.closeQuietly(consumer, "consumer")
      }
    } finally {
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @Test
  def testDeleteConsumerGroupOffsets(): Unit = {
    val config = createConfig()
    client = AdminClient.create(config)
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
      newConsumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Defaults.GroupMaxSessionTimeoutMs.toString)
      val consumer = createConsumer(configOverrides = newConsumerConfig)

      try {
        TestUtils.subscribeAndWaitForRecords(testTopicName, consumer)
        consumer.commitSync()

        // Test offset deletion while consuming
        val offsetDeleteResult = client.deleteConsumerGroupOffsets(testGroupId, Set(tp1, tp2).asJava)

        assertNull(offsetDeleteResult.all().get())
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

      assertNull(offsetDeleteResult.all().get())
      assertNull(offsetDeleteResult.partitionResult(tp1).get())
      assertFutureExceptionTypeEquals(offsetDeleteResult.partitionResult(tp2),
        classOf[UnknownTopicOrPartitionException])

    } finally {
      Utils.closeQuietly(client, "adminClient")
    }
  }

  @Test
  def testElectPreferredLeaders(): Unit = {
    client = AdminClient.create(createConfig)

    val prefer0 = Seq(0, 1, 2)
    val prefer1 = Seq(1, 2, 0)
    val prefer2 = Seq(2, 0, 1)

    val partition1 = new TopicPartition("elect-preferred-leaders-topic-1", 0)
    TestUtils.createTopic(zkClient, partition1.topic, Map[Int, Seq[Int]](partition1.partition -> prefer0), servers)

    val partition2 = new TopicPartition("elect-preferred-leaders-topic-2", 0)
    TestUtils.createTopic(zkClient, partition2.topic, Map[Int, Seq[Int]](partition2.partition -> prefer0), servers)

    def preferredLeader(topicPartition: TopicPartition): Int = {
      val partitionMetadata = getTopicMetadata(client, topicPartition.topic).partitions.get(topicPartition.partition)
      val preferredLeaderMetadata = partitionMetadata.replicas.get(0)
      preferredLeaderMetadata.id
    }

    /** Changes the <i>preferred</i> leader without changing the <i>current</i> leader. */
    def changePreferredLeader(newAssignment: Seq[Int]) = {
      val preferred = newAssignment.head
      val prior1 = zkClient.getLeaderForPartition(partition1).get
      val prior2 = zkClient.getLeaderForPartition(partition2).get

      var m = Map.empty[TopicPartition, Seq[Int]]

      if (prior1 != preferred)
        m += partition1 -> newAssignment
      if (prior2 != preferred)
        m += partition2 -> newAssignment

      zkClient.createPartitionReassignment(m)
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
    var exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[ElectionNotNeededException], exception.getClass)
    assertEquals("Leader election not needed for topic partition", exception.getMessage)
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
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    TestUtils.assertLeader(client, partition1, 1)

    // topic 2 unchanged
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition2, 0)

    // meaningful election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertEquals(Set(partition2), electResult.partitions.get.keySet.asScala)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 1)

    // unknown topic
    val unknownPartition = new TopicPartition("topic-does-not-exist", 0)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition).asJava)
    assertEquals(Set(unknownPartition).asJava, electResult.partitions.get.keySet)
    exception = electResult.partitions.get.get(unknownPartition).get
    assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
    assertEquals("The partition does not exist.", exception.getMessage)
    TestUtils.assertLeader(client, partition1, 1)
    TestUtils.assertLeader(client, partition2, 1)

    // Now change the preferred leader to 2
    changePreferredLeader(prefer2)

    // mixed results
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition, partition1).asJava)
    assertEquals(Set(unknownPartition, partition1).asJava, electResult.partitions.get.keySet)
    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 1)
    exception = electResult.partitions.get.get(unknownPartition).get
    assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
    assertEquals("The partition does not exist.", exception.getMessage)

    // elect preferred leader for partition 2
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition2).asJava)
    assertEquals(Set(partition2).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 2)

    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)
    // but shut it down...
    servers(1).shutdown()
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(1))

    // ... now what happens if we try to elect the preferred leader and it's down?
    val shortTimeout = new ElectLeadersOptions().timeoutMs(10000)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava, shortTimeout)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)
    exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
    assertTrue(s"Wrong message ${exception.getMessage}", exception.getMessage.contains(
      "Failed to elect leader for partition elect-preferred-leaders-topic-1-0 under strategy PreferredReplicaPartitionLeaderElectionStrategy"))
    TestUtils.assertLeader(client, partition1, 2)

    // preferred leader unavailable with null argument
    electResult = client.electLeaders(ElectionType.PREFERRED, null, shortTimeout)

    exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
    assertTrue(s"Wrong message ${exception.getMessage}", exception.getMessage.contains(
      "Failed to elect leader for partition elect-preferred-leaders-topic-1-0 under strategy PreferredReplicaPartitionLeaderElectionStrategy"))

    exception = electResult.partitions.get.get(partition2).get
    assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
    assertTrue(s"Wrong message ${exception.getMessage}", exception.getMessage.contains(
      "Failed to elect leader for partition elect-preferred-leaders-topic-2-0 under strategy PreferredReplicaPartitionLeaderElectionStrategy"))

    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 2)
  }

  @Test
  def testElectUncleanLeadersForOnePartition(): Unit = {
    // Case: unclean leader election with one topic partition
    client = AdminClient.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)

    val partition1 = new TopicPartition("unclean-test-topic-1", 0)
    TestUtils.createTopic(zkClient, partition1.topic, Map[Int, Seq[Int]](partition1.partition -> assignment1), servers)

    TestUtils.assertLeader(client, partition1, broker1)

    servers(broker2).shutdown()
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1), Set(broker2))
    servers(broker1).shutdown()
    TestUtils.assertNoLeader(client, partition1)
    servers(broker2).startup()

    val electResult = client.electLeaders(ElectionType.UNCLEAN, Set(partition1).asJava)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    TestUtils.assertLeader(client, partition1, broker2)
  }

  @Test
  def testElectUncleanLeadersForManyPartitions(): Unit = {
    // Case: unclean leader election with many topic partitions
    client = AdminClient.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)
    val assignment2 = Seq(broker1, broker2)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)
    val partition2 = new TopicPartition(topic, 1)

    TestUtils.createTopic(
      zkClient,
      topic,
      Map(partition1.partition -> assignment1, partition2.partition -> assignment2),
      servers
    )

    TestUtils.assertLeader(client, partition1, broker1)
    TestUtils.assertLeader(client, partition2, broker1)

    servers(broker2).shutdown()
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(broker2))
    servers(broker1).shutdown()
    TestUtils.assertNoLeader(client, partition1)
    TestUtils.assertNoLeader(client, partition2)
    servers(broker2).startup()

    val electResult = client.electLeaders(ElectionType.UNCLEAN, Set(partition1, partition2).asJava)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition1, broker2)
    TestUtils.assertLeader(client, partition2, broker2)
  }

  @Test
  def testElectUncleanLeadersForAllPartitions(): Unit = {
    // Case: noop unclean leader election and valid unclean leader election for all partitions
    client = AdminClient.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val broker3 = 0
    val assignment1 = Seq(broker1, broker2)
    val assignment2 = Seq(broker1, broker3)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)
    val partition2 = new TopicPartition(topic, 1)

    TestUtils.createTopic(
      zkClient,
      topic,
      Map(partition1.partition -> assignment1, partition2.partition -> assignment2),
      servers
    )

    TestUtils.assertLeader(client, partition1, broker1)
    TestUtils.assertLeader(client, partition2, broker1)

    servers(broker2).shutdown()
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1), Set(broker2))
    servers(broker1).shutdown()
    TestUtils.assertNoLeader(client, partition1)
    TestUtils.assertLeader(client, partition2, broker3)
    servers(broker2).startup()

    val electResult = client.electLeaders(ElectionType.UNCLEAN, null)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition1, broker2)
    TestUtils.assertLeader(client, partition2, broker3)
  }

  @Test
  def testElectUncleanLeadersForUnknownPartitions(): Unit = {
    // Case: unclean leader election for unknown topic
    client = AdminClient.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)

    val topic = "unclean-test-topic-1"
    val unknownPartition = new TopicPartition(topic, 1)
    val unknownTopic = new TopicPartition("unknown-topic", 0)

    TestUtils.createTopic(
      zkClient,
      topic,
      Map(0 -> assignment1),
      servers
    )

    TestUtils.assertLeader(client, new TopicPartition(topic, 0), broker1)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, Set(unknownPartition, unknownTopic).asJava)
    assertTrue(electResult.partitions.get.get(unknownPartition).get.isInstanceOf[UnknownTopicOrPartitionException])
    assertTrue(electResult.partitions.get.get(unknownTopic).get.isInstanceOf[UnknownTopicOrPartitionException])
  }

  @Test
  def testElectUncleanLeadersWhenNoLiveBrokers(): Unit = {
    // Case: unclean leader election with no live brokers
    client = AdminClient.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)

    TestUtils.createTopic(
      zkClient,
      topic,
      Map(partition1.partition -> assignment1),
      servers
    )

    TestUtils.assertLeader(client, partition1, broker1)

    servers(broker2).shutdown()
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1), Set(broker2))
    servers(broker1).shutdown()
    TestUtils.assertNoLeader(client, partition1)

    val electResult = client.electLeaders(ElectionType.UNCLEAN, Set(partition1).asJava)
    assertTrue(electResult.partitions.get.get(partition1).get.isInstanceOf[EligibleLeadersNotAvailableException])
  }

  @Test
  def testElectUncleanLeadersNoop(): Unit = {
    // Case: noop unclean leader election with explicit topic partitions
    client = AdminClient.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val assignment1 = Seq(broker1, broker2)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)

    TestUtils.createTopic(
      zkClient,
      topic,
      Map(partition1.partition -> assignment1),
      servers
    )

    TestUtils.assertLeader(client, partition1, broker1)

    servers(broker1).shutdown()
    TestUtils.assertLeader(client, partition1, broker2)
    servers(broker1).startup()

    val electResult = client.electLeaders(ElectionType.UNCLEAN, Set(partition1).asJava)
    assertTrue(electResult.partitions.get.get(partition1).get.isInstanceOf[ElectionNotNeededException])
  }

  @Test
  def testElectUncleanLeadersAndNoop(): Unit = {
    // Case: one noop unclean leader election and one valid unclean leader election
    client = AdminClient.create(createConfig)

    val broker1 = 1
    val broker2 = 2
    val broker3 = 0
    val assignment1 = Seq(broker1, broker2)
    val assignment2 = Seq(broker1, broker3)

    val topic = "unclean-test-topic-1"
    val partition1 = new TopicPartition(topic, 0)
    val partition2 = new TopicPartition(topic, 1)

    TestUtils.createTopic(
      zkClient,
      topic,
      Map(partition1.partition -> assignment1, partition2.partition -> assignment2),
      servers
    )

    TestUtils.assertLeader(client, partition1, broker1)
    TestUtils.assertLeader(client, partition2, broker1)

    servers(broker2).shutdown()
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1), Set(broker2))
    servers(broker1).shutdown()
    TestUtils.assertNoLeader(client, partition1)
    TestUtils.assertLeader(client, partition2, broker3)
    servers(broker2).startup()

    val electResult = client.electLeaders(ElectionType.UNCLEAN, Set(partition1, partition2).asJava)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    assertTrue(electResult.partitions.get.get(partition2).get.isInstanceOf[ElectionNotNeededException])
    TestUtils.assertLeader(client, partition1, broker2)
    TestUtils.assertLeader(client, partition2, broker3)
  }

  @Test
  def testListReassignmentsDoesNotShowNonReassigningPartitions(): Unit = {
    client = AdminClient.create(createConfig())

    // Create topics
    val topic = "list-reassignments-no-reassignments"
    createTopic(topic, numPartitions = 1, replicationFactor = 3)
    val tp = new TopicPartition(topic, 0)

    val reassignmentsMap = client.listPartitionReassignments(Set(tp).asJava).reassignments().get()
    assertEquals(0, reassignmentsMap.size())

    val allReassignmentsMap = client.listPartitionReassignments().reassignments().get()
    assertEquals(0, allReassignmentsMap.size())
  }

  @Test
  def testListReassignmentsDoesNotShowDeletedPartitions(): Unit = {
    client = AdminClient.create(createConfig())
    
    val topic = "list-reassignments-no-reassignments"
    val tp = new TopicPartition(topic, 0)

    val reassignmentsMap = client.listPartitionReassignments(Set(tp).asJava).reassignments().get()
    assertEquals(0, reassignmentsMap.size())

    val allReassignmentsMap = client.listPartitionReassignments().reassignments().get()
    assertEquals(0, allReassignmentsMap.size())
  }

  @Test
  def testValidIncrementalAlterConfigs(): Unit = {
    client = AdminClient.create(createConfig)

    // Create topics
    val topic1 = "incremental-alter-configs-topic-1"
    val topic1Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    val topic1CreateConfigs = new Properties
    topic1CreateConfigs.setProperty(LogConfig.RetentionMsProp, "60000000")
    topic1CreateConfigs.setProperty(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    createTopic(topic1, numPartitions = 1, replicationFactor = 1, topic1CreateConfigs)

    val topic2 = "incremental-alter-configs-topic-2"
    val topic2Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopic(topic2)

    // Alter topic configs
    var topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(LogConfig.FlushMsProp, "1000"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(LogConfig.CleanupPolicyProp, LogConfig.Delete), AlterConfigOp.OpType.APPEND),
      new AlterConfigOp(new ConfigEntry(LogConfig.RetentionMsProp, ""), AlterConfigOp.OpType.DELETE)
    ).asJavaCollection

    val topic2AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "0.9"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(LogConfig.CompressionTypeProp, "lz4"), AlterConfigOp.OpType.SET)
    ).asJavaCollection

    var alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs,
      topic2Resource -> topic2AlterConfigs
    ).asJava)

    assertEquals(Set(topic1Resource, topic2Resource).asJava, alterResult.values.keySet)
    alterResult.all.get

    // Verify that topics were updated correctly
    var describeResult = client.describeConfigs(Seq(topic1Resource, topic2Resource).asJava)
    var configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals("1000", configs.get(topic1Resource).get(LogConfig.FlushMsProp).value)
    assertEquals("compact,delete", configs.get(topic1Resource).get(LogConfig.CleanupPolicyProp).value)
    assertEquals((Defaults.LogRetentionHours * 60 * 60 * 1000).toString, configs.get(topic1Resource).get(LogConfig.RetentionMsProp).value)

    assertEquals("0.9", configs.get(topic2Resource).get(LogConfig.MinCleanableDirtyRatioProp).value)
    assertEquals("lz4", configs.get(topic2Resource).get(LogConfig.CompressionTypeProp).value)

    //verify subtract operation
    topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(LogConfig.CleanupPolicyProp, LogConfig.Compact), AlterConfigOp.OpType.SUBTRACT)
    ).asJava

   alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs
    ).asJava)
    alterResult.all.get

    // Verify that topics were updated correctly
    describeResult = client.describeConfigs(Seq(topic1Resource).asJava)
    configs = describeResult.all.get

    assertEquals("delete", configs.get(topic1Resource).get(LogConfig.CleanupPolicyProp).value)
    assertEquals("1000", configs.get(topic1Resource).get(LogConfig.FlushMsProp).value) // verify previous change is still intact

    // Alter topics with validateOnly=true
    topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(LogConfig.CleanupPolicyProp, LogConfig.Compact), AlterConfigOp.OpType.APPEND)
    ).asJava

    alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs
    ).asJava, new AlterConfigsOptions().validateOnly(true))
    alterResult.all.get

    // Verify that topics were not updated due to validateOnly = true
    describeResult = client.describeConfigs(Seq(topic1Resource).asJava)
    configs = describeResult.all.get

    assertEquals("delete", configs.get(topic1Resource).get(LogConfig.CleanupPolicyProp).value)

    //Alter topics with validateOnly=true with invalid configs
    topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(LogConfig.CompressionTypeProp, "zip"), AlterConfigOp.OpType.SET)
    ).asJava

    alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs
    ).asJava, new AlterConfigsOptions().validateOnly(true))

    assertFutureExceptionTypeEquals(alterResult.values().get(topic1Resource), classOf[InvalidRequestException],
      Some("Invalid config value for resource"))
  }

  @Test
  def testInvalidIncrementalAlterConfigs(): Unit = {
    client = AdminClient.create(createConfig)

    // Create topics
    val topic1 = "incremental-alter-configs-topic-1"
    val topic1Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    createTopic(topic1)

    val topic2 = "incremental-alter-configs-topic-2"
    val topic2Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopic(topic2)

    //Add duplicate Keys for topic1
    var topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "0.75"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "0.65"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(LogConfig.CompressionTypeProp, "gzip"), AlterConfigOp.OpType.SET) // valid entry
    ).asJavaCollection

    //Add valid config for topic2
    var topic2AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "0.9"), AlterConfigOp.OpType.SET)
    ).asJavaCollection

    var alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs,
      topic2Resource -> topic2AlterConfigs
    ).asJava)
    assertEquals(Set(topic1Resource, topic2Resource).asJava, alterResult.values.keySet)

    //InvalidRequestException error for topic1
    assertFutureExceptionTypeEquals(alterResult.values().get(topic1Resource), classOf[InvalidRequestException],
      Some("Error due to duplicate config keys"))

    //operation should succeed for topic2
    alterResult.values().get(topic2Resource).get()

    // Verify that topic1 is not config not updated, and topic2 config is updated
    val describeResult = client.describeConfigs(Seq(topic1Resource, topic2Resource).asJava)
    val configs = describeResult.all.get
    assertEquals(2, configs.size)

    assertEquals(Defaults.LogCleanerMinCleanRatio.toString, configs.get(topic1Resource).get(LogConfig.MinCleanableDirtyRatioProp).value)
    assertEquals(Defaults.CompressionType.toString, configs.get(topic1Resource).get(LogConfig.CompressionTypeProp).value)
    assertEquals("0.9", configs.get(topic2Resource).get(LogConfig.MinCleanableDirtyRatioProp).value)

    //check invalid use of append/subtract operation types
    topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(LogConfig.CompressionTypeProp, "gzip"), AlterConfigOp.OpType.APPEND)
    ).asJavaCollection

    topic2AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(LogConfig.CompressionTypeProp, "snappy"), AlterConfigOp.OpType.SUBTRACT)
    ).asJavaCollection

    alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs,
      topic2Resource -> topic2AlterConfigs
    ).asJava)
    assertEquals(Set(topic1Resource, topic2Resource).asJava, alterResult.values.keySet)

    assertFutureExceptionTypeEquals(alterResult.values().get(topic1Resource), classOf[InvalidRequestException],
      Some("Config value append is not allowed for config"))

    assertFutureExceptionTypeEquals(alterResult.values().get(topic2Resource), classOf[InvalidRequestException],
      Some("Config value subtract is not allowed for config"))


    //try to add invalid config
    topic1AlterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "1.1"), AlterConfigOp.OpType.SET)
    ).asJavaCollection

    alterResult = client.incrementalAlterConfigs(Map(
      topic1Resource -> topic1AlterConfigs
    ).asJava)
    assertEquals(Set(topic1Resource).asJava, alterResult.values.keySet)

    assertFutureExceptionTypeEquals(alterResult.values().get(topic1Resource), classOf[InvalidRequestException],
      Some("Invalid config value for resource"))
  }

  @Test
  def testInvalidAlterPartitionReassignments(): Unit = {
    client = AdminClient.create(createConfig)
    val topic = "alter-reassignments-topic-1"
    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 1)
    val tp3 = new TopicPartition(topic, 2)
    createTopic(topic, numPartitions = 3)

    val validAssignment = new NewPartitionReassignment((0 until brokerCount).map(_.asInstanceOf[Integer]).asJava)

    val nonExistentTp1 = new TopicPartition("topicA", 0)
    val nonExistentTp2 = new TopicPartition(topic, 3)
    val nonExistentPartitionsResult = client.alterPartitionReassignments(Map(
      tp1 -> java.util.Optional.of(validAssignment),
      tp2 -> java.util.Optional.of(validAssignment),
      tp3 -> java.util.Optional.of(validAssignment),
      nonExistentTp1 -> java.util.Optional.of(validAssignment),
      nonExistentTp2 -> java.util.Optional.of(validAssignment)
    ).asJava).values()
    assertFutureExceptionTypeEquals(nonExistentPartitionsResult.get(nonExistentTp1), classOf[UnknownTopicOrPartitionException])
    assertFutureExceptionTypeEquals(nonExistentPartitionsResult.get(nonExistentTp2), classOf[UnknownTopicOrPartitionException])

    val extraNonExistentReplica = new NewPartitionReassignment((0 until brokerCount + 1).map(_.asInstanceOf[Integer]).asJava)
    val negativeIdReplica = new NewPartitionReassignment(Seq(-3, -2, -1).map(_.asInstanceOf[Integer]).asJava)
    val duplicateReplica = new NewPartitionReassignment(Seq(0, 1, 1).map(_.asInstanceOf[Integer]).asJava)
    val invalidReplicaResult = client.alterPartitionReassignments(Map(
      tp1 -> java.util.Optional.of(extraNonExistentReplica),
      tp2 -> java.util.Optional.of(negativeIdReplica),
      tp3 -> java.util.Optional.of(duplicateReplica)
    ).asJava).values()
    assertFutureExceptionTypeEquals(invalidReplicaResult.get(tp1), classOf[InvalidReplicaAssignmentException])
    assertFutureExceptionTypeEquals(invalidReplicaResult.get(tp2), classOf[InvalidReplicaAssignmentException])
    assertFutureExceptionTypeEquals(invalidReplicaResult.get(tp3), classOf[InvalidReplicaAssignmentException])
  }

  @Test
  def testLongTopicNames(): Unit = {
    val client = AdminClient.create(createConfig)
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
      Map(new TopicPartitionReplica(longTopicName, 0, 0) -> servers(0).config.logDirs(0)).asJava).all(),
      classOf[InvalidTopicException])
    client.close()
  }

  @Test
  def testDescribeConfigsForLog4jLogLevels(): Unit = {
    client = AdminClient.create(createConfig())

    val loggerConfig = describeBrokerLoggers()
    val rootLogLevel = loggerConfig.get(Log4jController.ROOT_LOGGER).value()
    val logCleanerLogLevelConfig = loggerConfig.get("kafka.cluster.Replica")
    assertEquals(rootLogLevel, logCleanerLogLevelConfig.value()) // we expect an undefined log level to be the same as the root logger
    assertEquals("kafka.cluster.Replica", logCleanerLogLevelConfig.name())
    assertEquals(ConfigEntry.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG, logCleanerLogLevelConfig.source())
    assertEquals(false, logCleanerLogLevelConfig.isReadOnly)
    assertEquals(false, logCleanerLogLevelConfig.isSensitive)
    assertTrue(logCleanerLogLevelConfig.synonyms().isEmpty)
  }

  @Test
  @Ignore // To be re-enabled once KAFKA-8779 is resolved
  def testIncrementalAlterConfigsForLog4jLogLevels(): Unit = {
    client = AdminClient.create(createConfig())

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
    * 5. Ensure the kafka.controller.KafkaController logger's level is ERROR (the curent root logger level)
    */
  @Test
  @Ignore // To be re-enabled once KAFKA-8779 is resolved
  def testIncrementalAlterConfigsForLog4jLogLevelsCanResetLoggerToCurrentRoot(): Unit = {
    client = AdminClient.create(createConfig())
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

  @Test
  @Ignore // To be re-enabled once KAFKA-8779 is resolved
  def testIncrementalAlterConfigsForLog4jLogLevelsCannotResetRootLogger(): Unit = {
    client = AdminClient.create(createConfig())
    val deleteRootLoggerEntry = Seq(
      new AlterConfigOp(new ConfigEntry(Log4jController.ROOT_LOGGER, ""), AlterConfigOp.OpType.DELETE)
    ).asJavaCollection

    assertTrue(intercept[ExecutionException](alterBrokerLoggers(deleteRootLoggerEntry)).getCause.isInstanceOf[InvalidRequestException])
  }

  @Test
  @Ignore // To be re-enabled once KAFKA-8779 is resolved
  def testIncrementalAlterConfigsForLog4jLogLevelsDoesNotWorkWithInvalidConfigs(): Unit = {
    client = AdminClient.create(createConfig())
    val validLoggerName = "kafka.server.KafkaRequestHandler"
    val expectedValidLoggerLogLevel = describeBrokerLoggers().get(validLoggerName)
    def assertLogLevelDidNotChange(): Unit = {
      assertEquals(
        expectedValidLoggerLogLevel,
        describeBrokerLoggers().get(validLoggerName)
      )
    }

    val appendLogLevelEntries = Seq(
      new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
      new AlterConfigOp(new ConfigEntry("kafka.network.SocketServer", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.APPEND) // append is not supported
    ).asJavaCollection
    assertTrue(intercept[ExecutionException](alterBrokerLoggers(appendLogLevelEntries)).getCause.isInstanceOf[InvalidRequestException])
    assertLogLevelDidNotChange()

    val subtractLogLevelEntries = Seq(
      new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
      new AlterConfigOp(new ConfigEntry("kafka.network.SocketServer", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SUBTRACT) // subtract is not supported
    ).asJavaCollection
    assertTrue(intercept[ExecutionException](alterBrokerLoggers(subtractLogLevelEntries)).getCause.isInstanceOf[InvalidRequestException])
    assertLogLevelDidNotChange()

    val invalidLogLevelLogLevelEntries = Seq(
      new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
      new AlterConfigOp(new ConfigEntry("kafka.network.SocketServer", "OFF"), AlterConfigOp.OpType.SET) // OFF is not a valid log level
    ).asJavaCollection
    assertTrue(intercept[ExecutionException](alterBrokerLoggers(invalidLogLevelLogLevelEntries)).getCause.isInstanceOf[InvalidRequestException])
    assertLogLevelDidNotChange()

    val invalidLoggerNameLogLevelEntries = Seq(
      new AlterConfigOp(new ConfigEntry("kafka.server.KafkaRequestHandler", LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
      new AlterConfigOp(new ConfigEntry("Some Other LogCleaner", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SET) // invalid logger name is not supported
    ).asJavaCollection
    assertTrue(intercept[ExecutionException](alterBrokerLoggers(invalidLoggerNameLogLevelEntries)).getCause.isInstanceOf[InvalidRequestException])
    assertLogLevelDidNotChange()
  }

  /**
    * The AlterConfigs API is deprecated and should not support altering log levels
    */
  @Test
  @Ignore // To be re-enabled once KAFKA-8779 is resolved
  def testAlterConfigsForLog4jLogLevelsDoesNotWork(): Unit = {
    client = AdminClient.create(createConfig())

    val alterLogLevelsEntries = Seq(
      new ConfigEntry("kafka.controller.KafkaController", LogLevelConfig.INFO_LOG_LEVEL)
    ).asJavaCollection
    val alterResult = client.alterConfigs(Map(brokerLoggerConfigResource -> new Config(alterLogLevelsEntries)).asJava)
    assertTrue(intercept[ExecutionException](alterResult.values.get(brokerLoggerConfigResource).get).getCause.isInstanceOf[InvalidRequestException])
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
  def teardownBrokerLoggers(): Unit = {
    if (changedBrokerLoggers.nonEmpty) {
      val validLoggers = describeBrokerLoggers().entries().asScala.filterNot(_.name().equals(Log4jController.ROOT_LOGGER)).map(_.name).toSet
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
}

object AdminClientIntegrationTest {

  def checkValidAlterConfigs(client: Admin, topicResource1: ConfigResource, topicResource2: ConfigResource): Unit = {
    // Alter topics
    var topicConfigEntries1 = Seq(
      new ConfigEntry(LogConfig.FlushMsProp, "1000")
    ).asJava

    var topicConfigEntries2 = Seq(
      new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "0.9"),
      new ConfigEntry(LogConfig.CompressionTypeProp, "lz4")
    ).asJava

    var alterResult = client.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2)
    ).asJava)

    assertEquals(Set(topicResource1, topicResource2).asJava, alterResult.values.keySet)
    alterResult.all.get

    // Verify that topics were updated correctly
    var describeResult = client.describeConfigs(Seq(topicResource1, topicResource2).asJava)
    var configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals("1000", configs.get(topicResource1).get(LogConfig.FlushMsProp).value)
    assertEquals(Defaults.MessageMaxBytes.toString,
      configs.get(topicResource1).get(LogConfig.MaxMessageBytesProp).value)
    assertEquals((Defaults.LogRetentionHours * 60 * 60 * 1000).toString,
      configs.get(topicResource1).get(LogConfig.RetentionMsProp).value)

    assertEquals("0.9", configs.get(topicResource2).get(LogConfig.MinCleanableDirtyRatioProp).value)
    assertEquals("lz4", configs.get(topicResource2).get(LogConfig.CompressionTypeProp).value)

    // Alter topics with validateOnly=true
    topicConfigEntries1 = Seq(
      new ConfigEntry(LogConfig.MaxMessageBytesProp, "10")
    ).asJava

    topicConfigEntries2 = Seq(
      new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "0.3")
    ).asJava

    alterResult = client.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2)
    ).asJava, new AlterConfigsOptions().validateOnly(true))

    assertEquals(Set(topicResource1, topicResource2).asJava, alterResult.values.keySet)
    alterResult.all.get

    // Verify that topics were not updated due to validateOnly = true
    describeResult = client.describeConfigs(Seq(topicResource1, topicResource2).asJava)
    configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals(Defaults.MessageMaxBytes.toString,
      configs.get(topicResource1).get(LogConfig.MaxMessageBytesProp).value)
    assertEquals("0.9", configs.get(topicResource2).get(LogConfig.MinCleanableDirtyRatioProp).value)
  }

  def checkInvalidAlterConfigs(zkClient: KafkaZkClient, servers: Seq[KafkaServer], client: Admin): Unit = {
    // Create topics
    val topic1 = "invalid-alter-configs-topic-1"
    val topicResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    TestUtils.createTopic(zkClient, topic1, 1, 1, servers)

    val topic2 = "invalid-alter-configs-topic-2"
    val topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    TestUtils.createTopic(zkClient, topic2, 1, 1, servers)

    val topicConfigEntries1 = Seq(
      new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "1.1"), // this value is invalid as it's above 1.0
      new ConfigEntry(LogConfig.CompressionTypeProp, "lz4")
    ).asJava

    var topicConfigEntries2 = Seq(new ConfigEntry(LogConfig.CompressionTypeProp, "snappy")).asJava

    val brokerResource = new ConfigResource(ConfigResource.Type.BROKER, servers.head.config.brokerId.toString)
    val brokerConfigEntries = Seq(new ConfigEntry(KafkaConfig.ZkConnectProp, "localhost:2181")).asJava

    // Alter configs: first and third are invalid, second is valid
    var alterResult = client.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2),
      brokerResource -> new Config(brokerConfigEntries)
    ).asJava)

    assertEquals(Set(topicResource1, topicResource2, brokerResource).asJava, alterResult.values.keySet)
    assertTrue(intercept[ExecutionException](alterResult.values.get(topicResource1).get).getCause.isInstanceOf[InvalidRequestException])
    alterResult.values.get(topicResource2).get
    assertTrue(intercept[ExecutionException](alterResult.values.get(brokerResource).get).getCause.isInstanceOf[InvalidRequestException])

    // Verify that first and third resources were not updated and second was updated
    var describeResult = client.describeConfigs(Seq(topicResource1, topicResource2, brokerResource).asJava)
    var configs = describeResult.all.get
    assertEquals(3, configs.size)

    assertEquals(Defaults.LogCleanerMinCleanRatio.toString,
      configs.get(topicResource1).get(LogConfig.MinCleanableDirtyRatioProp).value)
    assertEquals(Defaults.CompressionType.toString,
      configs.get(topicResource1).get(LogConfig.CompressionTypeProp).value)

    assertEquals("snappy", configs.get(topicResource2).get(LogConfig.CompressionTypeProp).value)

    assertEquals(Defaults.CompressionType.toString, configs.get(brokerResource).get(KafkaConfig.CompressionTypeProp).value)

    // Alter configs with validateOnly = true: first and third are invalid, second is valid
    topicConfigEntries2 = Seq(new ConfigEntry(LogConfig.CompressionTypeProp, "gzip")).asJava

    alterResult = client.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2),
      brokerResource -> new Config(brokerConfigEntries)
    ).asJava, new AlterConfigsOptions().validateOnly(true))

    assertEquals(Set(topicResource1, topicResource2, brokerResource).asJava, alterResult.values.keySet)
    assertTrue(intercept[ExecutionException](alterResult.values.get(topicResource1).get).getCause.isInstanceOf[InvalidRequestException])
    alterResult.values.get(topicResource2).get
    assertTrue(intercept[ExecutionException](alterResult.values.get(brokerResource).get).getCause.isInstanceOf[InvalidRequestException])

    // Verify that no resources are updated since validate_only = true
    describeResult = client.describeConfigs(Seq(topicResource1, topicResource2, brokerResource).asJava)
    configs = describeResult.all.get
    assertEquals(3, configs.size)

    assertEquals(Defaults.LogCleanerMinCleanRatio.toString,
      configs.get(topicResource1).get(LogConfig.MinCleanableDirtyRatioProp).value)
    assertEquals(Defaults.CompressionType.toString,
      configs.get(topicResource1).get(LogConfig.CompressionTypeProp).value)

    assertEquals("snappy", configs.get(topicResource2).get(LogConfig.CompressionTypeProp).value)

    assertEquals(Defaults.CompressionType.toString, configs.get(brokerResource).get(KafkaConfig.CompressionTypeProp).value)
  }

  private def getTopicMetadata(client: Admin,
                               topic: String,
                               describeOptions: DescribeTopicsOptions = new DescribeTopicsOptions,
                               expectedNumPartitionsOpt: Option[Int] = None): TopicDescription = {
    var result: TopicDescription = null

    TestUtils.waitUntilTrue(() => {
      val topicResult = client.describeTopics(Set(topic).asJava, describeOptions).values.get(topic)
      try {
        result = topicResult.get
        expectedNumPartitionsOpt.map(_ == result.partitions.size).getOrElse(true)
      } catch {
        case e: ExecutionException if e.getCause.isInstanceOf[UnknownTopicOrPartitionException] => false  // metadata may not have propagated yet, so retry
      }
    }, s"Timed out waiting for metadata for $topic")

    result
  }
}
