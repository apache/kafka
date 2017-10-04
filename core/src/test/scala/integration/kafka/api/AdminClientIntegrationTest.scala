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

import java.util
import java.util.{Collections, Properties}
import java.util.Arrays.asList
import java.util.concurrent.{ExecutionException, TimeUnit}
import java.io.File

import org.apache.kafka.clients.admin.KafkaAdminClientTest
import org.apache.kafka.common.utils.{Time, Utils}
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig
import kafka.server.{Defaults, KafkaConfig, KafkaServer}
import org.apache.kafka.clients.admin._
import kafka.utils.{Logging, TestUtils, ZkUtils}
import kafka.utils.Implicits._
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.{KafkaFuture, TopicPartition, TopicPartitionReplica}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors._
import org.junit.{After, Before, Rule, Test}
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.resource.{Resource, ResourceType}
import org.junit.rules.Timeout
import org.junit.Assert._

import scala.util.Random
import scala.collection.JavaConverters._

/**
 * An integration test of the KafkaAdminClient.
 *
 * Also see {@link org.apache.kafka.clients.admin.KafkaAdminClientTest} for a unit test of the admin client.
 */
class AdminClientIntegrationTest extends KafkaServerTestHarness with Logging {

  import AdminClientIntegrationTest._

  @Rule
  def globalTimeout = Timeout.millis(120000)

  var client: AdminClient = null

  @Before
  override def setUp(): Unit = {
    super.setUp
    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
  }

  @After
  override def tearDown(): Unit = {
    if (client != null)
      Utils.closeQuietly(client, "AdminClient")
    super.tearDown()
  }

  val brokerCount = 3
  lazy val serverConfig = new Properties

  def createConfig(): util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.asScala.foreach { case (key, value) => config.put(key.asInstanceOf[String], value) }
    config
  }

  def waitForTopics(client: AdminClient, expectedPresent: Seq[String], expectedMissing: Seq[String]): Unit = {
    TestUtils.waitUntilTrue(() => {
        val topics = client.listTopics.names.get()
        expectedPresent.forall(topicName => topics.contains(topicName)) &&
          expectedMissing.forall(topicName => !topics.contains(topicName))
      }, "timed out waiting for topics")
  }

  def assertFutureExceptionTypeEquals(future: KafkaFuture[_], clazz: Class[_ <: Throwable]): Unit = {
    try {
      future.get()
      fail("Expected CompletableFuture.get to return an exception")
    } catch {
      case e: ExecutionException =>
        val cause = e.getCause()
        assertTrue("Expected an exception of type " + clazz.getName + "; got type " +
            cause.getClass().getName, clazz.isInstance(cause))
    }
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
    val topics = Seq("mytopic", "mytopic2")
    val newTopics = Seq(
      new NewTopic("mytopic", Map((0: Integer) -> Seq[Integer](1, 2).asJava, (1: Integer) -> Seq[Integer](2, 0).asJava).asJava),
      new NewTopic("mytopic2", 3, 3)
    )
    client.createTopics(newTopics.asJava, new CreateTopicsOptions().validateOnly(true)).all.get()
    waitForTopics(client, List(), topics)

    client.createTopics(newTopics.asJava).all.get()
    waitForTopics(client, topics, List())

    val results = client.createTopics(newTopics.asJava).values()
    assertTrue(results.containsKey("mytopic"))
    assertFutureExceptionTypeEquals(results.get("mytopic"), classOf[TopicExistsException])
    assertTrue(results.containsKey("mytopic2"))
    assertFutureExceptionTypeEquals(results.get("mytopic2"), classOf[TopicExistsException])

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

    client.deleteTopics(topics.asJava).all.get()
    waitForTopics(client, List(), topics)
  }

  /**
    * describe should not auto create topics
    */
  @Test
  def testDescribeNonExistingTopic(): Unit = {
    client = AdminClient.create(createConfig())

    val existingTopic = "existing-topic"
    client.createTopics(Seq(existingTopic).map(new NewTopic(_, 1, 1)).asJava).all.get()
    waitForTopics(client, Seq(existingTopic), List())

    val nonExistingTopic = "non-existing"
    val results = client.describeTopics(Seq(nonExistingTopic, existingTopic).asJava).values
    assertEquals(existingTopic, results.get(existingTopic).get.name)
    intercept[ExecutionException](results.get(nonExistingTopic).get).getCause.isInstanceOf[UnknownTopicOrPartitionException]
    assertEquals(None, zkUtils.getTopicPartitionCount(nonExistingTopic))
  }

  @Test
  def testDescribeCluster(): Unit = {
    client = AdminClient.create(createConfig())
    val nodes = client.describeCluster.nodes.get()
    val clusterId = client.describeCluster().clusterId().get()
    assertEquals(servers.head.apis.clusterId, clusterId)
    val controller = client.describeCluster().controller().get()
    assertEquals(servers.head.apis.metadataCache.getControllerId.
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
    val leaderByPartition = TestUtils.createTopic(zkUtils, topic, 10, 1, servers, new Properties())
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

    client.close()
  }

  @Test
  def testDescribeReplicaLogDirs(): Unit = {
    client = AdminClient.create(createConfig())
    val topic = "topic"
    val leaderByPartition = TestUtils.createTopic(zkUtils, topic, 10, 1, servers, new Properties())
    val replicas = leaderByPartition.map { case (partition, brokerId) => new TopicPartitionReplica(topic, partition, brokerId) }.toSeq

    val replicaDirInfos = client.describeReplicaLogDirs(replicas.asJavaCollection).all.get
    replicaDirInfos.asScala.foreach { case (topicPartitionReplica, replicaDirInfo) =>
      val server = servers.find(_.config.brokerId == topicPartitionReplica.brokerId()).get
      val tp = new TopicPartition(topicPartitionReplica.topic(), topicPartitionReplica.partition())
      assertEquals(server.logManager.getLog(tp).get.dir.getParent, replicaDirInfo.getCurrentReplicaLogDir)
    }

    client.close()
  }

  @Test
  def testAlterReplicaLogDirsBeforeTopicCreation(): Unit = {
    val adminClient = AdminClient.create(createConfig())
    val topic = "topic"
    val tp = new TopicPartition(topic, 0)

    val replicaAssignment = servers.map { server =>
      val logDir = new File(server.config.logDirs(Random.nextInt(2))).getAbsolutePath
      new TopicPartitionReplica(topic, 0, server.config.brokerId) -> logDir
    }.toMap

    adminClient.alterReplicaLogDirs(replicaAssignment.asJava, new AlterReplicaLogDirsOptions()).values().asScala.values.foreach { future =>
      try {
        future.get()
        fail("Future should fail with ReplicaNotAvailableException")
      } catch {
        case e: ExecutionException => assertTrue(e.getCause.isInstanceOf[ReplicaNotAvailableException])
      }
    }

    TestUtils.createTopic(zkUtils, topic, 1, brokerCount, servers, new Properties())
    servers.foreach { server =>
      val logDir = server.logManager.getLog(tp).get.dir.getParent
      assertEquals(replicaAssignment(new TopicPartitionReplica(topic, 0, server.config.brokerId)), logDir)
    }
    adminClient.close()
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
    TestUtils.createTopic(zkUtils, topic1, 1, 1, servers, topicConfig1)

    val topic2 = "describe-alter-configs-topic-2"
    val topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    TestUtils.createTopic(zkUtils, topic2, 1, 1, servers, new Properties)

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
    assertTrue(listenerSecurityProtocolMap.isReadOnly)
    val truststorePassword = configs.get(brokerResource1).get(KafkaConfig.SslTruststorePasswordProp)
    assertEquals(KafkaConfig.SslTruststorePasswordProp, truststorePassword.name)
    assertNull(truststorePassword.value)
    assertFalse(truststorePassword.isDefault)
    assertTrue(truststorePassword.isSensitive)
    assertTrue(truststorePassword.isReadOnly)
    val compressionType = configs.get(brokerResource1).get(KafkaConfig.CompressionTypeProp)
    assertEquals(servers(1).config.compressionType.toString, compressionType.value)
    assertEquals(KafkaConfig.CompressionTypeProp, compressionType.name)
    assertTrue(compressionType.isDefault)
    assertFalse(compressionType.isSensitive)
    assertTrue(compressionType.isReadOnly)

    assertEquals(servers(2).config.values.size, configs.get(brokerResource2).entries.size)
    assertEquals(servers(2).config.brokerId.toString, configs.get(brokerResource2).get(KafkaConfig.BrokerIdProp).value)
    assertEquals(servers(2).config.logCleanerThreads.toString,
      configs.get(brokerResource2).get(KafkaConfig.LogCleanerThreadsProp).value)

    checkValidAlterConfigs(zkUtils, servers, client, topicResource1, topicResource2)
  }

  @Test
  def testCreatePartitions(): Unit = {
    client = AdminClient.create(createConfig)

    // Create topics
    val topic1 = "create-partitions-topic-1"
    TestUtils.createTopic(zkUtils, topic1, 1, 1, servers, new Properties)

    val topic2 = "create-partitions-topic-2"
    TestUtils.createTopic(zkUtils, topic2, 1, 2, servers, new Properties)

    // assert that both the topics have 1 partition
    assertEquals(1, client.describeTopics(Set(topic1).asJava).values.get(topic1).get.partitions.size)
    assertEquals(1, client.describeTopics(Set(topic2).asJava).values.get(topic2).get.partitions.size)

    val validateOnly = new CreatePartitionsOptions().validateOnly(true)
    val actuallyDoIt = new CreatePartitionsOptions().validateOnly(false)

    def partitions(topic: String) =
      client.describeTopics(Set(topic).asJava).values.get(topic).get.partitions

    def numPartitions(topic: String) =
      partitions(topic).size

    // validateOnly: try creating a new partition (no assignments), to bring the total to 3 partitions
    var alterResult = client.createPartitions(Map(topic1 ->
      NewPartitions.increaseTo(3)).asJava, validateOnly)
    var altered = alterResult.values.get(topic1).get
    assertEquals(1, numPartitions(topic1))

    // try creating a new partition (no assignments), to bring the total to 3 partitions
    alterResult = client.createPartitions(Map(topic1 ->
      NewPartitions.increaseTo(3)).asJava, actuallyDoIt)
    altered = alterResult.values.get(topic1).get
    assertEquals(3, numPartitions(topic1))

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
    var actualPartitions2 = partitions(topic2)
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
    assertEquals(4, numPartitions(topic1))
    try {
      altered = alterResult.values.get(topic2).get
    } catch {
      case e: ExecutionException =>
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
  def testInvalidAlterConfigs(): Unit = {
    client = AdminClient.create(createConfig)
    checkInvalidAlterConfigs(zkUtils, servers, client)
  }

  val ACL1 = new AclBinding(new Resource(ResourceType.TOPIC, "mytopic3"),
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
    client.close()
  }

  /**
    * Test closing the AdminClient with a generous timeout.  Calls in progress should be completed,
    * since they can be done within the timeout.  New calls should receive timeouts.
    */
  @Test
  def testDelayedClose(): Unit = {
    client = AdminClient.create(createConfig())
    val topics = Seq("mytopic", "mytopic2")
    val newTopics = topics.map(new NewTopic(_, 1, 1))
    val future = client.createTopics(newTopics.asJava, new CreateTopicsOptions().validateOnly(true)).all()
    client.close(2, TimeUnit.HOURS)
    val future2 = client.createTopics(newTopics.asJava, new CreateTopicsOptions().validateOnly(true)).all()
    assertFutureExceptionTypeEquals(future2, classOf[TimeoutException])
    future.get
    client.close(30, TimeUnit.MINUTES) // multiple close-with-timeout should have no effect
  }

  /**
    * Test closing the AdminClient with a timeout of 0, when there are calls with extremely long
    * timeouts in progress.  The calls should be aborted after the hard shutdown timeout elapses.
    */
  @Test
  def testForceClose(): Unit = {
    val config = createConfig()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:22")
    client = AdminClient.create(config)
    // Because the bootstrap servers are set up incorrectly, this call will not complete, but must be
    // cancelled by the close operation.
    val future = client.createTopics(Seq("mytopic", "mytopic2").map(new NewTopic(_, 1, 1)).asJava,
      new CreateTopicsOptions().timeoutMs(900000)).all()
    client.close(0, TimeUnit.MILLISECONDS)
    assertFutureExceptionTypeEquals(future, classOf[TimeoutException])
  }

  /**
    * Check that a call with a timeout does not complete before the minimum timeout has elapsed,
    * even when the default request timeout is shorter.
    */
  @Test
  def testMinimumRequestTimeouts(): Unit = {
    val config = createConfig()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:22")
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "0")
    client = AdminClient.create(config)
    val startTimeMs = Time.SYSTEM.milliseconds()
    val future = client.createTopics(Seq("mytopic", "mytopic2").map(new NewTopic(_, 1, 1)).asJava,
      new CreateTopicsOptions().timeoutMs(2)).all()
    assertFutureExceptionTypeEquals(future, classOf[TimeoutException])
    val endTimeMs = Time.SYSTEM.milliseconds()
    assertTrue("Expected the timeout to take at least one millisecond.", endTimeMs > startTimeMs);
    client.close()
  }

  override def generateConfigs = {
    val cfgs = TestUtils.createBrokerConfigs(brokerCount, zkConnect, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties, logDirCount = 2)
    cfgs.foreach { config =>
      config.setProperty(KafkaConfig.ListenersProp, s"${listenerName.value}://localhost:${TestUtils.RandomPort}")
      config.remove(KafkaConfig.InterBrokerSecurityProtocolProp)
      config.setProperty(KafkaConfig.InterBrokerListenerNameProp, listenerName.value)
      config.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, s"${listenerName.value}:${securityProtocol.name}")
      config.setProperty(KafkaConfig.DeleteTopicEnableProp, "true")
      // We set this in order to test that we don't expose sensitive data via describe configs. This will already be
      // set for subclasses with security enabled and we don't want to overwrite it.
      if (!config.containsKey(KafkaConfig.SslTruststorePasswordProp))
        config.setProperty(KafkaConfig.SslTruststorePasswordProp, "some.invalid.pass")
    }
    cfgs.foreach(_ ++= serverConfig)
    cfgs.map(KafkaConfig.fromProps)
  }

  /**
    * Test injecting timeouts for calls that are in flight.
    */
  @Test
  def testCallInFlightTimeouts(): Unit = {
    val config = createConfig()
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "100000000")
    val factory = new KafkaAdminClientTest.FailureInjectingTimeoutProcessorFactory()
    val client = KafkaAdminClientTest.createInternal(new AdminClientConfig(config), factory)
    val future = client.createTopics(Seq("mytopic", "mytopic2").map(new NewTopic(_, 1, 1)).asJava,
        new CreateTopicsOptions().validateOnly(true)).all()
    assertFutureExceptionTypeEquals(future, classOf[TimeoutException])
    val future2 = client.createTopics(Seq("mytopic3", "mytopic4").map(new NewTopic(_, 1, 1)).asJava,
      new CreateTopicsOptions().validateOnly(true)).all()
    future2.get
    client.close()
    assertEquals(1, factory.failuresInjected)
  }
}

object AdminClientIntegrationTest {

  import org.scalatest.Assertions._

  def checkValidAlterConfigs(zkUtils: ZkUtils, servers: Seq[KafkaServer], client: AdminClient,
                             topicResource1: ConfigResource, topicResource2: ConfigResource): Unit = {
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

  def checkInvalidAlterConfigs(zkUtils: ZkUtils, servers: Seq[KafkaServer], client: AdminClient): Unit = {
    // Create topics
    val topic1 = "invalid-alter-configs-topic-1"
    val topicResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    TestUtils.createTopic(zkUtils, topic1, 1, 1, servers, new Properties())

    val topic2 = "invalid-alter-configs-topic-2"
    val topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    TestUtils.createTopic(zkUtils, topic2, 1, 1, servers, new Properties)

    val topicConfigEntries1 = Seq(
      new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "1.1"), // this value is invalid as it's above 1.0
      new ConfigEntry(LogConfig.CompressionTypeProp, "lz4")
    ).asJava

    var topicConfigEntries2 = Seq(new ConfigEntry(LogConfig.CompressionTypeProp, "snappy")).asJava

    val brokerResource = new ConfigResource(ConfigResource.Type.BROKER, servers.head.config.brokerId.toString)
    val brokerConfigEntries = Seq(new ConfigEntry(KafkaConfig.CompressionTypeProp, "gzip")).asJava

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

}
