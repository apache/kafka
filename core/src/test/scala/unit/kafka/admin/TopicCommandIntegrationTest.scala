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
package kafka.admin

import java.util.{Collection, Collections, Optional, Properties}

import kafka.admin.TopicCommand.{TopicCommandOptions, TopicService}
import kafka.integration.KafkaServerTestHarness
import kafka.server.{ConfigType, KafkaConfig}
import kafka.utils.{Logging, TestUtils}
import kafka.zk.{ConfigEntityChangeNotificationZNode, DeleteTopicsTopicZNode}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.config.{ConfigException, ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, ThrottlingQuotaExceededException, TopicExistsException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.{Node, TopicPartition, TopicPartitionInfo}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers.{eq => eqThat, _}
import org.mockito.Mockito._

import scala.collection.Seq
import scala.concurrent.ExecutionException
import scala.jdk.CollectionConverters._
import scala.util.Random

class TopicCommandIntegrationTest extends KafkaServerTestHarness with Logging with RackAwareTest {

  /**
    * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
    * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
    *
    * Note the replica fetch max bytes is set to `1` in order to throttle the rate of replication for test
    * `testDescribeUnderReplicatedPartitionsWhenReassignmentIsInProgress`.
    */
  override def generateConfigs: Seq[KafkaConfig] = TestUtils.createBrokerConfigs(
    numConfigs = 6,
    zkConnect = zkConnect,
    rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 4 -> "rack3", 5 -> "rack3"),
    numPartitions = numPartitions,
    defaultReplicationFactor = defaultReplicationFactor,
  ).map { props =>
    props.put(KafkaConfig.ReplicaFetchMaxBytesProp, "1")
    KafkaConfig.fromProps(props)
  }

  private val numPartitions = 1
  private val defaultReplicationFactor = 1.toShort

  private var topicService: TopicService = _
  private var adminClient: Admin = _
  private var testTopicName: String = _

  private[this] def createAndWaitTopic(opts: TopicCommandOptions): Unit = {
    topicService.createTopic(opts)
    waitForTopicCreated(opts.topic.get)
  }

  private[this] def waitForTopicCreated(topicName: String, timeout: Int = 10000): Unit = {
    TestUtils.waitForPartitionMetadata(servers, topicName, partition = 0, timeout)
  }

  @BeforeEach
  override def setUp(info: TestInfo): Unit = {
    super.setUp(info)

    // create adminClient
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    adminClient = Admin.create(props)
    topicService = TopicService(adminClient)
    testTopicName = s"${info.getTestMethod.get().getName}-${Random.alphanumeric.take(10).mkString}"
  }

  @AfterEach
  def close(): Unit = {
    // adminClient is closed by topicService
    if (topicService != null)
      topicService.close()
  }

  @Test
  def testCreate(): Unit = {
    createAndWaitTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "1", "--topic", testTopicName)))

    adminClient.listTopics().names().get().contains(testTopicName)
  }

  @Test
  def testCreateWithDefaults(): Unit = {
    createAndWaitTopic(new TopicCommandOptions(Array("--topic", testTopicName)))

    val partitions = adminClient
      .describeTopics(Collections.singletonList(testTopicName))
      .allTopicNames()
      .get()
      .get(testTopicName)
      .partitions()
    assertEquals(partitions.size(), numPartitions)
    assertEquals(partitions.get(0).replicas().size(), defaultReplicationFactor)
  }

  @Test
  def testCreateWithDefaultReplication(): Unit = {
    createAndWaitTopic(new TopicCommandOptions(
      Array("--topic", testTopicName, "--partitions", "2")))

    val partitions = adminClient
      .describeTopics(Collections.singletonList(testTopicName))
      .allTopicNames()
      .get()
      .get(testTopicName)
      .partitions()
    assertEquals(partitions.size(), 2)
    assertEquals(partitions.get(0).replicas().size(), defaultReplicationFactor)
  }

  @Test
  def testCreateWithDefaultPartitions(): Unit = {
    createAndWaitTopic(new TopicCommandOptions(
      Array("--topic", testTopicName, "--replication-factor", "2")))

    val partitions = adminClient
      .describeTopics(Collections.singletonList(testTopicName))
      .allTopicNames()
      .get()
      .get(testTopicName)
      .partitions()

    assertEquals(partitions.size(), numPartitions)
    assertEquals(partitions.get(0).replicas().size(), 2)
  }

  @Test
  def testCreateWithConfigs(): Unit = {
    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName)
    createAndWaitTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "2", "--topic", testTopicName, "--config", "delete.retention.ms=1000")))

    val configs = adminClient
      .describeConfigs(Collections.singleton(configResource))
      .all().get().get(configResource)
    assertEquals(1000, Integer.valueOf(configs.get("delete.retention.ms").value()))
  }

  @Test
  def testCreateWhenAlreadyExists(): Unit = {
    val numPartitions = 1

    // create the topic
    val createOpts = new TopicCommandOptions(
      Array("--partitions", numPartitions.toString, "--replication-factor", "1", "--topic", testTopicName))
    createAndWaitTopic(createOpts)

    // try to re-create the topic
    assertThrows(classOf[TopicExistsException], () => topicService.createTopic(createOpts))
  }

  @Test
  def testCreateWhenAlreadyExistsWithIfNotExists(): Unit = {
    val createOpts = new TopicCommandOptions(Array("--topic", testTopicName, "--if-not-exists"))
    createAndWaitTopic(createOpts)
    topicService.createTopic(createOpts)
  }

  @Test
  def testCreateWithReplicaAssignment(): Unit = {
    // create the topic
    val createOpts = new TopicCommandOptions(
      Array("--replica-assignment", "5:4,3:2,1:0", "--topic", testTopicName))
    createAndWaitTopic(createOpts)

    val partitions = adminClient
      .describeTopics(Collections.singletonList(testTopicName))
      .allTopicNames()
      .get()
      .get(testTopicName)
      .partitions()
    assertEquals(3, partitions.size())
    assertEquals(List(5, 4), partitions.get(0).replicas().asScala.map(_.id()))
    assertEquals(List(3, 2), partitions.get(1).replicas().asScala.map(_.id()))
    assertEquals(List(1, 0), partitions.get(2).replicas().asScala.map(_.id()))
  }

  @Test
  def testCreateWithInvalidReplicationFactor(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "2", "--replication-factor", (Short.MaxValue+1).toString, "--topic", testTopicName))))
  }

  @Test
  def testCreateWithNegativeReplicationFactor(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "2", "--replication-factor", "-1", "--topic", testTopicName))))
  }

  @Test
  def testCreateWithNegativePartitionCount(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "-1", "--replication-factor", "1", "--topic", testTopicName))))
  }

  @Test
  def testInvalidTopicLevelConfig(): Unit = {
    val createOpts = new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName,
        "--config", "message.timestamp.type=boom"))
    assertThrows(classOf[ConfigException], () => topicService.createTopic(createOpts))
  }

  @Test
  def testListTopics(): Unit = {
    createAndWaitTopic(new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName)))

    val output = TestUtils.grabConsoleOutput(
      topicService.listTopics(new TopicCommandOptions(Array())))

    assertTrue(output.contains(testTopicName))
  }

  @Test
  def testListTopicsWithIncludeList(): Unit = {
    val topic1 = "kafka.testTopic1"
    val topic2 = "kafka.testTopic2"
    val topic3 = "oooof.testTopic1"
    adminClient.createTopics(
      List(new NewTopic(topic1, 2, 2.toShort),
        new NewTopic(topic2, 2, 2.toShort),
        new NewTopic(topic3, 2, 2.toShort)).asJavaCollection)
      .all().get()
    waitForTopicCreated(topic1)
    waitForTopicCreated(topic2)
    waitForTopicCreated(topic3)

    val output = TestUtils.grabConsoleOutput(
      topicService.listTopics(new TopicCommandOptions(Array("--topic", "kafka.*"))))

    assertTrue(output.contains(topic1))
    assertTrue(output.contains(topic2))
    assertFalse(output.contains(topic3))
  }

  @Test
  def testListTopicsWithExcludeInternal(): Unit = {
    val topic1 = "kafka.testTopic1"
    adminClient.createTopics(
      List(new NewTopic(topic1, 2, 2.toShort),
        new NewTopic(Topic.GROUP_METADATA_TOPIC_NAME, 2, 2.toShort)).asJavaCollection)
      .all().get()
    waitForTopicCreated(topic1)

    val output = TestUtils.grabConsoleOutput(
      topicService.listTopics(new TopicCommandOptions(Array("--exclude-internal"))))

    assertTrue(output.contains(topic1))
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))
  }

  @Test
  def testAlterPartitionCount(): Unit = {
    adminClient.createTopics(
      List(new NewTopic(testTopicName, 2, 2.toShort)).asJavaCollection).all().get()
    waitForTopicCreated(testTopicName)

    topicService.alterTopic(new TopicCommandOptions(
      Array("--topic", testTopicName, "--partitions", "3")))

    val topicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName)).topicNameValues().get(testTopicName).get()
    assertTrue(topicDescription.partitions().size() == 3)
  }

  @Test
  def testAlterAssignment(): Unit = {
    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, 2, 2.toShort))).all().get()
    waitForTopicCreated(testTopicName)

    topicService.alterTopic(new TopicCommandOptions(
      Array("--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2", "--partitions", "3")))

    val topicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName)).topicNameValues().get(testTopicName).get()
    assertTrue(topicDescription.partitions().size() == 3)
    assertEquals(List(4,2), topicDescription.partitions().get(2).replicas().asScala.map(_.id()))
  }

  @Test
  def testAlterAssignmentWithMoreAssignmentThanPartitions(): Unit = {
    adminClient.createTopics(
      List(new NewTopic(testTopicName, 2, 2.toShort)).asJavaCollection).all().get()
    waitForTopicCreated(testTopicName)

    assertThrows(classOf[ExecutionException],
      () => topicService.alterTopic(new TopicCommandOptions(
        Array("--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2,3:2", "--partitions", "3"))))
  }

  @Test
  def testAlterAssignmentWithMorePartitionsThanAssignment(): Unit = {
    adminClient.createTopics(
      List(new NewTopic(testTopicName, 2, 2.toShort)).asJavaCollection).all().get()
    waitForTopicCreated(testTopicName)

    assertThrows(classOf[ExecutionException],
      () => topicService.alterTopic(new TopicCommandOptions(
        Array("--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2", "--partitions", "6"))))
  }

  @Test
  def testAlterWithInvalidPartitionCount(): Unit = {
    createAndWaitTopic(new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName)))

    assertThrows(classOf[ExecutionException],
      () => topicService.alterTopic(new TopicCommandOptions(
        Array("--partitions", "-1", "--topic", testTopicName))))
  }

  @Test
  def testAlterWhenTopicDoesntExist(): Unit = {
    // alter a topic that does not exist without --if-exists
    val alterOpts = new TopicCommandOptions(Array("--topic", testTopicName, "--partitions", "1"))
    val topicService = TopicService(adminClient)
    assertThrows(classOf[IllegalArgumentException], () => topicService.alterTopic(alterOpts))
  }

  @Test
  def testAlterWhenTopicDoesntExistWithIfExists(): Unit = {
    topicService.alterTopic(new TopicCommandOptions(
      Array("--topic", testTopicName, "--partitions", "1", "--if-exists")))
  }

  @Test
  def testCreateAlterTopicWithRackAware(): Unit = {
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 4 -> "rack3", 5 -> "rack3")

    val numPartitions = 18
    val replicationFactor = 3
    val createOpts = new TopicCommandOptions(Array(
      "--partitions", numPartitions.toString,
      "--replication-factor", replicationFactor.toString,
      "--topic", testTopicName))
    createAndWaitTopic(createOpts)

    var assignment = zkClient.getReplicaAssignmentForTopics(Set(testTopicName)).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, numPartitions, replicationFactor)

    val alteredNumPartitions = 36
    // verify that adding partitions will also be rack aware
    val alterOpts = new TopicCommandOptions(Array(
      "--partitions", alteredNumPartitions.toString,
      "--topic", testTopicName))
    topicService.alterTopic(alterOpts)
    assignment = zkClient.getReplicaAssignmentForTopics(Set(testTopicName)).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, alteredNumPartitions, replicationFactor)
  }

  @Test
  def testConfigPreservationAcrossPartitionAlteration(): Unit = {
    val numPartitionsOriginal = 1
    val cleanupKey = "cleanup.policy"
    val cleanupVal = "compact"

    // create the topic
    val createOpts = new TopicCommandOptions(Array(
      "--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--config", cleanupKey + "=" + cleanupVal,
      "--topic", testTopicName))
    createAndWaitTopic(createOpts)
    val props = adminZkClient.fetchEntityConfig(ConfigType.Topic, testTopicName)
    assertTrue(props.containsKey(cleanupKey), "Properties after creation don't contain " + cleanupKey)
    assertTrue(props.getProperty(cleanupKey).equals(cleanupVal), "Properties after creation have incorrect value")

    // pre-create the topic config changes path to avoid a NoNodeException
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)

    // modify the topic to add new partitions
    val numPartitionsModified = 3
    val alterOpts = new TopicCommandOptions(
      Array("--partitions", numPartitionsModified.toString, "--topic", testTopicName))
    topicService.alterTopic(alterOpts)
    val newProps = adminZkClient.fetchEntityConfig(ConfigType.Topic, testTopicName)
    assertTrue(newProps.containsKey(cleanupKey), "Updated properties do not contain " + cleanupKey)
    assertTrue(newProps.getProperty(cleanupKey).equals(cleanupVal), "Updated properties have incorrect value")
  }

  @Test
  def testTopicDeletion(): Unit = {
    // create the NormalTopic
    val createOpts = new TopicCommandOptions(Array("--partitions", "1",
      "--replication-factor", "1",
      "--topic", testTopicName))
    createAndWaitTopic(createOpts)

    // delete the NormalTopic
    val deleteOpts = new TopicCommandOptions(Array("--topic", testTopicName))

    val deletePath = DeleteTopicsTopicZNode.path(testTopicName)
    assertFalse(zkClient.pathExists(deletePath), "Delete path for topic shouldn't exist before deletion.")
    topicService.deleteTopic(deleteOpts)
    TestUtils.verifyTopicDeletion(zkClient, testTopicName, 1, servers)
  }

  @Test
  def testDeleteInternalTopic(): Unit = {
    // create the offset topic
    val createOffsetTopicOpts = new TopicCommandOptions(Array("--partitions", "1",
      "--replication-factor", "1",
      "--topic", Topic.GROUP_METADATA_TOPIC_NAME))
    createAndWaitTopic(createOffsetTopicOpts)

    // Try to delete the Topic.GROUP_METADATA_TOPIC_NAME which is allowed by default.
    // This is a difference between the new and the old command as the old one didn't allow internal topic deletion.
    // If deleting internal topics is not desired, ACLS should be used to control it.
    val deleteOffsetTopicOpts = new TopicCommandOptions(
      Array("--topic", Topic.GROUP_METADATA_TOPIC_NAME))
    val deleteOffsetTopicPath = DeleteTopicsTopicZNode.path(Topic.GROUP_METADATA_TOPIC_NAME)
    assertFalse(zkClient.pathExists(deleteOffsetTopicPath), "Delete path for topic shouldn't exist before deletion.")
    topicService.deleteTopic(deleteOffsetTopicOpts)
    TestUtils.verifyTopicDeletion(zkClient, Topic.GROUP_METADATA_TOPIC_NAME, 1, servers)
  }

  @Test
  def testDeleteWhenTopicDoesntExist(): Unit = {
    // delete a topic that does not exist
    val deleteOpts = new TopicCommandOptions(Array("--topic", testTopicName))
    assertThrows(classOf[IllegalArgumentException], () => topicService.deleteTopic(deleteOpts))
  }

  @Test
  def testDeleteWhenTopicDoesntExistWithIfExists(): Unit = {
    topicService.deleteTopic(new TopicCommandOptions(Array("--topic", testTopicName, "--if-exists")))
  }

  @Test
  def testDescribe(): Unit = {
    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, 2, 2.toShort))).all().get()
    waitForTopicCreated(testTopicName)

    val output = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array("--topic", testTopicName))))
    val rows = output.split("\n")
    assertEquals(3, rows.size)
    assertTrue(rows(0).startsWith(s"Topic: $testTopicName"))
  }

  @Test
  def testDescribeWhenTopicDoesntExist(): Unit = {
    assertThrows(classOf[IllegalArgumentException],
      () => topicService.describeTopic(new TopicCommandOptions(Array("--topic", testTopicName))))
  }

  @Test
  def testDescribeWhenTopicDoesntExistWithIfExists(): Unit = {
    topicService.describeTopic(new TopicCommandOptions(Array("--topic", testTopicName, "--if-exists")))
  }

  @Test
  def testDescribeUnavailablePartitions(): Unit = {
    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, 6, 1.toShort))).all().get()
    waitForTopicCreated(testTopicName)

    try {
      // check which partition is on broker 0 which we'll kill
      val testTopicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName))
        .allTopicNames().get().asScala(testTopicName)
      val partitionOnBroker0 = testTopicDescription.partitions().asScala.find(_.leader().id() == 0).get.partition()

      killBroker(0)

      // wait until the topic metadata for the test topic is propagated to each alive broker
      TestUtils.waitUntilTrue(() => {
        servers
          .filterNot(_.config.brokerId == 0)
          .foldLeft(true) {
            (result, server) => {
              val topicMetadatas = server.dataPlaneRequestProcessor.metadataCache
                .getTopicMetadata(Set(testTopicName), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
              val testPartitionMetadata = topicMetadatas.find(_.name.equals(testTopicName)).get.partitions.asScala.find(_.partitionIndex == partitionOnBroker0)
              testPartitionMetadata match {
                case None => throw new AssertionError(s"Partition metadata is not found in metadata cache")
                case Some(metadata) => result && metadata.errorCode == Errors.LEADER_NOT_AVAILABLE.code
              }
            }
          }
      }, s"Partition metadata for $testTopicName is not propagated")

      // grab the console output and assert
      val output = TestUtils.grabConsoleOutput(
          topicService.describeTopic(new TopicCommandOptions(
            Array("--topic", testTopicName, "--unavailable-partitions"))))
      val rows = output.split("\n")
      assertTrue(rows(0).startsWith(s"\tTopic: $testTopicName"))
      assertTrue(rows(0).contains("Leader: none\tReplicas: 0\tIsr:"))
    } finally {
      restartDeadBrokers()
    }
  }

  @Test
  def testDescribeUnderReplicatedPartitions(): Unit = {
    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, 1, 6.toShort))).all().get()
    waitForTopicCreated(testTopicName)

    try {
      killBroker(0)
      val aliveServers = servers.filterNot(_.config.brokerId == 0)
      TestUtils.waitForPartitionMetadata(aliveServers, testTopicName, 0)
      val output = TestUtils.grabConsoleOutput(
        topicService.describeTopic(new TopicCommandOptions(Array("--under-replicated-partitions"))))
      val rows = output.split("\n")
      assertTrue(rows(0).startsWith(s"\tTopic: $testTopicName"))
    } finally {
      restartDeadBrokers()
    }
  }

  @Test
  def testDescribeUnderMinIsrPartitions(): Unit = {
    val configMap = new java.util.HashMap[String, String]()
    configMap.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "6")

    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, 1, 6.toShort).configs(configMap))).all().get()
    waitForTopicCreated(testTopicName)

    try {
      killBroker(0)
      val aliveServers = servers.filterNot(_.config.brokerId == 0)
      TestUtils.waitForPartitionMetadata(aliveServers, testTopicName, 0)
      val output = TestUtils.grabConsoleOutput(
        topicService.describeTopic(new TopicCommandOptions(Array("--under-min-isr-partitions"))))
      val rows = output.split("\n")
      assertTrue(rows(0).startsWith(s"\tTopic: $testTopicName"))
    } finally {
      restartDeadBrokers()
    }
  }

  @Test
  def testDescribeUnderReplicatedPartitionsWhenReassignmentIsInProgress(): Unit = {
    val configMap = new java.util.HashMap[String, String]()
    val replicationFactor: Short = 1
    val partitions = 1
    val tp = new TopicPartition(testTopicName, 0)

    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, partitions, replicationFactor).configs(configMap))).all().get()
    waitForTopicCreated(testTopicName)

    // Produce multiple batches.
    TestUtils.generateAndProduceMessages(servers, testTopicName, numMessages = 10, acks = -1)
    TestUtils.generateAndProduceMessages(servers, testTopicName, numMessages = 10, acks = -1)

    // Enable throttling. Note the broker config sets the replica max fetch bytes to `1` upon to minimize replication
    // throughput so the reassignment doesn't complete quickly.
    val brokerIds = servers.map(_.config.brokerId)
    TestUtils.setReplicationThrottleForPartitions(adminClient, brokerIds, Set(tp), throttleBytes = 1)

    val testTopicDesc = adminClient.describeTopics(Collections.singleton(testTopicName)).allTopicNames().get().get(testTopicName)
    val firstPartition = testTopicDesc.partitions().asScala.head

    val replicasOfFirstPartition = firstPartition.replicas().asScala.map(_.id())
    val targetReplica = brokerIds.diff(replicasOfFirstPartition).head

    adminClient.alterPartitionReassignments(Collections.singletonMap(tp,
      Optional.of(new NewPartitionReassignment(Collections.singletonList(targetReplica))))).all().get()

    // let's wait until the LAIR is propagated
    TestUtils.waitUntilTrue(() => {
      val reassignments = adminClient.listPartitionReassignments(Collections.singleton(tp)).reassignments().get()
      !reassignments.get(tp).addingReplicas().isEmpty
    }, "Reassignment didn't add the second node")

    // describe the topic and test if it's under-replicated
    val simpleDescribeOutput = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array("--topic", testTopicName))))
    val simpleDescribeOutputRows = simpleDescribeOutput.split("\n")
    assertTrue(simpleDescribeOutputRows(0).startsWith(s"Topic: $testTopicName"))
    assertEquals(2, simpleDescribeOutputRows.size)

    val underReplicatedOutput = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array("--under-replicated-partitions"))))
    assertEquals("", underReplicatedOutput, s"--under-replicated-partitions shouldn't return anything: '$underReplicatedOutput'")

    // Verify reassignment is still ongoing.
    val reassignments = adminClient.listPartitionReassignments(Collections.singleton(tp)).reassignments.get().get(tp)
    assertFalse(Option(reassignments).forall(_.addingReplicas.isEmpty))

    TestUtils.removeReplicationThrottleForPartitions(adminClient, brokerIds, Set(tp))
    TestUtils.waitForAllReassignmentsToComplete(adminClient)
  }

  @Test
  def testDescribeAtMinIsrPartitions(): Unit = {
    val configMap = new java.util.HashMap[String, String]()
    configMap.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4")

    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, 1, 6.toShort).configs(configMap))).all().get()
    waitForTopicCreated(testTopicName)

    try {
      killBroker(0)
      killBroker(1)
      val output = TestUtils.grabConsoleOutput(
        topicService.describeTopic(new TopicCommandOptions(Array("--at-min-isr-partitions"))))
      val rows = output.split("\n")
      assertTrue(rows(0).startsWith(s"\tTopic: $testTopicName"))
      assertEquals(1, rows.length)
    } finally {
      restartDeadBrokers()
    }
  }

  /**
    * Test describe --under-min-isr-partitions option with four topics:
    *   (1) topic with partition under the configured min ISR count
    *   (2) topic with under-replicated partition (but not under min ISR count)
    *   (3) topic with offline partition
    *   (4) topic with fully replicated partition
    *
    * Output should only display the (1) topic with partition under min ISR count and (3) topic with offline partition
    */
  @Test
  def testDescribeUnderMinIsrPartitionsMixed(): Unit = {
    val underMinIsrTopic = "under-min-isr-topic"
    val notUnderMinIsrTopic = "not-under-min-isr-topic"
    val offlineTopic = "offline-topic"
    val fullyReplicatedTopic = "fully-replicated-topic"

    val configMap = new java.util.HashMap[String, String]()
    configMap.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "6")

    adminClient.createTopics(
      java.util.Arrays.asList(
        new NewTopic(underMinIsrTopic, 1, 6.toShort).configs(configMap),
        new NewTopic(notUnderMinIsrTopic, 1, 6.toShort),
        new NewTopic(offlineTopic, Collections.singletonMap(0, Collections.singletonList(0))),
        new NewTopic(fullyReplicatedTopic, Collections.singletonMap(0, java.util.Arrays.asList(1, 2, 3))))).all().get()

    waitForTopicCreated(underMinIsrTopic)
    waitForTopicCreated(notUnderMinIsrTopic)
    waitForTopicCreated(offlineTopic)
    waitForTopicCreated(fullyReplicatedTopic)

    try {
      killBroker(0)
      val aliveServers = servers.filterNot(_.config.brokerId == 0)
      TestUtils.waitForPartitionMetadata(aliveServers, underMinIsrTopic, 0)
      val output = TestUtils.grabConsoleOutput(
        topicService.describeTopic(new TopicCommandOptions(Array("--under-min-isr-partitions"))))
      val rows = output.split("\n")
      assertTrue(rows(0).startsWith(s"\tTopic: $underMinIsrTopic"))
      assertTrue(rows(1).startsWith(s"\tTopic: $offlineTopic"))
      assertEquals(2, rows.length)
    } finally {
      restartDeadBrokers()
    }
  }

  @Test
  def testDescribeReportOverriddenConfigs(): Unit = {
    val config = "file.delete.delay.ms=1000"
    createAndWaitTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "2", "--topic", testTopicName, "--config", config)))
    val output = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array())))
    assertTrue(output.contains(config), s"Describe output should have contained $config")
  }

  @Test
  def testDescribeAndListTopicsWithoutInternalTopics(): Unit = {
    createAndWaitTopic(
      new TopicCommandOptions(Array("--partitions", "1", "--replication-factor", "1", "--topic", testTopicName)))
    // create a internal topic
    createAndWaitTopic(
      new TopicCommandOptions(Array("--partitions", "1", "--replication-factor", "1", "--topic", Topic.GROUP_METADATA_TOPIC_NAME)))

    // test describe
    var output = TestUtils.grabConsoleOutput(topicService.describeTopic(new TopicCommandOptions(
      Array("--describe", "--exclude-internal"))))
    assertTrue(output.contains(testTopicName), s"Output should have contained $testTopicName")
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))

    // test list
    output = TestUtils.grabConsoleOutput(topicService.listTopics(new TopicCommandOptions(Array("--list", "--exclude-internal"))))
    assertTrue(output.contains(testTopicName))
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))
  }

  @Test
  def testDescribeDoesNotFailWhenListingReassignmentIsUnauthorized(): Unit = {
    adminClient = spy(adminClient)
    topicService = TopicService(adminClient)

    val result = AdminClientTestUtils.listPartitionReassignmentsResult(
      new ClusterAuthorizationException("Unauthorized"))

    // Passing `null` here to help the compiler disambiguate the `doReturn` methods,
    // compilation for scala 2.12 fails otherwise.
    doReturn(result, null).when(adminClient).listPartitionReassignments(
      Set(new TopicPartition(testTopicName, 0)).asJava
    )

    adminClient.createTopics(
      Collections.singletonList(new NewTopic(testTopicName, 1, 1.toShort))
    ).all().get()
    waitForTopicCreated(testTopicName)

    val output = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array("--topic", testTopicName))))
    val rows = output.split("\n")
    assertEquals(2, rows.size)
    assertTrue(rows(0).startsWith(s"Topic: $testTopicName"))
  }

  @Test
  def testCreateTopicDoesNotRetryThrottlingQuotaExceededException(): Unit = {
    val adminClient = mock(classOf[Admin])
    val topicService = TopicService(adminClient)

    val result = AdminClientTestUtils.createTopicsResult(testTopicName, Errors.THROTTLING_QUOTA_EXCEEDED.exception())
    when(adminClient.createTopics(any(), any())).thenReturn(result)

    assertThrows(classOf[ThrottlingQuotaExceededException],
      () => topicService.createTopic(new TopicCommandOptions(Array("--topic", testTopicName))))

    val expectedNewTopic = new NewTopic(testTopicName, Optional.empty[Integer](), Optional.empty[java.lang.Short]())
      .configs(Map.empty[String, String].asJava)

    verify(adminClient, times(1)).createTopics(
      eqThat(Set(expectedNewTopic).asJava),
      argThat((_.shouldRetryOnQuotaViolation() == false): ArgumentMatcher[CreateTopicsOptions])
    )
  }

  @Test
  def testDeleteTopicDoesNotRetryThrottlingQuotaExceededException(): Unit = {
    val adminClient = mock(classOf[Admin])
    val topicService = TopicService(adminClient)

    val listResult = AdminClientTestUtils.listTopicsResult(testTopicName)
    when(adminClient.listTopics(any())).thenReturn(listResult)

    val result = AdminClientTestUtils.deleteTopicsResult(testTopicName, Errors.THROTTLING_QUOTA_EXCEEDED.exception())
    when(adminClient.deleteTopics(any[Collection[String]](), any())).thenReturn(result)

    val exception = assertThrows(classOf[ExecutionException],
      () => topicService.deleteTopic(new TopicCommandOptions(Array("--topic", testTopicName))))
    assertTrue(exception.getCause.isInstanceOf[ThrottlingQuotaExceededException])

    verify(adminClient, times(1)).deleteTopics(
      eqThat(Seq(testTopicName).asJavaCollection),
      argThat((_.shouldRetryOnQuotaViolation() == false): ArgumentMatcher[DeleteTopicsOptions])
    )
  }

  @Test
  def testCreatePartitionsDoesNotRetryThrottlingQuotaExceededException(): Unit = {
    val adminClient = mock(classOf[Admin])
    val topicService = TopicService(adminClient)

    val listResult = AdminClientTestUtils.listTopicsResult(testTopicName)
    when(adminClient.listTopics(any())).thenReturn(listResult)

    val topicPartitionInfo = new TopicPartitionInfo(0, new Node(0, "", 0),
      Collections.emptyList(), Collections.emptyList())
    val describeResult = AdminClientTestUtils.describeTopicsResult(testTopicName, new TopicDescription(
      testTopicName, false, Collections.singletonList(topicPartitionInfo)))
    when(adminClient.describeTopics(any(classOf[java.util.Collection[String]]))).thenReturn(describeResult)

    val result = AdminClientTestUtils.createPartitionsResult(testTopicName, Errors.THROTTLING_QUOTA_EXCEEDED.exception())
    when(adminClient.createPartitions(any(), any())).thenReturn(result)

    val exception = assertThrows(classOf[ExecutionException],
      () => topicService.alterTopic(new TopicCommandOptions(Array("--topic", testTopicName, "--partitions", "3"))))
    assertTrue(exception.getCause.isInstanceOf[ThrottlingQuotaExceededException])

    verify(adminClient, times(1)).createPartitions(
      argThat((_.get(testTopicName).totalCount() == 3): ArgumentMatcher[java.util.Map[String, NewPartitions]]),
      argThat((_.shouldRetryOnQuotaViolation() == false): ArgumentMatcher[CreatePartitionsOptions])
    )
  }
}
