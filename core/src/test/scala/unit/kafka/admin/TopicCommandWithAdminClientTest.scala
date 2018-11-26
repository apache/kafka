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
package unit.kafka.admin

import java.util.{Collections, Properties}

import kafka.admin.TopicCommand.{AdminClientTopicService, TopicCommandOptions}
import kafka.admin.{AdminOperationException, RackAwareTest, TopicCommand}
import kafka.common.AdminCommandFailedException
import kafka.integration.KafkaServerTestHarness
import kafka.server.{ConfigType, KafkaConfig}
import kafka.utils.ZkUtils.getDeleteTopicPath
import kafka.utils.{Exit, Logging, TestUtils}
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.config.{ConfigException, ConfigResource}
import org.apache.kafka.common.internals.Topic
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionException

class TopicCommandWithAdminClientTest extends KafkaServerTestHarness with Logging with RackAwareTest {

  /**
    * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
    * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
    */
  override def generateConfigs: Seq[KafkaConfig] = TestUtils.createBrokerConfigs(
    numConfigs = 6,
    zkConnect = zkConnect,
    rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 4 -> "rack3", 5 -> "rack3"
    )).map(KafkaConfig.fromProps)

  private var topicService: AdminClientTopicService = _
  private var adminClient: AdminClient = _

  def assertExitCode(expected: Int, method: () => Unit) {
    def mockExitProcedure(exitCode: Int, exitMessage: Option[String]): Nothing = {
      assertEquals(1, exitCode)
      throw new RuntimeException
    }
    Exit.setExitProcedure(mockExitProcedure)
    try {
      intercept[RuntimeException] {
        method()
      }
    } finally {
      Exit.resetExitProcedure()
    }
  }

  def assertCheckArgsExitCode(expected: Int, options: TopicCommandOptions) {
    assertExitCode(1, options.checkArgs)
  }

  @Before
  def setup() {
    // create adminClient
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    adminClient = AdminClient.create(props)
    topicService = AdminClientTopicService(adminClient)
  }

  @After
  def close(): Unit = {
    // adminClient is closed by topicService
    if (topicService != null)
      topicService.close()
  }

  @Test
  def testParseAssignment(): Unit = {
    val actualAssignment = TopicCommand.parseReplicaAssignment("5:4,3:2,1:0")
    val expectedAssignment = Map(0 -> List(5, 4), 1 -> List(3, 2), 2 -> List(1, 0))
    assertEquals(expectedAssignment, actualAssignment)
  }

  @Test
  def testParseAssignmentDuplicateEntries(): Unit = {
    intercept[AdminCommandFailedException] {
      TopicCommand.parseReplicaAssignment("5:5")
    }
  }

  @Test
  def testParseAssignmentPartitionsOfDifferentSize(): Unit = {
    intercept[AdminOperationException] {
      TopicCommand.parseReplicaAssignment("5:4:3,2:1")
    }
  }

  @Test
  def testInvalidConfigOptWithBootstrapServers(): Unit = {
    assertCheckArgsExitCode(1,
      new TopicCommandOptions(Array("--bootstrap-server", brokerList ,"--alter", "--topic", "test", "--config", "cleanup.policy=compact")))
    assertCheckArgsExitCode(1,
      new TopicCommandOptions(Array("--bootstrap-server", brokerList ,"--alter", "--topic", "test", "--delete-config", "cleanup.policy")))
  }

  @Test
  def testCreate(): Unit = {
    val topic = "test"

    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "1", "--topic", topic)))

    adminClient.listTopics().names().get().contains(topic)
  }

  @Test
  def testCreateWithConfigs(): Unit = {
    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, "testTopic")
    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "2", "--topic", configResource.name(), "--config", "delete.retention.ms=1000")))

    val configs = adminClient
      .describeConfigs(Collections.singleton(configResource))
      .all().get().get(configResource)
    assertEquals(1000, Integer.valueOf(configs.get("delete.retention.ms").value()))
  }

  @Test
  def testCreateIfNotExists(): Unit = {
    val topic = "test"
    val numPartitions = 1

    // create the topic
    val createOpts = new TopicCommandOptions(
      Array("--partitions", numPartitions.toString, "--replication-factor", "1", "--topic", topic))
    val topicService = AdminClientTopicService(adminClient)
    topicService.createTopic(createOpts)

    // try to re-create the topic without --if-not-exists
    intercept[ExecutionException] {
      topicService.createTopic(createOpts)
    }

    // try to re-create the topic with --if-not-exists
    val createNotExistsOpts = new TopicCommandOptions(
      Array("--partitions", numPartitions.toString, "--replication-factor", "1", "--topic", topic, "--if-not-exists"))
    topicService.createTopic(createNotExistsOpts)

    // try to create a new topic with --if-not-exists
    val createNewNotExistsOpts = new TopicCommandOptions(
      Array("--partitions", numPartitions.toString, "--replication-factor", "1", "--topic", "test2", "--if-not-exists"))
    topicService.createTopic(createNewNotExistsOpts)

    adminClient.listTopics().names().get().contains("test2")
  }

  @Test
  def testCreateWithReplicaAssignment(): Unit = {
    val topic = "test"

    // create the topic
    val createOpts = new TopicCommandOptions(
      Array("--replica-assignment", "5:4,3:2,1:0", "--topic", topic))
    topicService.createTopic(createOpts)

    val partitions = adminClient
      .describeTopics(Collections.singletonList(topic))
      .all()
      .get()
      .get(topic)
      .partitions()
    assertEquals(3, partitions.size())
    assertEquals(List(5, 4), partitions.get(0).replicas().asScala.map(_.id()))
    assertEquals(List(3, 2), partitions.get(1).replicas().asScala.map(_.id()))
    assertEquals(List(1, 0), partitions.get(2).replicas().asScala.map(_.id()))
  }

  @Test
  def testCreateWithInvalidReplicationFactor() {
    intercept[IllegalArgumentException] {
      topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "2", "--replication-factor", (Short.MaxValue+1).toString, "--topic", "testTopic")))
    }
  }

  @Test
  def testCreateWithNegativeReplicationFactor(): Unit = {
    intercept[ExecutionException] {
      topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "2", "--replication-factor", "-1", "--topic", "testTopic")))
    }
  }

  @Test
  def testCreateWithAssignmentAndPartitionCount(): Unit = {
    assertCheckArgsExitCode(1,
      new TopicCommandOptions(
        Array("--bootstrap-server", brokerList,
          "--create",
          "--replica-assignment", "3:0,5:1",
          "--partitions", "2",
          "--topic", "testTopic")))
  }

  @Test
  def testCreateWithAssignmentAndReplicationFactor(): Unit = {
    assertCheckArgsExitCode(1,
      new TopicCommandOptions(
        Array("--bootstrap-server", brokerList,
          "--create",
          "--replica-assignment", "3:0,5:1",
          "--replication-factor", "2",
          "--topic", "testTopic")))
  }

  @Test
  def testCreateWithNegativePartitionCount(): Unit = {
    intercept[ExecutionException] {
      topicService.createTopic(new TopicCommandOptions(
        Array("--partitions", "-1", "--replication-factor", "1", "--topic", "testTopic")))
    }
  }

  @Test
  def testCreateWithUnspecifiedPartitionCount(): Unit = {
    assertExitCode(1,
      () => topicService.createTopic(new TopicCommandOptions(
        Array("--replication-factor", "1", "--topic", "testTopic"))))
  }

  @Test
  def testInvalidTopicLevelConfig(): Unit = {
    val createOpts = new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", "test",
        "--config", "message.timestamp.type=boom"))
    intercept[ConfigException] {
      topicService.createTopic(createOpts)
    }
  }

  @Test
  def testListTopics(): Unit = {
    val topic = "testTopic"
    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", topic)))

    val output = TestUtils.grabConsoleOutput(
      topicService.listTopics(new TopicCommandOptions(Array())))

    assertTrue(output.contains(topic))
  }

  @Test
  def testListTopicsWithWhitelist(): Unit = {
    val topic1 = "kafka.testTopic1"
    val topic2 = "kafka.testTopic2"
    val topic3 = "oooof.testTopic1"
    adminClient.createTopics(
      List(new NewTopic(topic1, 2, 2),
        new NewTopic(topic2, 2, 2),
        new NewTopic(topic3, 2, 2)).asJavaCollection)
      .all().get()

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
      List(new NewTopic(topic1, 2, 2),
        new NewTopic(Topic.GROUP_METADATA_TOPIC_NAME, 2, 2)).asJavaCollection)
      .all().get()

    val output = TestUtils.grabConsoleOutput(
      topicService.listTopics(new TopicCommandOptions(Array("--exclude-internal"))))

    assertTrue(output.contains(topic1))
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))
  }

  @Test
  def testAlterPartitionCount(): Unit = {
    val topic1 = "testTopic1"

    adminClient.createTopics(
      List(new NewTopic(topic1, 2, 2)).asJavaCollection).all().get()

    topicService.alterTopic(new TopicCommandOptions(
      Array("--topic", topic1, "--partitions", "3")))

    val topicDescription = adminClient.describeTopics(Collections.singletonList(topic1)).values().get(topic1).get()
    assertTrue(topicDescription.partitions().size() == 3)
  }

  @Test
  def testAlterAssignment(): Unit = {
    val topic1 = "testTopic1"

    adminClient.createTopics(
      Collections.singletonList(new NewTopic(topic1, 2, 2))).all().get()

    topicService.alterTopic(new TopicCommandOptions(
      Array("--topic", topic1, "--replica-assignment", "5:3,3:1,4:2", "--partitions", "3")))

    val topicDescription = adminClient.describeTopics(Collections.singletonList(topic1)).values().get(topic1).get()
    assertTrue(topicDescription.partitions().size() == 3)
    assertEquals(List(4,2), topicDescription.partitions().get(2).replicas().asScala.map(_.id()))
  }

  @Test
  def testAlterAssignmentWithMoreAssignmentThanPartitions(): Unit = {
    val topic1 = "testTopic1"

    adminClient.createTopics(
      List(new NewTopic(topic1, 2, 2)).asJavaCollection).all().get()

    intercept[ExecutionException] {
      topicService.alterTopic(new TopicCommandOptions(
        Array("--topic", topic1, "--replica-assignment", "5:3,3:1,4:2,3:2", "--partitions", "3")))
    }
  }

  @Test
  def testAlterAssignmentWithMorePartitionsThanAssignment(): Unit = {
    val topic1 = "testTopic1"

    adminClient.createTopics(
      List(new NewTopic(topic1, 2, 2)).asJavaCollection).all().get()

    intercept[ExecutionException] {
      topicService.alterTopic(new TopicCommandOptions(
        Array("--topic", topic1, "--replica-assignment", "5:3,3:1,4:2", "--partitions", "6")))
    }
  }

  @Test
  def testAlterWithInvalidPartitionCount(): Unit = {
    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "1", "--replication-factor", "1", "--topic", "testTopic")))

    intercept[ExecutionException] {
      topicService.alterTopic(new TopicCommandOptions(
        Array("--partitions", "-1", "--topic", "testTopic")))
    }
  }

  @Test
  def testAlterWithUnspecifiedPartitionCount(): Unit = {
    assertCheckArgsExitCode(1, new TopicCommandOptions(
      Array("--bootstrap-server", brokerList ,"--alter", "--topic", "testTopic")))
  }

  @Test
  def testAlterIfExists(): Unit = {
    // alter a topic that does not exist without --if-exists
    val alterOpts = new TopicCommandOptions(Array("--topic", "test", "--partitions", "1"))
    val topicService = AdminClientTopicService(adminClient)
    intercept[IllegalArgumentException] {
      topicService.alterTopic(alterOpts)
    }

    // alter a topic that does not exist with --if-exists
    val alterExistsOpts = new TopicCommandOptions(Array("--topic", "test", "--partitions", "1", "--if-exists"))
    topicService.alterTopic(alterExistsOpts)
  }

  @Test
  def testCreateAlterTopicWithRackAware(): Unit = {
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 4 -> "rack3", 5 -> "rack3")

    val numPartitions = 18
    val replicationFactor = 3
    val createOpts = new TopicCommandOptions(Array(
      "--partitions", numPartitions.toString,
      "--replication-factor", replicationFactor.toString,
      "--topic", "foo"))
    val topicService = AdminClientTopicService(adminClient)
    topicService.createTopic(createOpts)

    var assignment = zkClient.getReplicaAssignmentForTopics(Set("foo")).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, numPartitions, replicationFactor)

    val alteredNumPartitions = 36
    // verify that adding partitions will also be rack aware
    val alterOpts = new TopicCommandOptions(Array(
      "--partitions", alteredNumPartitions.toString,
      "--topic", "foo"))
    topicService.alterTopic(alterOpts)
    assignment = zkClient.getReplicaAssignmentForTopics(Set("foo")).map { case (tp, replicas) =>
      tp.partition -> replicas
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, alteredNumPartitions, replicationFactor)
  }

  @Test
  def testConfigPreservationAcrossPartitionAlteration(): Unit = {
    val topic = "test"
    val numPartitionsOriginal = 1
    val cleanupKey = "cleanup.policy"
    val cleanupVal = "compact"

    // create the topic
    val createOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--config", cleanupKey + "=" + cleanupVal,
      "--topic", topic))
    topicService.createTopic(createOpts)
    val props = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
    assertTrue("Properties after creation don't contain " + cleanupKey, props.containsKey(cleanupKey))
    assertTrue("Properties after creation have incorrect value", props.getProperty(cleanupKey).equals(cleanupVal))

    // pre-create the topic config changes path to avoid a NoNodeException
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)

    // modify the topic to add new partitions
    val numPartitionsModified = 3
    val alterOpts = new TopicCommandOptions(Array("--partitions", numPartitionsModified.toString, "--topic", topic))
    topicService.alterTopic(alterOpts)
    val newProps = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
    assertTrue("Updated properties do not contain " + cleanupKey, newProps.containsKey(cleanupKey))
    assertTrue("Updated properties have incorrect value", newProps.getProperty(cleanupKey).equals(cleanupVal))
  }

  @Test
  def testTopicDeletion(): Unit = {

    val normalTopic = "test"

    // create the NormalTopic
    val createOpts = new TopicCommandOptions(Array("--partitions", "1",
      "--replication-factor", "1",
      "--topic", normalTopic))
    topicService.createTopic(createOpts)

    // delete the NormalTopic
    val deleteOpts = new TopicCommandOptions(Array("--topic", normalTopic))

    val deletePath = getDeleteTopicPath(normalTopic)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkClient.pathExists(deletePath))
    topicService.deleteTopic(deleteOpts)
    assertTrue("Delete path for topic should exist after deletion.", zkClient.pathExists(deletePath))
  }

  @Test
  def testDeleteInternalTopic(): Unit = {
    // create the offset topic
    val createOffsetTopicOpts = new TopicCommandOptions(Array("--partitions", "1",
      "--replication-factor", "1",
      "--topic", Topic.GROUP_METADATA_TOPIC_NAME))
    topicService.createTopic(createOffsetTopicOpts)

    // Try to delete the Topic.GROUP_METADATA_TOPIC_NAME which is allowed by default.
    // This is a difference between the new and the old command as the old one didn't allow internal topic deletion.
    // If deleting internal topics is not desired, ACLS should be used to control it.
    val deleteOffsetTopicOpts = new TopicCommandOptions(Array("--topic", Topic.GROUP_METADATA_TOPIC_NAME))
    val deleteOffsetTopicPath = getDeleteTopicPath(Topic.GROUP_METADATA_TOPIC_NAME)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkClient.pathExists(deleteOffsetTopicPath))
    topicService.deleteTopic(deleteOffsetTopicOpts)
    assertTrue("Delete path for topic should exist after deletion.", zkClient.pathExists(deleteOffsetTopicPath))
  }

  @Test
  def testDeleteIfExists(): Unit = {
    // delete a topic that does not exist without --if-exists
    val deleteOpts = new TopicCommandOptions(Array("--topic", "test"))
    intercept[IllegalArgumentException] {
      topicService.deleteTopic(deleteOpts)
    }

    // delete a topic that does not exist with --if-exists
    val deleteExistsOpts = new TopicCommandOptions(Array("--topic", "test", "--if-exists"))
    topicService.deleteTopic(deleteExistsOpts)

    // delete a topic that is already marked for deletion
    val normalTopic = "test"
    val numPartitionsOriginal = 1
    val createOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--topic", normalTopic))
    topicService.createTopic(createOpts)
    topicService.deleteTopic(deleteOpts)
  }

  @Test
  def testDescribe(): Unit = {
    val topic = "testTopic"
    adminClient.createTopics(
      Collections.singletonList(new NewTopic(topic, 2, 2))).all().get()
    val output = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array("--topic", topic))))
    val rows = output.split("\n")
    assertEquals(3, rows.size)
    rows(0).startsWith("Topic:testTopic\tPartitionCount:2")
  }

  @Test
  def testDescribeUnavailablePartitions(): Unit = {
    val topic = "testTopic"
    adminClient.createTopics(
      Collections.singletonList(new NewTopic(topic, 6, 1))).all().get()

    try {
      killBroker(0)
      val output = TestUtils.grabConsoleOutput(
        topicService.describeTopic(new TopicCommandOptions(Array("--topic", topic, "--unavailable-partitions"))))
      val rows = output.split("\n")
      assertTrue(rows(0).startsWith("\tTopic: testTopic"))
      assertTrue(rows(0).endsWith("Leader: none\tReplicas: 0\tIsr: "))
    } finally {
      restartDeadBrokers()
    }
  }

  @Test
  def testDescribeUnderreplicatedPartitions(): Unit = {
    val topic = "testTopic"
    adminClient.createTopics(
      Collections.singletonList(new NewTopic(topic, 1, 6))).all().get()

    try {
      killBroker(0)
      val output = TestUtils.grabConsoleOutput(
        topicService.describeTopic(new TopicCommandOptions(Array("--under-replicated-partitions"))))
      val rows = output.split("\n")
      assertTrue(rows(0).startsWith("\tTopic: testTopic"))
    } finally {
      restartDeadBrokers()
    }
  }

  @Test
  def testDescribeReportOverriddenConfigs(): Unit = {
    val config = "file.delete.delay.ms=1000"
    val configResource = new ConfigResource(ConfigResource.Type.TOPIC, "testTopic")
    topicService.createTopic(new TopicCommandOptions(
      Array("--partitions", "2", "--replication-factor", "2", "--topic", configResource.name(), "--config", config)))
    val output = TestUtils.grabConsoleOutput(
      topicService.describeTopic(new TopicCommandOptions(Array())))
    assertTrue(output.contains(config))
  }

  @Test
  def testDescribeAndListTopicsWithoutInternalTopics(): Unit = {
    val topic = "testDescribeAndListTopicsWithoutInternalTopics"

    topicService.createTopic(
      new TopicCommandOptions(Array("--partitions", "1", "--replication-factor", "1", "--topic", topic)))
    // create a internal topic
    topicService.createTopic(
      new TopicCommandOptions(Array("--partitions", "1", "--replication-factor", "1", "--topic", Topic.GROUP_METADATA_TOPIC_NAME)))

    // test describe
    var output = TestUtils.grabConsoleOutput(topicService.describeTopic(new TopicCommandOptions(Array("--describe", "--exclude-internal"))))
    assertTrue(output.contains(topic))
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))

    // test list
    output = TestUtils.grabConsoleOutput(topicService.listTopics(new TopicCommandOptions(Array("--list", "--exclude-internal"))))
    assertTrue(output.contains(topic))
    assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME))
  }
}