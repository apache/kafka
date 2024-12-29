/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.util.Properties
import java.util.concurrent.ExecutionException
import kafka.utils.Logging
import kafka.utils.TestUtils._
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, CreateTopicsOptions, CreateTopicsResult, DescribeClusterOptions, DescribeTopicsOptions, NewTopic, TopicDescription}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.errors.{TopicExistsException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.security.authorizer.AclEntry
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.apache.kafka.server.config.{ReplicationConfigs, ServerConfigs, ServerLogConfigs}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.jdk.CollectionConverters._
import scala.collection.Seq
import scala.jdk.OptionConverters.RichOption

/**
 * Base integration test cases for [[Admin]]. Each test case added here will be executed
 * in extending classes. Typically we prefer to write basic Admin functionality test cases in
 * [[kafka.api.PlaintextAdminIntegrationTest]] rather than here to avoid unnecessary execution
 * time to the build. However, if an admin API involves differing interactions with
 * authentication/authorization layers, we may add the test case here.
 */
@Timeout(120)
abstract class BaseAdminIntegrationTest extends IntegrationTestHarness with Logging {
  def brokerCount = 3
  override def logDirCount = 2

  var testInfo: TestInfo = _

  var client: Admin = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    this.testInfo = testInfo
    super.setUp(testInfo)
    waitUntilBrokerMetadataIsPropagated(brokers)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (client != null)
      Utils.closeQuietly(client, "AdminClient")
    super.tearDown()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testCreateDeleteTopics(quorum: String): Unit = {
    client = createAdminClient
    val topics = Seq("mytopic", "mytopic2", "mytopic3")
    val newTopics = Seq(
      new NewTopic("mytopic", Map((0: Integer) -> Seq[Integer](1, 2).asJava, (1: Integer) -> Seq[Integer](2, 0).asJava).asJava),
      new NewTopic("mytopic2", 3, 3.toShort),
      new NewTopic("mytopic3", Option.empty[Integer].toJava, Option.empty[java.lang.Short].toJava)
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
    val topicIds = getTopicIds()
    topics.foreach { topic =>
      assertNotEquals(Uuid.ZERO_UUID, createResult.topicId(topic).get())
      assertEquals(topicIds(topic), createResult.topicId(topic).get())
    }

    val failedCreateResult = client.createTopics(newTopics.asJava)
    val results = failedCreateResult.values()
    assertTrue(results.containsKey("mytopic"))
    assertFutureThrows(results.get("mytopic"), classOf[TopicExistsException])
    assertTrue(results.containsKey("mytopic2"))
    assertFutureThrows(results.get("mytopic2"), classOf[TopicExistsException])
    assertTrue(results.containsKey("mytopic3"))
    assertFutureThrows(results.get("mytopic3"), classOf[TopicExistsException])
    assertFutureThrows(failedCreateResult.numPartitions("mytopic3"), classOf[TopicExistsException])
    assertFutureThrows(failedCreateResult.replicationFactor("mytopic3"), classOf[TopicExistsException])
    assertFutureThrows(failedCreateResult.config("mytopic3"), classOf[TopicExistsException])

    val topicToDescription = client.describeTopics(topics.asJava).allTopicNames.get()
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
      partition.replicas.forEach { replica =>
        assertTrue(replica.id >= 0)
        assertTrue(replica.id < brokerCount)
      }
      assertEquals(partition.replicas.size, partition.replicas.asScala.map(_.id).distinct.size, "No duplicate replica ids")

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

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAuthorizedOperations(quorum: String): Unit = {
    client = createAdminClient

    // without includeAuthorizedOperations flag
    var result = client.describeCluster
    assertNull(result.authorizedOperations().get())

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
    assertNull(topicResult.authorizedOperations)

    //with includeAuthorizedOperations flag
    topicResult = getTopicMetadata(client, topic, new DescribeTopicsOptions().includeAuthorizedOperations(true))
    expectedOperations = AclEntry.supportedOperations(ResourceType.TOPIC)
    assertEquals(expectedOperations, topicResult.authorizedOperations)
  }

  def configuredClusterPermissions: Set[AclOperation] =
    AclEntry.supportedOperations(ResourceType.CLUSTER).asScala.toSet

  override def modifyConfigs(configs: Seq[Properties]): Unit = {
    super.modifyConfigs(configs)
    // For testCreateTopicsReturnsConfigs, set some static broker configurations so that we can
    // verify that they show up in the "configs" output of CreateTopics.
    if (testInfo.getTestMethod.toString.contains("testCreateTopicsReturnsConfigs")) {
      configs.foreach(config => {
        config.setProperty(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, "2")
        config.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, "240")
        config.setProperty(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG, "123")
      })
    }
    configs.foreach { config =>
      config.setProperty(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, "true")
      config.setProperty(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0")
      config.setProperty(ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG, "false")
      config.setProperty(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "false")
      // We set this in order to test that we don't expose sensitive data via describe configs. This will already be
      // set for subclasses with security enabled and we don't want to overwrite it.
      if (!config.containsKey(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG))
        config.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "some.invalid.pass")
    }
  }

  override def kraftControllerConfigs(testInfo: TestInfo): Seq[Properties] = {
    val controllerConfig = new Properties()
    val controllerConfigs = Seq(controllerConfig)
    modifyConfigs(controllerConfigs)
    controllerConfigs
  }

  def createConfig: util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
    config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "40000")
    config
  }

  def createAdminClient: Admin = {
    val props = new Properties()
    props.putAll(createConfig)
    val client = createAdminClient(configOverrides = props)
    client
  }

  def waitForTopics(client: Admin, expectedPresent: Seq[String], expectedMissing: Seq[String]): Unit = {
    waitUntilTrue(() => {
      val topics = client.listTopics.names.get()
      expectedPresent.forall(topicName => topics.contains(topicName)) &&
        expectedMissing.forall(topicName => !topics.contains(topicName))
    }, "timed out waiting for topics")
  }

  def getTopicMetadata(client: Admin,
                       topic: String,
                       describeOptions: DescribeTopicsOptions = new DescribeTopicsOptions,
                       expectedNumPartitionsOpt: Option[Int] = None): TopicDescription = {
    var result: TopicDescription = null
    waitUntilTrue(() => {
      val topicResult = client.describeTopics(Set(topic).asJava, describeOptions).topicNameValues().get(topic)
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
