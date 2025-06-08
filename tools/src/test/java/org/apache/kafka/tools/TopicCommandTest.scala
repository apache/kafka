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
package org.apache.kafka.tools

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientTestUtils
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.CreatePartitionsResult
import org.apache.kafka.clients.admin.CreateTopicsResult
import org.apache.kafka.clients.admin.DeleteTopicsOptions
import org.apache.kafka.clients.admin.DeleteTopicsResult
import org.apache.kafka.clients.admin.DescribeTopicsResult
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult
import org.apache.kafka.clients.admin.ListTopicsResult
import org.apache.kafka.clients.admin.NewPartitionReassignment
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.PartitionReassignment
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.ClusterConfig
import org.apache.kafka.common.test.api.ClusterConfigProperty
import org.apache.kafka.common.test.api.ClusterTemplate
import org.apache.kafka.common.test.api.ClusterTest
import org.apache.kafka.common.test.api.Type
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.metadata.LeaderAndIsr
import org.apache.kafka.storage.internals.log.LogConfig
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util
import java.util.Collections
import java.util.Optional
import java.util.Properties
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Collectors
import java.util.stream.IntStream
import java.util.stream.Stream
import org.apache.kafka.server.config.ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_CONFIG
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyCollection
import org.mockito.ArgumentMatchers.argThat
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.mock
import org.mockito.Mockito.spy
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when

object TopicCommandTest {
  private val CLUSTER_WAIT_MS = 60000

  private[tools] def generate = {
    val serverProp = new util.HashMap[String, String]
    serverProp.put(REPLICA_FETCH_MAX_BYTES_CONFIG, "1") // if config name error, no exception throw
    serverProp.put("log.initial.task.delay.ms", "100")
    serverProp.put("log.segment.delete.delay.ms", "1000")
    val rackInfo = new util.HashMap[Integer, util.Map[String, String]]
    val infoPerBroker1 = new util.HashMap[String, String]
    infoPerBroker1.put("broker.rack", "rack1")
    val infoPerBroker2 = new util.HashMap[String, String]
    infoPerBroker2.put("broker.rack", "rack2")
    val infoPerBroker3 = new util.HashMap[String, String]
    infoPerBroker3.put("broker.rack", "rack2")
    val infoPerBroker4 = new util.HashMap[String, String]
    infoPerBroker4.put("broker.rack", "rack1")
    val infoPerBroker5 = new util.HashMap[String, String]
    infoPerBroker5.put("broker.rack", "rack3")
    val infoPerBroker6 = new util.HashMap[String, String]
    infoPerBroker6.put("broker.rack", "rack3")
    rackInfo.put(0, infoPerBroker1)
    rackInfo.put(1, infoPerBroker2)
    rackInfo.put(2, infoPerBroker3)
    rackInfo.put(3, infoPerBroker4)
    rackInfo.put(4, infoPerBroker5)
    rackInfo.put(5, infoPerBroker6)
    Collections.singletonList(ClusterConfig.defaultBuilder.setBrokers(6).setServerProperties(serverProp).setPerServerProperties(rackInfo).setTypes(Stream.of(Type.KRAFT).collect(Collectors.toSet)).build)
  }

  private def getReplicaDistribution(assignment: util.Map[Integer, util.List[Integer]], brokerRackMapping: util.Map[Integer, String]) = {
    val leaderCount = new util.HashMap[Integer, Integer]
    val partitionCount = new util.HashMap[Integer, Integer]
    val partitionRackMap = new util.HashMap[Integer, util.List[String]]
    assignment.forEach((partitionId: Integer, replicaList: util.List[Integer]) => {
      val leader = replicaList.get(0)
      leaderCount.put(leader, leaderCount.getOrDefault(leader, 0) + 1)
      replicaList.forEach((brokerId: Integer) => {
        partitionCount.put(brokerId, partitionCount.getOrDefault(brokerId, 0) + 1)
        var rack: String = null
        if (brokerRackMapping.containsKey(brokerId)) {
          rack = brokerRackMapping.get(brokerId)
          val partitionRackValues = Stream.of(Collections.singletonList(rack), partitionRackMap.getOrDefault(partitionId, Collections.emptyList)).flatMap(util.List.stream).collect(Collectors.toList)
          partitionRackMap.put(partitionId, partitionRackValues)
        }
        else System.err.printf("No mapping found for %s in `brokerRackMapping`%n", brokerId)
      })
    })
    new TopicCommandTest.ReplicaDistributions(partitionRackMap, leaderCount, partitionCount)
  }

  private class ReplicaDistributions(private val partitionRacks: util.Map[Integer, util.List[String]], private val brokerLeaderCount: util.Map[Integer, Integer], private val brokerReplicasCount: util.Map[Integer, Integer]) {
  }
}

class TopicCommandTest {
  final private val defaultReplicationFactor = 1
  final private val defaultNumPartitions = 1
  final private val bootstrapServer = "localhost:9092"
  final private val topicName = "topicName"

  @Test def testIsNotUnderReplicatedWhenAdding(): Unit = {
    val replicaIds = util.Arrays.asList(1, 2)
    val replicas = new util.ArrayList[Node]
    import scala.collection.JavaConversions._
    for (id <- replicaIds) {
      replicas.add(new Node(id, "localhost", 9090 + id))
    }
    val partitionDescription = new TopicCommand.PartitionDescription("test-topic", new TopicPartitionInfo(0, new Node(1, "localhost", 9091), replicas, Collections.singletonList(new Node(1, "localhost", 9091))), null, false, new PartitionReassignment(replicaIds, util.Arrays.asList(2), Collections.emptyList))
    assertFalse(partitionDescription.isUnderReplicated)
  }

  @Test def testAlterWithUnspecifiedPartitionCount(): Unit = {
    val options = Array[String](" --bootstrap-server", bootstrapServer, "--alter", "--topic", topicName)
    assertInitializeInvalidOptionsExitCode(1, options)
  }

  @Test def testConfigOptWithBootstrapServers(): Unit = {
    assertInitializeInvalidOptionsExitCode(1, Array[String]("--bootstrap-server", bootstrapServer, "--alter", "--topic", topicName, "--partitions", "3", "--config", "cleanup.policy=compact"))
    val opts = new TopicCommand.TopicCommandOptions(Array[String]("--bootstrap-server", bootstrapServer, "--create", "--topic", topicName, "--partitions", "3", "--replication-factor", "3", "--config", "cleanup.policy=compact"))
    assertTrue(opts.hasCreateOption)
    assertEquals(bootstrapServer, opts.bootstrapServer.get)
    assertEquals("cleanup.policy=compact", opts.topicConfig.get.get(0))
  }

  @Test def testCreateWithPartitionCountWithoutReplicationFactorShouldSucceed(): Unit = {
    val opts = new TopicCommand.TopicCommandOptions(Array[String]("--bootstrap-server", bootstrapServer, "--create", "--partitions", "2", "--topic", topicName))
    assertTrue(opts.hasCreateOption)
    assertEquals(topicName, opts.topic.get)
    assertEquals(2, opts.partitions.get)
  }

  @Test def testCreateWithReplicationFactorWithoutPartitionCountShouldSucceed(): Unit = {
    val opts = new TopicCommand.TopicCommandOptions(Array[String]("--bootstrap-server", bootstrapServer, "--create", "--replication-factor", "3", "--topic", topicName))
    assertTrue(opts.hasCreateOption)
    assertEquals(topicName, opts.topic.get)
    assertEquals(3, opts.replicationFactor.get)
  }

  @Test def testCreateWithAssignmentAndPartitionCount(): Unit = {
    assertInitializeInvalidOptionsExitCode(1, Array[String]("--bootstrap-server", bootstrapServer, "--create", "--replica-assignment", "3:0,5:1", "--partitions", "2", "--topic", topicName))
  }

  @Test def testCreateWithAssignmentAndReplicationFactor(): Unit = {
    assertInitializeInvalidOptionsExitCode(1, Array[String]("--bootstrap-server", bootstrapServer, "--create", "--replica-assignment", "3:0,5:1", "--replication-factor", "2", "--topic", topicName))
  }

  @Test def testCreateWithoutPartitionCountAndReplicationFactorShouldSucceed(): Unit = {
    val opts = new TopicCommand.TopicCommandOptions(Array[String]("--bootstrap-server", bootstrapServer, "--create", "--topic", topicName))
    assertTrue(opts.hasCreateOption)
    assertEquals(topicName, opts.topic.get)
    assertFalse(opts.partitions.isPresent)
  }

  @Test def testDescribeShouldSucceed(): Unit = {
    val opts = new TopicCommand.TopicCommandOptions(Array[String]("--bootstrap-server", bootstrapServer, "--describe", "--topic", topicName))
    assertTrue(opts.hasDescribeOption)
    assertEquals(topicName, opts.topic.get)
  }

  @Test def testDescribeWithDescribeTopicsApiShouldSucceed(): Unit = {
    val opts = new TopicCommand.TopicCommandOptions(Array[String]("--bootstrap-server", bootstrapServer, "--describe", "--topic", topicName))
    assertTrue(opts.hasDescribeOption)
    assertEquals(topicName, opts.topic.get)
  }

  @Test def testParseAssignmentDuplicateEntries(): Unit = {
    assertThrows(classOf[AdminCommandFailedException], () => TopicCommand.parseReplicaAssignment("5:5"))
  }

  @Test def testParseAssignmentPartitionsOfDifferentSize(): Unit = {
    assertThrows(classOf[AdminOperationException], () => TopicCommand.parseReplicaAssignment("5:4:3,2:1"))
  }

  @Test def testParseAssignment(): Unit = {
    val actualAssignment = TopicCommand.parseReplicaAssignment("5:4,3:2,1:0")
    val expectedAssignment = new util.HashMap[Integer, util.List[Integer]]
    expectedAssignment.put(0, util.Arrays.asList(5, 4))
    expectedAssignment.put(1, util.Arrays.asList(3, 2))
    expectedAssignment.put(2, util.Arrays.asList(1, 0))
    assertEquals(expectedAssignment, actualAssignment)
  }

  @Test def testCreateTopicDoesNotRetryThrottlingQuotaExceededException(): Unit = {
    val adminClient = mock(classOf[Admin])
    val topicService = new TopicCommand.TopicService(adminClient)
    val result = AdminClientTestUtils.createTopicsResult(topicName, Errors.THROTTLING_QUOTA_EXCEEDED.exception)
    when(adminClient.createTopics(any, any)).thenReturn(result)
    assertThrows(classOf[ThrottlingQuotaExceededException], () => topicService.createTopic(new TopicCommand.TopicCommandOptions(Array[String]("--bootstrap-server", bootstrapServer, "--create", "--topic", topicName))))
    val expectedNewTopic = new NewTopic(topicName, Optional.empty, Optional.empty).configs(Collections.emptyMap)
    verify(adminClient, times(1)).createTopics(eq(Set.of(expectedNewTopic)), argThat((exception: CreateTopicsOptions) => !exception.shouldRetryOnQuotaViolation))
  }

  @Test def testDeleteTopicDoesNotRetryThrottlingQuotaExceededException(): Unit = {
    val adminClient = mock(classOf[Admin])
    val topicService = new TopicCommand.TopicService(adminClient)
    val listResult = AdminClientTestUtils.listTopicsResult(topicName)
    when(adminClient.listTopics(any)).thenReturn(listResult)
    val result = AdminClientTestUtils.deleteTopicsResult(topicName, Errors.THROTTLING_QUOTA_EXCEEDED.exception)
    when(adminClient.deleteTopics(anyCollection, any)).thenReturn(result)
    val exception = assertThrows(classOf[ExecutionException], () => topicService.deleteTopic(new TopicCommand.TopicCommandOptions(Array[String]("--bootstrap-server", bootstrapServer, "--delete", "--topic", topicName))))
    assertInstanceOf(classOf[ThrottlingQuotaExceededException], exception.getCause)
    verify(adminClient).deleteTopics(argThat((topics: util.Collection[String]) => topics == util.Arrays.asList(topicName)), argThat((options: DeleteTopicsOptions) => !options.shouldRetryOnQuotaViolation))
  }

  @Test def testCreatePartitionsDoesNotRetryThrottlingQuotaExceededException(): Unit = {
    val adminClient = mock(classOf[Admin])
    val topicService = new TopicCommand.TopicService(adminClient)
    val listResult = AdminClientTestUtils.listTopicsResult(topicName)
    when(adminClient.listTopics(any)).thenReturn(listResult)
    val topicPartitionInfo = new TopicPartitionInfo(0, new Node(0, "", 0), Collections.emptyList, Collections.emptyList)
    val describeResult = AdminClientTestUtils.describeTopicsResult(topicName, new TopicDescription(topicName, false, Collections.singletonList(topicPartitionInfo)))
    when(adminClient.describeTopics(anyCollection)).thenReturn(describeResult)
    val result = AdminClientTestUtils.createPartitionsResult(topicName, Errors.THROTTLING_QUOTA_EXCEEDED.exception)
    when(adminClient.createPartitions(any, any)).thenReturn(result)
    val exception = assertThrows(classOf[ExecutionException], () => topicService.alterTopic(new TopicCommand.TopicCommandOptions(Array[String]("--alter", "--topic", topicName, "--partitions", "3", "--bootstrap-server", bootstrapServer))))
    assertInstanceOf(classOf[ThrottlingQuotaExceededException], exception.getCause)
    verify(adminClient, times(1)).createPartitions(argThat((newPartitions: util.Map[String, NewPartitions]) => newPartitions.get(topicName).totalCount == 3), argThat((createPartitionOption: CreatePartitionsOptions) => !createPartitionOption.shouldRetryOnQuotaViolation))
  }

  def assertInitializeInvalidOptionsExitCode(expected: Int, options: Array[String]): Unit = {
    Exit.setExitProcedure((exitCode: Int, message: String) => {
      assertEquals(expected, exitCode)
      throw new RuntimeException
    })
    try assertThrows(classOf[RuntimeException], () => new TopicCommand.TopicCommandOptions(options))
    finally Exit.resetExitProcedure()
  }

  private def buildTopicCommandOptionsWithBootstrap(clusterInstance: ClusterInstance, opts: String*) = {
    val bootstrapServer = clusterInstance.bootstrapServers
    val finalOptions = Stream.concat(util.Arrays.stream(opts), Stream.of("--bootstrap-server", bootstrapServer)).toArray(`new`)
    new TopicCommand.TopicCommandOptions(finalOptions)
  }

  @ClusterTest(brokers = 3, serverProperties = Array(Array(new ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"), new ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000"))))
  @throws[InterruptedException]
  @throws[ExecutionException]
  def testCreate(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)))
        clusterInstance.waitForTopic(testTopicName, defaultNumPartitions)
        Assertions.assertTrue(adminClient.listTopics.names.get.contains(testTopicName), "Admin client didn't see the created topic. It saw: " + adminClient.listTopics.names.get)
        adminClient.deleteTopics(Collections.singletonList(testTopicName))
        clusterInstance.waitForTopic(testTopicName, 0)
        Assertions.assertTrue(adminClient.listTopics.names.get.isEmpty, "Admin client see the created topic. It saw: " + adminClient.listTopics.names.get)
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest(brokers = 3, serverProperties = Array(Array(new ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"), new ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000"))))
  @throws[InterruptedException]
  @throws[ExecutionException]
  def testCreateWithDefaults(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)))
        clusterInstance.waitForTopic(testTopicName, defaultNumPartitions)
        Assertions.assertTrue(adminClient.listTopics.names.get.contains(testTopicName), "Admin client didn't see the created topic. It saw: " + adminClient.listTopics.names.get)
        val partitions = adminClient.describeTopics(Collections.singletonList(testTopicName)).allTopicNames.get.get(testTopicName).partitions
        Assertions.assertEquals(defaultNumPartitions, partitions.size, "Unequal partition size: " + partitions.size)
        Assertions.assertEquals(defaultReplicationFactor, partitions.get(0).replicas.size.toShort, "Unequal replication factor: " + partitions.get(0).replicas.size)
        adminClient.deleteTopics(Collections.singletonList(testTopicName))
        clusterInstance.waitForTopic(testTopicName, 0)
        Assertions.assertTrue(adminClient.listTopics.names.get.isEmpty, "Admin client see the created topic. It saw: " + adminClient.listTopics.names.get)
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest(brokers = 3, serverProperties = Array(Array(new ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"), new ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000"))))
  @throws[InterruptedException]
  @throws[ExecutionException]
  def testCreateWithDefaultReplication(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, 2, defaultReplicationFactor)))
        clusterInstance.waitForTopic(testTopicName, 2)
        val partitions = adminClient.describeTopics(Collections.singletonList(testTopicName)).allTopicNames.get.get(testTopicName).partitions
        assertEquals(2, partitions.size, "Unequal partition size: " + partitions.size)
        assertEquals(defaultReplicationFactor, partitions.get(0).replicas.size.toShort, "Unequal replication factor: " + partitions.get(0).replicas.size)
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest(brokers = 3)
  @throws[InterruptedException]
  @throws[ExecutionException]
  def testCreateWithDefaultPartitions(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, 2.toShort)))
        clusterInstance.waitForTopic(testTopicName, defaultNumPartitions)
        val partitions = adminClient.describeTopics(Collections.singletonList(testTopicName)).allTopicNames.get.get(testTopicName).partitions
        assertEquals(defaultNumPartitions, partitions.size, "Unequal partition size: " + partitions.size)
        assertEquals(2, partitions.get(0).replicas.size.toShort, "Partitions not replicated: " + partitions.get(0).replicas.size)
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest(brokers = 3)
  @throws[Exception]
  def testCreateWithConfigs(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        val configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName)
        val topicConfig = new util.HashMap[String, String]
        topicConfig.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "1000")
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, 2, 2.toShort).configs(topicConfig)))
        clusterInstance.waitForTopic(testTopicName, 2)
        val configs = adminClient.describeConfigs(Collections.singleton(configResource)).all.get.get(configResource)
        assertEquals(1000, Integer.valueOf(configs.get("delete.retention.ms").value), "Config not set correctly: " + configs.get("delete.retention.ms").value)
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest(brokers = 3)
  @throws[Exception]
  def testCreateWhenAlreadyExists(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        val createOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--create", "--partitions", Integer.toString(defaultNumPartitions), "--replication-factor", "1", "--topic", testTopicName)
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)))
        clusterInstance.waitForTopic(testTopicName, defaultNumPartitions)
        // try to re-create the topic
        assertThrows(classOf[TopicExistsException], () => topicService.createTopic(createOpts), "Expected TopicExistsException to throw")
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest(brokers = 3)
  @throws[Exception]
  def testCreateWhenAlreadyExistsWithIfNotExists(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)))
        clusterInstance.waitForTopic(testTopicName, defaultNumPartitions)
        val createOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--create", "--topic", testTopicName, "--if-not-exists")
        topicService.createTopic(createOpts)
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  private def getPartitionReplicas(partitions: util.List[TopicPartitionInfo], partitionNumber: Int) = partitions.get(partitionNumber).replicas.stream.map(Node.id).collect(Collectors.toList)

  @ClusterTemplate("generate")
  @throws[Exception]
  def testCreateWithReplicaAssignment(clusterInstance: ClusterInstance): Unit = {
    val replicaAssignmentMap = new util.HashMap[Integer, util.List[Integer]]
    try {
      val adminClient = clusterInstance.admin
      try {
        val testTopicName = TestUtils.randomString(10)
        replicaAssignmentMap.put(0, util.Arrays.asList(5, 4))
        replicaAssignmentMap.put(1, util.Arrays.asList(3, 2))
        replicaAssignmentMap.put(2, util.Arrays.asList(1, 0))
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, replicaAssignmentMap)))
        clusterInstance.waitForTopic(testTopicName, 3)
        val partitions = adminClient.describeTopics(Collections.singletonList(testTopicName)).allTopicNames.get.get(testTopicName).partitions
        assertEquals(3, partitions.size, "Unequal partition size: " + partitions.size)
        assertEquals(util.Arrays.asList(5, 4), getPartitionReplicas(partitions, 0), "Unexpected replica assignment: " + getPartitionReplicas(partitions, 0))
        assertEquals(util.Arrays.asList(3, 2), getPartitionReplicas(partitions, 1), "Unexpected replica assignment: " + getPartitionReplicas(partitions, 1))
        assertEquals(util.Arrays.asList(1, 0), getPartitionReplicas(partitions, 2), "Unexpected replica assignment: " + getPartitionReplicas(partitions, 2))
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest(brokers = 3)
  @throws[Exception]
  def testCreateWithInvalidReplicationFactor(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        val opts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--create", "--partitions", "2", "--replication-factor", Integer.toString(Short.MAX_VALUE + 1), "--topic", testTopicName)
        assertThrows(classOf[IllegalArgumentException], () => topicService.createTopic(opts), "Expected IllegalArgumentException to throw")
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest
  @throws[Exception]
  def testCreateWithNegativeReplicationFactor(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        val opts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--create", "--partitions", "2", "--replication-factor", "-1", "--topic", testTopicName)
        assertThrows(classOf[IllegalArgumentException], () => topicService.createTopic(opts), "Expected IllegalArgumentException to throw")
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest
  @throws[Exception]
  def testCreateWithNegativePartitionCount(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        val opts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--create", "--partitions", "-1", "--replication-factor", "1", "--topic", testTopicName)
        assertThrows(classOf[IllegalArgumentException], () => topicService.createTopic(opts), "Expected IllegalArgumentException to throw")
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest def testInvalidTopicLevelConfig(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        val topicService = new TopicCommand.TopicService(adminClient)
        val createOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--create", "--partitions", "1", "--replication-factor", "1", "--topic", testTopicName, "--config", "message.timestamp.type=boom")
        assertThrows(classOf[ConfigException], () => topicService.createTopic(createOpts), "Expected ConfigException to throw")
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest
  @throws[InterruptedException]
  def testListTopics(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)))
        clusterInstance.waitForTopic(testTopicName, defaultNumPartitions)
        val output = captureListTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--list"))
        assertTrue(output.contains(testTopicName), "Expected topic name to be present in output: " + output)
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest(brokers = 3)
  @throws[InterruptedException]
  def testListTopicsWithIncludeList(clusterInstance: ClusterInstance): Unit = {
    try {
      val adminClient = clusterInstance.admin
      try {
        val topic1 = "kafka.testTopic1"
        val topic2 = "kafka.testTopic2"
        val topic3 = "oooof.testTopic1"
        val partition = 2
        val replicationFactor = 2
        adminClient.createTopics(Collections.singletonList(new NewTopic(topic1, partition, replicationFactor)))
        adminClient.createTopics(Collections.singletonList(new NewTopic(topic2, partition, replicationFactor)))
        adminClient.createTopics(Collections.singletonList(new NewTopic(topic3, partition, replicationFactor)))
        clusterInstance.waitForTopic(topic1, partition)
        clusterInstance.waitForTopic(topic2, partition)
        clusterInstance.waitForTopic(topic3, partition)
        val output = captureListTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--list", "--topic", "kafka.*"))
        assertTrue(output.contains(topic1), "Expected topic name " + topic1 + " to be present in output: " + output)
        assertTrue(output.contains(topic2), "Expected topic name " + topic2 + " to be present in output: " + output)
        assertFalse(output.contains(topic3), "Do not expect topic name " + topic3 + " to be present in output: " + output)
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest(brokers = 3)
  @throws[InterruptedException]
  def testListTopicsWithExcludeInternal(clusterInstance: ClusterInstance): Unit = {
    try {
      val adminClient = clusterInstance.admin
      try {
        val topic1 = "kafka.testTopic1"
        val hiddenConsumerTopic = Topic.GROUP_METADATA_TOPIC_NAME
        val partition = 2
        val replicationFactor = 2
        adminClient.createTopics(Collections.singletonList(new NewTopic(topic1, partition, replicationFactor)))
        clusterInstance.waitForTopic(topic1, partition)
        val output = captureListTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--list", "--exclude-internal"))
        assertTrue(output.contains(topic1), "Expected topic name " + topic1 + " to be present in output: " + output)
        assertFalse(output.contains(hiddenConsumerTopic), "Do not expect topic name " + hiddenConsumerTopic + " to be present in output: " + output)
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest(brokers = 3)
  @throws[Exception]
  def testAlterPartitionCount(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        val partition = 2
        val replicationFactor = 2
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partition, replicationFactor)))
        clusterInstance.waitForTopic(testTopicName, partition)
        topicService.alterTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--topic", testTopicName, "--partitions", "3"))
        TestUtils.waitForCondition(() => adminClient.listPartitionReassignments.reassignments.get.isEmpty, TopicCommandTest.CLUSTER_WAIT_MS, testTopicName + String.format("reassignmet not finished after %s ms", TopicCommandTest.CLUSTER_WAIT_MS))
        TestUtils.waitForCondition(() => clusterInstance.brokers.values.stream.allMatch((b: KafkaBroker) => b.metadataCache.numPartitions(testTopicName).orElse(0) eq 3), TestUtils.DEFAULT_MAX_WAIT_MS, "Timeout waiting for new assignment propagating to broker")
        val topicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName)).topicNameValues.get(testTopicName).get
        assertEquals(3, topicDescription.partitions.size, "Expected partition count to be 3. Got: " + topicDescription.partitions.size)
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTemplate("generate")
  @throws[Exception]
  def testAlterAssignment(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        val partition = 2
        val replicationFactor = 2
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partition, replicationFactor)))
        clusterInstance.waitForTopic(testTopicName, partition)
        topicService.alterTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2", "--partitions", "3"))
        TestUtils.waitForCondition(() => adminClient.listPartitionReassignments.reassignments.get.isEmpty, TopicCommandTest.CLUSTER_WAIT_MS, testTopicName + String.format("reassignmet not finished after %s ms", TopicCommandTest.CLUSTER_WAIT_MS))
        TestUtils.waitForCondition(() => clusterInstance.brokers.values.stream.allMatch((b: KafkaBroker) => b.metadataCache.numPartitions(testTopicName).orElse(0) eq 3), TestUtils.DEFAULT_MAX_WAIT_MS, "Timeout waiting for new assignment propagating to broker")
        val topicDescription = adminClient.describeTopics(Collections.singletonList(testTopicName)).topicNameValues.get(testTopicName).get
        assertEquals(3, topicDescription.partitions.size, "Expected partition count to be 3. Got: " + topicDescription.partitions.size)
        val partitionReplicas = getPartitionReplicas(topicDescription.partitions, 2)
        assertEquals(util.Arrays.asList(4, 2), partitionReplicas, "Expected to have replicas 4,2. Got: " + partitionReplicas)
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest(brokers = 3)
  @throws[Exception]
  def testAlterAssignmentWithMoreAssignmentThanPartitions(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        val partition = 2
        val replicationFactor = 2
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partition, replicationFactor)))
        clusterInstance.waitForTopic(testTopicName, partition)
        assertThrows(classOf[ExecutionException], () => topicService.alterTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2,3:2", "--partitions", "3")), "Expected to fail with ExecutionException")
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTemplate("generate")
  @throws[Exception]
  def testAlterAssignmentWithMorePartitionsThanAssignment(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        val partition = 2
        val replicationFactor = 2
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partition, replicationFactor)))
        clusterInstance.waitForTopic(testTopicName, partition)
        assertThrows(classOf[ExecutionException], () => topicService.alterTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--topic", testTopicName, "--replica-assignment", "5:3,3:1,4:2", "--partitions", "6")), "Expected to fail with ExecutionException")
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest
  @throws[Exception]
  def testAlterWithInvalidPartitionCount(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)))
        clusterInstance.waitForTopic(testTopicName, defaultNumPartitions)
        assertThrows(classOf[ExecutionException], () => topicService.alterTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--partitions", "-1", "--topic", testTopicName)), "Expected to fail with ExecutionException")
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest
  @throws[Exception]
  def testAlterWhenTopicDoesntExist(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        // alter a topic that does not exist without --if-exists
        val alterOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--topic", testTopicName, "--partitions", "1")
        assertThrows(classOf[IllegalArgumentException], () => topicService.alterTopic(alterOpts), "Expected to fail with IllegalArgumentException")
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest
  @throws[Exception]
  def testAlterWhenTopicDoesntExistWithIfExists(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    val adminClient = clusterInstance.admin
    val topicService = new TopicCommand.TopicService(adminClient)
    topicService.alterTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--topic", testTopicName, "--partitions", "1", "--if-exists"))
    adminClient.close()
    topicService.close()
  }

  @ClusterTemplate("generate")
  @throws[Exception]
  def testCreateAlterTopicWithRackAware(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        val rackInfo = new util.HashMap[Integer, String]
        rackInfo.put(0, "rack1")
        rackInfo.put(1, "rack2")
        rackInfo.put(2, "rack2")
        rackInfo.put(3, "rack1")
        rackInfo.put(4, "rack3")
        rackInfo.put(5, "rack3")
        val numPartitions = 18
        val replicationFactor = 3
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, numPartitions, replicationFactor.toShort)))
        clusterInstance.waitForTopic(testTopicName, numPartitions)
        var assignment = adminClient.describeTopics(Collections.singletonList(testTopicName)).allTopicNames.get.get(testTopicName).partitions.stream.collect(Collectors.toMap((info: TopicPartitionInfo) => info.partition, (info: TopicPartitionInfo) => info.replicas.stream.map(Node.id).collect(Collectors.toList)))
        checkReplicaDistribution(assignment, rackInfo, rackInfo.size, numPartitions, replicationFactor, true, true, true)
        val alteredNumPartitions = 36
        // verify that adding partitions will also be rack aware
        val alterOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--partitions", Integer.toString(alteredNumPartitions), "--topic", testTopicName)
        topicService.alterTopic(alterOpts)
        TestUtils.waitForCondition(() => adminClient.listPartitionReassignments.reassignments.get.isEmpty, TopicCommandTest.CLUSTER_WAIT_MS, testTopicName + String.format("reassignmet not finished after %s ms", TopicCommandTest.CLUSTER_WAIT_MS))
        TestUtils.waitForCondition(() => clusterInstance.brokers.values.stream.allMatch((p: KafkaBroker) => p.metadataCache.numPartitions(testTopicName).orElse(0) eq alteredNumPartitions), TestUtils.DEFAULT_MAX_WAIT_MS, "Timeout waiting for new assignment propagating to broker")
        assignment = adminClient.describeTopics(Collections.singletonList(testTopicName)).allTopicNames.get.get(testTopicName).partitions.stream.collect(Collectors.toMap((info: TopicPartitionInfo) => info.partition, (info: TopicPartitionInfo) => info.replicas.stream.map(Node.id).collect(Collectors.toList)))
        checkReplicaDistribution(assignment, rackInfo, rackInfo.size, alteredNumPartitions, replicationFactor, true, true, true)
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest(brokers = 3)
  @throws[Exception]
  def testConfigPreservationAcrossPartitionAlteration(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        val cleanUpPolicy = "compact"
        val topicConfig = new util.HashMap[String, String]
        topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanUpPolicy)
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor).configs(topicConfig)))
        clusterInstance.waitForTopic(testTopicName, defaultNumPartitions)
        val configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName)
        val props = adminClient.describeConfigs(Collections.singleton(configResource)).all.get.get(configResource)
        assertNotNull(props.get(TopicConfig.CLEANUP_POLICY_CONFIG), "Properties after creation don't contain " + cleanUpPolicy)
        assertEquals(cleanUpPolicy, props.get(TopicConfig.CLEANUP_POLICY_CONFIG).value, "Properties after creation have incorrect value")
        // modify the topic to add new partitions
        val numPartitionsModified = 3
        val alterOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--alter", "--partitions", Integer.toString(numPartitionsModified), "--topic", testTopicName)
        topicService.alterTopic(alterOpts)
        TestUtils.waitForCondition(() => clusterInstance.brokers.values.stream.allMatch((p: KafkaBroker) => p.metadataCache.numPartitions(testTopicName).orElse(0) eq numPartitionsModified), TestUtils.DEFAULT_MAX_WAIT_MS, "Timeout waiting for new assignment propagating to broker")
        val newProps = adminClient.describeConfigs(Collections.singleton(configResource)).all.get.get(configResource)
        assertNotNull(newProps.get(TopicConfig.CLEANUP_POLICY_CONFIG), "Updated properties do not contain " + TopicConfig.CLEANUP_POLICY_CONFIG)
        assertEquals(cleanUpPolicy, newProps.get(TopicConfig.CLEANUP_POLICY_CONFIG).value, "Updated properties have incorrect value")
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest(brokers = 3, serverProperties = Array(Array(new ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"), new ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000"))))
  @throws[Exception]
  def testTopicDeletion(clusterInstance: ClusterInstance): Unit = {
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        val testTopicName = TestUtils.randomString(10)
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)))
        clusterInstance.waitForTopic(testTopicName, defaultNumPartitions)
        // delete the NormalTopic
        val deleteOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--delete", "--topic", testTopicName)
        topicService.deleteTopic(deleteOpts)
        TestUtils.waitForCondition(() => adminClient.listTopics.listings.get.stream.noneMatch((topic: TopicListing) => topic.name == testTopicName), TopicCommandTest.CLUSTER_WAIT_MS, String.format("Delete topic fail in %s ms", TopicCommandTest.CLUSTER_WAIT_MS))
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest(brokers = 3, serverProperties = Array(Array(new ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"), new ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000"))))
  @throws[Exception]
  def testTopicWithCollidingCharDeletionAndCreateAgain(clusterInstance: ClusterInstance): Unit = {
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        // create the topic with colliding chars
        val topicWithCollidingChar = "test.a"
        adminClient.createTopics(Collections.singletonList(new NewTopic(topicWithCollidingChar, defaultNumPartitions, defaultReplicationFactor)))
        clusterInstance.waitForTopic(topicWithCollidingChar, defaultNumPartitions)
        // delete the topic
        val deleteOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--delete", "--topic", topicWithCollidingChar)
        topicService.deleteTopic(deleteOpts)
        TestUtils.waitForCondition(() => adminClient.listTopics.listings.get.stream.noneMatch((topic: TopicListing) => topic.name == topicWithCollidingChar), TopicCommandTest.CLUSTER_WAIT_MS, String.format("Delete topic fail in %s ms", TopicCommandTest.CLUSTER_WAIT_MS))
        clusterInstance.waitTopicDeletion(topicWithCollidingChar)
        // recreate same topic
        adminClient.createTopics(Collections.singletonList(new NewTopic(topicWithCollidingChar, defaultNumPartitions, defaultReplicationFactor)))
        clusterInstance.waitForTopic(topicWithCollidingChar, defaultNumPartitions)
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest(brokers = 3, serverProperties = Array(Array(new ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"), new ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000"))))
  @throws[Exception]
  def testDeleteInternalTopic(clusterInstance: ClusterInstance): Unit = {
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        // create the offset topic
        adminClient.createTopics(Collections.singletonList(new NewTopic(Topic.GROUP_METADATA_TOPIC_NAME, defaultNumPartitions, defaultReplicationFactor)))
        clusterInstance.waitForTopic(Topic.GROUP_METADATA_TOPIC_NAME, defaultNumPartitions)
        // Try to delete the Topic.GROUP_METADATA_TOPIC_NAME which is allowed by default.
        // This is a difference between the new and the old command as the old one didn't allow internal topic deletion.
        // If deleting internal topics is not desired, ACLS should be used to control it.
        val deleteOffsetTopicOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--delete", "--topic", Topic.GROUP_METADATA_TOPIC_NAME)
        topicService.deleteTopic(deleteOffsetTopicOpts)
        TestUtils.waitForCondition(() => adminClient.listTopics.listings.get.stream.noneMatch((topic: TopicListing) => topic.name == Topic.GROUP_METADATA_TOPIC_NAME), TopicCommandTest.CLUSTER_WAIT_MS, String.format("Delete topic fail in %s ms", TopicCommandTest.CLUSTER_WAIT_MS))
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest(brokers = 3, serverProperties = Array(Array(new ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"), new ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000"))))
  @throws[Exception]
  def testDeleteWhenTopicDoesntExist(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        // delete a topic that does not exist
        val deleteOpts = buildTopicCommandOptionsWithBootstrap(clusterInstance, "--delete", "--topic", testTopicName)
        assertThrows(classOf[IllegalArgumentException], () => topicService.deleteTopic(deleteOpts), "Expected an exception when trying to delete a topic that does not exist.")
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest(brokers = 3, serverProperties = Array(Array(new ClusterConfigProperty(key = "log.initial.task.delay.ms", value = "100"), new ClusterConfigProperty(key = "log.segment.delete.delay.ms", value = "1000"))))
  @throws[Exception]
  def testDeleteWhenTopicDoesntExistWithIfExists(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try topicService.deleteTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--delete", "--topic", testTopicName, "--if-exists"))
      finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTemplate("generate")
  @throws[InterruptedException]
  def testDescribe(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        val partition = 2
        val replicationFactor = 2
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partition, replicationFactor)))
        clusterInstance.waitForTopic(testTopicName, partition)
        val output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--topic", testTopicName))
        val rows = output.split(System.lineSeparator)
        assertEquals(3, rows.length, "Expected 3 rows in output, got " + rows.length)
        assertTrue(rows(0).startsWith(String.format("Topic: %s", testTopicName)), "Row does not start with " + testTopicName + ". Row is: " + rows(0))
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTemplate("generate")
  @throws[InterruptedException]
  def testDescribeWithDescribeTopicPartitionsApi(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        val topics = new util.ArrayList[NewTopic]
        topics.add(new NewTopic(testTopicName, 20, 2.toShort))
        topics.add(new NewTopic("test-2", 41, 2.toShort))
        topics.add(new NewTopic("test-3", 5, 2.toShort))
        topics.add(new NewTopic("test-4", 5, 2.toShort))
        topics.add(new NewTopic("test-5", 100, 2.toShort))
        adminClient.createTopics(topics)
        clusterInstance.waitForTopic(testTopicName, 20)
        clusterInstance.waitForTopic("test-2", 41)
        clusterInstance.waitForTopic("test-3", 5)
        clusterInstance.waitForTopic("test-4", 5)
        clusterInstance.waitForTopic("test-5", 100)
        val output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--partition-size-limit-per-response=20", "--exclude-internal"))
        val rows = output.split("\n")
        assertEquals(176, rows.length, String.join("\n", rows))
        assertTrue(rows(2).contains("\tElr"), rows(2))
        assertTrue(rows(2).contains("LastKnownElr"), rows(2))
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest def testDescribeWhenTopicDoesntExist(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        val topicService = new TopicCommand.TopicService(adminClient)
        assertThrows(classOf[IllegalArgumentException], () => topicService.describeTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--topic", testTopicName)), "Expected an exception when trying to describe a topic that does not exist.")
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest
  @throws[Exception]
  def testDescribeWhenTopicDoesntExistWithIfExists(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        val topicService = new TopicCommand.TopicService(adminClient)
        topicService.describeTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--topic", testTopicName, "--if-exists"))
        topicService.close()
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest(brokers = 3)
  @throws[InterruptedException]
  def testDescribeUnavailablePartitions(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        val partitions = 3
        val replicationFactor = 1
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partitions, replicationFactor)))
        clusterInstance.waitForTopic(testTopicName, partitions)
        // check which partition is on broker 0 which we'll kill
        clusterInstance.shutdownBroker(0)
        assertEquals(2, clusterInstance.aliveBrokers.size)
        // wait until the topic metadata for the test topic is propagated to each alive broker
        clusterInstance.waitForTopic(testTopicName, 3)
        // grab the console output and assert
        val output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--topic", testTopicName, "--unavailable-partitions"))
        val rows = output.split(System.lineSeparator)
        assertTrue(rows(0).startsWith(String.format("Topic: %s", testTopicName)), "Unexpected Topic " + rows(0) + " received. Expect " + String.format("Topic: %s", testTopicName))
        assertTrue(rows(0).contains("Leader: none\tReplicas: 0\tIsr:"), "Rows did not contain 'Leader: none\tReplicas: 0\tIsr:'")
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest(brokers = 3)
  @throws[InterruptedException]
  def testDescribeUnderReplicatedPartitions(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        val partitions = 1
        val replicationFactor = 3
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partitions, replicationFactor)))
        clusterInstance.waitForTopic(testTopicName, partitions)
        clusterInstance.shutdownBroker(0)
        Assertions.assertEquals(clusterInstance.aliveBrokers.size, 2)
        TestUtils.waitForCondition(() => clusterInstance.aliveBrokers.values.stream.allMatch((broker: KafkaBroker) => {
          val partitionState = Optional.ofNullable(broker.metadataCache.getLeaderAndIsr(testTopicName, 0).orElseGet(null))
          partitionState.map((s: LeaderAndIsr) => FetchRequest.isValidBrokerId(s.leader)).orElse(false)
        }), TopicCommandTest.CLUSTER_WAIT_MS, String.format("Meta data propogation fail in %s ms", TopicCommandTest.CLUSTER_WAIT_MS))
        val output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--under-replicated-partitions"))
        val rows = output.split(System.lineSeparator)
        assertTrue(rows(0).startsWith(String.format("Topic: %s", testTopicName)), String.format("Unexpected output: %s", rows(0)))
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest(brokers = 3)
  @throws[InterruptedException]
  def testDescribeUnderMinIsrPartitions(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        val topicConfig = new util.HashMap[String, String]
        topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
        val partitions = 1
        val replicationFactor = 3
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partitions, replicationFactor).configs(topicConfig)))
        clusterInstance.waitForTopic(testTopicName, partitions)
        clusterInstance.shutdownBroker(0)
        assertEquals(2, clusterInstance.aliveBrokers.size)
        TestUtils.waitForCondition(() => clusterInstance.aliveBrokers.values.stream.allMatch((broker: KafkaBroker) => broker.metadataCache.getLeaderAndIsr(testTopicName, 0).get.isr.size == 2), TopicCommandTest.CLUSTER_WAIT_MS, String.format("Timeout waiting for partition metadata propagating to brokers for %s topic", testTopicName))
        val output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--under-min-isr-partitions", "--exclude-internal"))
        val rows = output.split(System.lineSeparator)
        assertTrue(rows(0).startsWith(String.format("Topic: %s", testTopicName)), "Unexpected topic: " + rows(0))
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTemplate("generate")
  @throws[ExecutionException]
  @throws[InterruptedException]
  def testDescribeUnderReplicatedPartitionsWhenReassignmentIsInProgress(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      val producer = createProducer(clusterInstance)
      try {
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)))
        clusterInstance.waitForTopic(testTopicName, defaultNumPartitions)
        val tp = new TopicPartition(testTopicName, 0)
        // Produce multiple batches.
        sendProducerRecords(testTopicName, producer, 10)
        sendProducerRecords(testTopicName, producer, 10)
        // Enable throttling. Note the broker config sets the replica max fetch bytes to `1` upon to minimize replication
        // throughput so the reassignment doesn't complete quickly.
        val brokerIds = new util.ArrayList[Integer](clusterInstance.brokerIds)
        ToolsTestUtils.setReplicationThrottleForPartitions(adminClient, brokerIds, Collections.singleton(tp), 1)
        val testTopicDesc = adminClient.describeTopics(Collections.singleton(testTopicName)).allTopicNames.get.get(testTopicName)
        val firstPartition = testTopicDesc.partitions.get(0)
        val replicasOfFirstPartition = firstPartition.replicas.stream.map(Node.id).collect(Collectors.toList)
        val replicasDiff = new util.ArrayList[Integer](brokerIds)
        replicasDiff.removeAll(replicasOfFirstPartition)
        val targetReplica = replicasDiff.get(0)
        adminClient.alterPartitionReassignments(Collections.singletonMap(tp, Optional.of(new NewPartitionReassignment(Collections.singletonList(targetReplica))))).all.get
        // let's wait until the LAIR is propagated
        TestUtils.waitForCondition(() => !adminClient.listPartitionReassignments(Collections.singleton(tp)).reassignments.get.get(tp).addingReplicas.isEmpty, TopicCommandTest.CLUSTER_WAIT_MS, "Reassignment didn't add the second node")
        // describe the topic and test if it's under-replicated
        val simpleDescribeOutput = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--topic", testTopicName))
        val simpleDescribeOutputRows = simpleDescribeOutput.split(System.lineSeparator)
        assertTrue(simpleDescribeOutputRows(0).startsWith(String.format("Topic: %s", testTopicName)), "Unexpected describe output: " + simpleDescribeOutputRows(0))
        assertEquals(2, simpleDescribeOutputRows.length, "Unexpected describe output length: " + simpleDescribeOutputRows.length)
        val underReplicatedOutput = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--under-replicated-partitions"))
        assertEquals("", underReplicatedOutput, String.format("--under-replicated-partitions shouldn't return anything: '%s'", underReplicatedOutput))
        val maxRetries = 20
        val pause = 100L
        val waitTimeMs = maxRetries * pause
        val reassignmentsRef = new AtomicReference[PartitionReassignment]
        TestUtils.waitForCondition(() => {
          val tempReassignments = adminClient.listPartitionReassignments(Collections.singleton(tp)).reassignments.get.get(tp)
          reassignmentsRef.set(tempReassignments)
          reassignmentsRef.get != null
        }, waitTimeMs, "Reassignments did not become non-null within the specified time")
        assertFalse(reassignmentsRef.get.addingReplicas.isEmpty)
        ToolsTestUtils.removeReplicationThrottleForPartitions(adminClient, brokerIds, Collections.singleton(tp))
        TestUtils.waitForCondition(() => adminClient.listPartitionReassignments.reassignments.get.isEmpty, TopicCommandTest.CLUSTER_WAIT_MS, String.format("reassignmet not finished after %s ms", TopicCommandTest.CLUSTER_WAIT_MS))
      } finally {
        if (adminClient != null) adminClient.close()
        if (producer != null) producer.close()
      }
    }
  }

  @ClusterTemplate("generate")
  @throws[InterruptedException]
  def testDescribeAtMinIsrPartitions(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        val topicConfig = new util.HashMap[String, String]
        topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4")
        val partitions = 1
        val replicationFactor = 6
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partitions, replicationFactor).configs(topicConfig)))
        clusterInstance.waitForTopic(testTopicName, partitions)
        clusterInstance.shutdownBroker(0)
        clusterInstance.shutdownBroker(1)
        assertEquals(4, clusterInstance.aliveBrokers.size)
        TestUtils.waitForCondition(() => clusterInstance.aliveBrokers.values.stream.allMatch((broker: KafkaBroker) => broker.metadataCache.getLeaderAndIsr(testTopicName, 0).get.isr.size == 4), TopicCommandTest.CLUSTER_WAIT_MS, String.format("Timeout waiting for partition metadata propagating to brokers for %s topic", testTopicName))
        val output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--at-min-isr-partitions", "--exclude-internal"))
        val rows = output.split(System.lineSeparator)
        assertTrue(rows(0).startsWith(String.format("Topic: %s", testTopicName)), "Unexpected output: " + rows(0))
        assertEquals(1, rows.length)
      } finally if (adminClient != null) adminClient.close()
    }
  }

  /**
   * Test describe --under-min-isr-partitions option with four topics:
   * (1) topic with partition under the configured min ISR count
   * (2) topic with under-replicated partition (but not under min ISR count)
   * (3) topic with offline partition
   * (4) topic with fully replicated partition
   *
   * Output should only display the (1) topic with partition under min ISR count and (3) topic with offline partition
   */
  @ClusterTemplate("generate")
  @throws[InterruptedException]
  def testDescribeUnderMinIsrPartitionsMixed(clusterInstance: ClusterInstance): Unit = {
    try {
      val adminClient = clusterInstance.admin
      try {
        val underMinIsrTopic = "under-min-isr-topic"
        val notUnderMinIsrTopic = "not-under-min-isr-topic"
        val offlineTopic = "offline-topic"
        val fullyReplicatedTopic = "fully-replicated-topic"
        val partitions = 1
        val replicationFactor = 6
        val newTopics = new util.ArrayList[NewTopic]
        val fullyReplicatedReplicaAssignmentMap = new util.HashMap[Integer, util.List[Integer]]
        fullyReplicatedReplicaAssignmentMap.put(0, util.Arrays.asList(1, 2, 3))
        val offlineReplicaAssignmentMap = new util.HashMap[Integer, util.List[Integer]]
        offlineReplicaAssignmentMap.put(0, util.Arrays.asList(0))
        val topicConfig = new util.HashMap[String, String]
        topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "6")
        newTopics.add(new NewTopic(underMinIsrTopic, partitions, replicationFactor).configs(topicConfig))
        newTopics.add(new NewTopic(notUnderMinIsrTopic, partitions, replicationFactor))
        newTopics.add(new NewTopic(offlineTopic, offlineReplicaAssignmentMap))
        newTopics.add(new NewTopic(fullyReplicatedTopic, fullyReplicatedReplicaAssignmentMap))
        adminClient.createTopics(newTopics)
        import scala.collection.JavaConversions._
        for (topioc <- newTopics) {
          clusterInstance.waitForTopic(topioc.name, partitions)
        }
        clusterInstance.shutdownBroker(0)
        Assertions.assertEquals(5, clusterInstance.aliveBrokers.size)
        TestUtils.waitForCondition(() => clusterInstance.aliveBrokers.values.stream.allMatch((broker: KafkaBroker) => broker.metadataCache.getLeaderAndIsr(underMinIsrTopic, 0).get.isr.size < 6 && broker.metadataCache.getLeaderAndIsr(offlineTopic, 0).get.leader == MetadataResponse.NO_LEADER_ID), TopicCommandTest.CLUSTER_WAIT_MS, "Timeout waiting for partition metadata propagating to brokers for underMinIsrTopic topic")
        TestUtils.waitForCondition(() => adminClient.listPartitionReassignments.reassignments.get.isEmpty, TopicCommandTest.CLUSTER_WAIT_MS, String.format("reassignmet not finished after %s ms", TopicCommandTest.CLUSTER_WAIT_MS))
        val output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--under-min-isr-partitions", "--exclude-internal"))
        val rows = output.split(System.lineSeparator)
        assertTrue(rows(0).startsWith(String.format("Topic: %s", underMinIsrTopic)), "Unexpected output: " + rows(0))
        assertTrue(rows(1).startsWith(String.format("\tTopic: %s", offlineTopic)), "Unexpected output: " + rows(1))
        assertEquals(2, rows.length)
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest(brokers = 3)
  @throws[InterruptedException]
  def testDescribeReportOverriddenConfigs(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        val config = "file.delete.delay.ms=1000"
        val topicConfig = new util.HashMap[String, String]
        topicConfig.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "1000")
        val partitions = 2
        val replicationFactor = 2
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, partitions, replicationFactor).configs(topicConfig)))
        clusterInstance.waitForTopic(testTopicName, partitions)
        val output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe"))
        assertTrue(output.contains(config), String.format("Describe output should have contained %s", config))
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest
  @throws[InterruptedException]
  def testDescribeAndListTopicsWithoutInternalTopics(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = clusterInstance.admin
      try {
        adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)))
        clusterInstance.waitForTopic(testTopicName, defaultNumPartitions)
        // test describe
        var output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--describe", "--exclude-internal"))
        assertTrue(output.contains(testTopicName), String.format("Output should have contained %s", testTopicName))
        assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME), "Output should not have contained " + Topic.GROUP_METADATA_TOPIC_NAME)
        // test list
        output = captureListTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--list", "--exclude-internal"))
        assertTrue(output.contains(testTopicName), String.format("Output should have contained %s", testTopicName))
        assertFalse(output.contains(Topic.GROUP_METADATA_TOPIC_NAME), "Output should not have contained " + Topic.GROUP_METADATA_TOPIC_NAME)
      } finally if (adminClient != null) adminClient.close()
    }
  }

  @ClusterTest
  @throws[Exception]
  def testDescribeDoesNotFailWhenListingReassignmentIsUnauthorized(clusterInstance: ClusterInstance): Unit = {
    val testTopicName = TestUtils.randomString(10)
    var adminClient = clusterInstance.admin
    adminClient = spy(adminClient)
    val result = AdminClientTestUtils.listPartitionReassignmentsResult(new ClusterAuthorizationException("Unauthorized"))
    doReturn(result).when(adminClient).listPartitionReassignments(Collections.singleton(new TopicPartition(testTopicName, 0)))
    adminClient.createTopics(Collections.singletonList(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)))
    clusterInstance.waitForTopic(testTopicName, defaultNumPartitions)
    val output = captureDescribeTopicStandardOut(clusterInstance, buildTopicCommandOptionsWithBootstrap(clusterInstance, "--describe", "--topic", testTopicName))
    val rows = output.split(System.lineSeparator)
    assertEquals(2, rows.length, "Unexpected output: " + output)
    assertTrue(rows(0).startsWith(String.format("Topic: %s", testTopicName)), "Unexpected output: " + rows(0))
    adminClient.close()
  }

  @ClusterTest(brokers = 3)
  @throws[Exception]
  def testCreateWithTopicNameCollision(clusterInstance: ClusterInstance): Unit = {
    try {
      val adminClient = clusterInstance.admin
      val topicService = new TopicCommand.TopicService(adminClient)
      try {
        val topic = "foo_bar"
        val partitions = 1
        val replicationFactor = 3
        adminClient.createTopics(Collections.singletonList(new NewTopic(topic, partitions, replicationFactor)))
        clusterInstance.waitForTopic(topic, defaultNumPartitions)
        assertThrows(classOf[TopicExistsException], () => topicService.createTopic(buildTopicCommandOptionsWithBootstrap(clusterInstance, "--create", "--topic", topic)))
      } finally {
        if (adminClient != null) adminClient.close()
        if (topicService != null) topicService.close()
      }
    }
  }

  @ClusterTest
  @throws[InterruptedException]
  @throws[ExecutionException]
  def testCreateWithInternalConfig(cluster: ClusterInstance): Unit = {
    val internalConfigTopicName = TestUtils.randomString(10)
    val testTopicName = TestUtils.randomString(10)
    try {
      val adminClient = cluster.admin
      try {
        val internalResult = adminClient.createTopics(util.List.of(new NewTopic(internalConfigTopicName, defaultNumPartitions, defaultReplicationFactor).configs(util.Map.of(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, "1000"))))
        val internalConfigEntry = internalResult.config(internalConfigTopicName).get.get(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG)
        assertNotNull(internalConfigEntry, "Internal config entry should not be null")
        assertEquals("1000", internalConfigEntry.value)
        val nonInternalResult = adminClient.createTopics(util.List.of(new NewTopic(testTopicName, defaultNumPartitions, defaultReplicationFactor)))
        val nonInternalConfigEntry = nonInternalResult.config(testTopicName).get.get(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG)
        assertNull(nonInternalConfigEntry, "Non-internal config entry should be null")
      } finally if (adminClient != null) adminClient.close()
    }
  }

  private def checkReplicaDistribution(assignment: util.Map[Integer, util.List[Integer]], brokerRackMapping: util.Map[Integer, String], numBrokers: Integer, numPartitions: Integer, replicationFactor: Integer, verifyRackAware: Boolean, verifyLeaderDistribution: Boolean, verifyReplicasDistribution: Boolean): Unit = {
    // always verify that no broker will be assigned for more than one replica
    assignment.forEach((partition: Integer, assignedNodes: util.List[Integer]) => assertEquals(new util.HashSet[Integer](assignedNodes).size, assignedNodes.size, "More than one replica is assigned to same broker for the same partition"))
    val distribution = TopicCommandTest.getReplicaDistribution(assignment, brokerRackMapping)
    if (verifyRackAware) {
      val partitionRackMap = distribution.partitionRacks
      val partitionRackMapValueSize = partitionRackMap.values.stream.map((value: util.List[String]) => value.stream.distinct.count.toInt).collect(Collectors.toList)
      val expected = Collections.nCopies(numPartitions, replicationFactor)
      assertEquals(expected, partitionRackMapValueSize, "More than one replica of the same partition is assigned to the same rack")
    }
    if (verifyLeaderDistribution) {
      val leaderCount = distribution.brokerLeaderCount
      val leaderCountPerBroker = numPartitions / numBrokers
      val expected = Collections.nCopies(numBrokers, leaderCountPerBroker)
      assertEquals(expected, new util.ArrayList[Integer](leaderCount.values), "Preferred leader count is not even for brokers")
    }
    if (verifyReplicasDistribution) {
      val replicasCount = distribution.brokerReplicasCount
      val numReplicasPerBroker = numPartitions * replicationFactor / numBrokers
      val expected = Collections.nCopies(numBrokers, numReplicasPerBroker)
      assertEquals(expected, new util.ArrayList[Integer](replicasCount.values), "Replica count is not even for broker")
    }
  }

  private def captureDescribeTopicStandardOut(clusterInstance: ClusterInstance, opts: TopicCommand.TopicCommandOptions) = {
    val runnable = () => {
      try {
        val adminClient = clusterInstance.admin
        val topicService = new TopicCommand.TopicService(adminClient)
        try topicService.describeTopic(opts)
        catch {
          case e: Exception =>
            throw new RuntimeException(e)
        } finally {
          if (adminClient != null) adminClient.close()
          if (topicService != null) topicService.close()
        }
      }
    }
    ToolsTestUtils.captureStandardOut(runnable)
  }

  private def captureListTopicStandardOut(clusterInstance: ClusterInstance, opts: TopicCommand.TopicCommandOptions) = {
    val runnable = () => {
      try {
        val adminClient = clusterInstance.admin
        val topicService = new TopicCommand.TopicService(adminClient)
        try topicService.listTopics(opts)
        catch {
          case e: Exception =>
            throw new RuntimeException(e)
        } finally {
          if (adminClient != null) adminClient.close()
          if (topicService != null) topicService.close()
        }
      }
    }
    ToolsTestUtils.captureStandardOut(runnable)
  }

  private def createProducer(clusterInstance: ClusterInstance) = {
    val producerProps = new Properties
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers)
    producerProps.put(ProducerConfig.ACKS_CONFIG, "-1")
    new KafkaProducer[String, String](producerProps, new StringSerializer, new StringSerializer)
  }

  private def sendProducerRecords(testTopicName: String, producer: KafkaProducer[String, String], numMessage: Int): Unit = {
    IntStream.range(0, numMessage).forEach((i: Int) => producer.send(new ProducerRecord[String, String](testTopicName, "test-" + i)))
    producer.flush()
  }
}