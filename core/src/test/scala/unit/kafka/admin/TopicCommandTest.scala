/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import kafka.admin.TopicCommand.{PartitionDescription, TopicCommandOptions, TopicService}
import kafka.common.AdminCommandFailedException
import kafka.utils.Exit
import org.apache.kafka.clients.admin.{Admin, AdminClientTestUtils, CreatePartitionsOptions, CreateTopicsOptions, DeleteTopicsOptions, NewPartitions, NewTopic, PartitionReassignment, TopicDescription}
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers.{any, argThat, eq => eqThat}
import org.mockito.Mockito.{mock, times, verify, when}

import java.util.{Collection, Collections, Optional}
import scala.collection.Seq
import scala.concurrent.ExecutionException
import scala.jdk.CollectionConverters._

class TopicCommandTest {

  private[this] val brokerList = "localhost:9092"
  private[this] val topicName = "topicName"

  @Test
  def testIsNotUnderReplicatedWhenAdding(): Unit = {
    val replicaIds = List(1, 2)
    val replicas = replicaIds.map { id =>
      new Node(id, "localhost", 9090 + id)
    }

    val partitionDescription = PartitionDescription(
      "test-topic",
      new TopicPartitionInfo(
        0,
        new Node(1, "localhost", 9091),
        replicas.asJava,
        List(new Node(1, "localhost", 9091)).asJava
      ),
      None,
      markedForDeletion = false,
      Some(
        new PartitionReassignment(
          replicaIds.map(id => id: java.lang.Integer).asJava,
          List(2: java.lang.Integer).asJava,
          List.empty.asJava
        )
      )
    )

    assertFalse(partitionDescription.isUnderReplicated)
  }

  @Test
  def testAlterWithUnspecifiedPartitionCount(): Unit = {
    assertCheckArgsExitCode(1, new TopicCommandOptions(
      Array("--bootstrap-server", brokerList ,"--alter", "--topic", topicName)))
  }

  @Test
  def testConfigOptWithBootstrapServers(): Unit = {
    assertCheckArgsExitCode(1,
      new TopicCommandOptions(Array("--bootstrap-server", brokerList ,"--alter", "--topic", topicName, "--partitions", "3", "--config", "cleanup.policy=compact")))
    assertCheckArgsExitCode(1,
      new TopicCommandOptions(Array("--bootstrap-server", brokerList ,"--alter", "--topic", topicName, "--partitions", "3", "--delete-config", "cleanup.policy")))
    val opts =
      new TopicCommandOptions(Array("--bootstrap-server", brokerList ,"--create", "--topic", topicName, "--partitions", "3", "--replication-factor", "3", "--config", "cleanup.policy=compact"))
    opts.checkArgs()
    assertTrue(opts.hasCreateOption)
    assertEquals(brokerList, opts.bootstrapServer.get)
    assertEquals("cleanup.policy=compact", opts.topicConfig.get.get(0))
  }

  @Test
  def testCreateWithPartitionCountWithoutReplicationFactorShouldSucceed(): Unit = {
    val opts = new TopicCommandOptions(
      Array("--bootstrap-server", brokerList,
        "--create",
        "--partitions", "2",
        "--topic", topicName))
    opts.checkArgs()
  }

  @Test
  def testCreateWithReplicationFactorWithoutPartitionCountShouldSucceed(): Unit = {
    val opts = new TopicCommandOptions(
      Array("--bootstrap-server", brokerList,
        "--create",
        "--replication-factor", "3",
        "--topic", topicName))
    opts.checkArgs()
  }

  @Test
  def testCreateWithAssignmentAndPartitionCount(): Unit = {
    assertCheckArgsExitCode(1,
      new TopicCommandOptions(
        Array("--bootstrap-server", brokerList,
          "--create",
          "--replica-assignment", "3:0,5:1",
          "--partitions", "2",
          "--topic", topicName)))
  }

  @Test
  def testCreateWithAssignmentAndReplicationFactor(): Unit = {
    assertCheckArgsExitCode(1,
      new TopicCommandOptions(
        Array("--bootstrap-server", brokerList,
          "--create",
          "--replica-assignment", "3:0,5:1",
          "--replication-factor", "2",
          "--topic", topicName)))
  }

  @Test
  def testCreateWithoutPartitionCountAndReplicationFactorShouldSucceed(): Unit = {
    val opts = new TopicCommandOptions(
      Array("--bootstrap-server", brokerList,
        "--create",
        "--topic", topicName))
    opts.checkArgs()
  }

  @Test
  def testDescribeShouldSucceed(): Unit = {
    val opts = new TopicCommandOptions(
      Array("--bootstrap-server", brokerList,
        "--describe",
        "--topic", topicName))
    opts.checkArgs()
  }


  @Test
  def testParseAssignmentDuplicateEntries(): Unit = {
    assertThrows(classOf[AdminCommandFailedException], () => TopicCommand.parseReplicaAssignment("5:5"))
  }

  @Test
  def testParseAssignmentPartitionsOfDifferentSize(): Unit = {
    assertThrows(classOf[AdminOperationException], () => TopicCommand.parseReplicaAssignment("5:4:3,2:1"))
  }

  @Test
  def testParseAssignment(): Unit = {
    val actualAssignment = TopicCommand.parseReplicaAssignment("5:4,3:2,1:0")
    val expectedAssignment = Map(0 -> List(5, 4), 1 -> List(3, 2), 2 -> List(1, 0))
    assertEquals(expectedAssignment, actualAssignment)
  }

  @Test
  def testCreateTopicDoesNotRetryThrottlingQuotaExceededException(): Unit = {
    val adminClient = mock(classOf[Admin])
    val topicService = TopicService(adminClient)

    val result = AdminClientTestUtils.createTopicsResult(topicName, Errors.THROTTLING_QUOTA_EXCEEDED.exception())
    when(adminClient.createTopics(any(), any())).thenReturn(result)

    assertThrows(classOf[ThrottlingQuotaExceededException],
      () => topicService.createTopic(new TopicCommandOptions(Array("--topic", topicName))))

    val expectedNewTopic = new NewTopic(topicName, Optional.empty[Integer](), Optional.empty[java.lang.Short]())
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

    val listResult = AdminClientTestUtils.listTopicsResult(topicName)
    when(adminClient.listTopics(any())).thenReturn(listResult)

    val result = AdminClientTestUtils.deleteTopicsResult(topicName, Errors.THROTTLING_QUOTA_EXCEEDED.exception())
    when(adminClient.deleteTopics(any[Collection[String]](), any())).thenReturn(result)

    val exception = assertThrows(classOf[ExecutionException],
      () => topicService.deleteTopic(new TopicCommandOptions(Array("--topic", topicName))))
    assertTrue(exception.getCause.isInstanceOf[ThrottlingQuotaExceededException])

    verify(adminClient).deleteTopics(
      argThat((topics: java.util.Collection[String]) => topics.asScala.toBuffer.equals(Seq(topicName))),
      argThat((options: DeleteTopicsOptions) => !options.shouldRetryOnQuotaViolation)
    )
  }

  @Test
  def testCreatePartitionsDoesNotRetryThrottlingQuotaExceededException(): Unit = {
    val adminClient = mock(classOf[Admin])
    val topicService = TopicService(adminClient)

    val listResult = AdminClientTestUtils.listTopicsResult(topicName)
    when(adminClient.listTopics(any())).thenReturn(listResult)

    val topicPartitionInfo = new TopicPartitionInfo(0, new Node(0, "", 0),
      Collections.emptyList(), Collections.emptyList())
    val describeResult = AdminClientTestUtils.describeTopicsResult(topicName, new TopicDescription(
      topicName, false, Collections.singletonList(topicPartitionInfo)))
    when(adminClient.describeTopics(any(classOf[java.util.Collection[String]]))).thenReturn(describeResult)

    val result = AdminClientTestUtils.createPartitionsResult(topicName, Errors.THROTTLING_QUOTA_EXCEEDED.exception())
    when(adminClient.createPartitions(any(), any())).thenReturn(result)

    val exception = assertThrows(classOf[ExecutionException],
      () => topicService.alterTopic(new TopicCommandOptions(Array("--topic", topicName, "--partitions", "3"))))
    assertTrue(exception.getCause.isInstanceOf[ThrottlingQuotaExceededException])

    verify(adminClient, times(1)).createPartitions(
      argThat((_.get(topicName).totalCount() == 3): ArgumentMatcher[java.util.Map[String, NewPartitions]]),
      argThat((_.shouldRetryOnQuotaViolation() == false): ArgumentMatcher[CreatePartitionsOptions])
    )
  }

  private[this] def assertCheckArgsExitCode(expected: Int, options: TopicCommandOptions): Unit = {
    Exit.setExitProcedure {
      (exitCode: Int, _: Option[String]) =>
        assertEquals(expected, exitCode)
        throw new RuntimeException
    }
    try assertThrows(classOf[RuntimeException], () => options.checkArgs()) finally Exit.resetExitProcedure()
  }
}
