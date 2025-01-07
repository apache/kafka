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

import java.util.Collections
import kafka.controller.ReplicaAssignment
import kafka.server.{BaseRequestTest, BrokerServer}
import kafka.utils.TestUtils
import kafka.utils.TestUtils._
import org.apache.kafka.clients.admin.{Admin, NewPartitions, NewTopic}
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException
import org.apache.kafka.common.requests.{MetadataRequest, MetadataResponse}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util
import java.util.Arrays.asList
import java.util.Collections.singletonList
import java.util.concurrent.ExecutionException
import scala.jdk.CollectionConverters._

class AddPartitionsTest extends BaseRequestTest {

  override def brokerCount: Int = 4

  val partitionId = 0

  val topic1 = "new-topic1"
  val topic1Assignment = Map(0 -> ReplicaAssignment(Seq(0,1), List(), List()))
  val topic2 = "new-topic2"
  val topic2Assignment = Map(0 -> ReplicaAssignment(Seq(1,2), List(), List()))
  val topic3 = "new-topic3"
  val topic3Assignment = Map(0 -> ReplicaAssignment(Seq(2,3,0,1), List(), List()))
  val topic4 = "new-topic4"
  val topic4Assignment = Map(0 -> ReplicaAssignment(Seq(0,3), List(), List()))
  val topic5 = "new-topic5"
  val topic5Assignment = Map(1 -> ReplicaAssignment(Seq(0,1), List(), List()))
  var admin: Admin = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    brokers.foreach(broker => broker.asInstanceOf[BrokerServer].lifecycleManager.initialUnfenceFuture.get())
    createTopicWithAssignment(topic1, partitionReplicaAssignment = topic1Assignment.map { case (k, v) => k -> v.replicas })
    createTopicWithAssignment(topic2, partitionReplicaAssignment = topic2Assignment.map { case (k, v) => k -> v.replicas })
    createTopicWithAssignment(topic3, partitionReplicaAssignment = topic3Assignment.map { case (k, v) => k -> v.replicas })
    createTopicWithAssignment(topic4, partitionReplicaAssignment = topic4Assignment.map { case (k, v) => k -> v.replicas })
    admin = createAdminClient()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testWrongReplicaCount(quorum: String): Unit = {
    assertEquals(classOf[InvalidReplicaAssignmentException], assertThrows(classOf[ExecutionException], () => {
        admin.createPartitions(Collections.singletonMap(topic1,
          NewPartitions.increaseTo(2, singletonList(asList(0, 1, 2))))).all().get()
      }).getCause.getClass)
  }

  /**
   * Test that when we supply a manual partition assignment to createTopics, it must be 0-based
   * and consecutive.
   */
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testMissingPartitionsInCreateTopics(quorum: String): Unit = {
    val topic6Placements = new util.HashMap[Integer, util.List[Integer]]
    topic6Placements.put(1, asList(0, 1))
    topic6Placements.put(2, asList(1, 0))
    val topic7Placements = new util.HashMap[Integer, util.List[Integer]]
    topic7Placements.put(2, asList(0, 1))
    topic7Placements.put(3, asList(1, 0))
    val futures = admin.createTopics(asList(
      new NewTopic("new-topic6", topic6Placements),
      new NewTopic("new-topic7", topic7Placements))).values()
    val topic6Cause = assertThrows(classOf[ExecutionException], () => futures.get("new-topic6").get()).getCause
    assertEquals(classOf[InvalidReplicaAssignmentException], topic6Cause.getClass)
    assertTrue(topic6Cause.getMessage.contains("partitions should be a consecutive 0-based integer sequence"),
      "Unexpected error message: " + topic6Cause.getMessage)
    val topic7Cause = assertThrows(classOf[ExecutionException], () => futures.get("new-topic7").get()).getCause
    assertEquals(classOf[InvalidReplicaAssignmentException], topic7Cause.getClass)
    assertTrue(topic7Cause.getMessage.contains("partitions should be a consecutive 0-based integer sequence"),
      "Unexpected error message: " + topic7Cause.getMessage)
  }

  /**
   * Test that when we supply a manual partition assignment to createPartitions, it must contain
   * enough partitions.
   */
  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testMissingPartitionsInCreatePartitions(quorum: String): Unit = {
    val cause = assertThrows(classOf[ExecutionException], () =>
      admin.createPartitions(Collections.singletonMap(topic1,
        NewPartitions.increaseTo(3, singletonList(asList(0, 1, 2))))).all().get()).getCause
    assertEquals(classOf[InvalidReplicaAssignmentException], cause.getClass)
    assertTrue(cause.getMessage.contains("Attempted to add 2 additional partition(s), but only 1 assignment(s) " +
      "were specified."), "Unexpected error message: " + cause.getMessage)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testIncrementPartitions(quorum: String): Unit = {
    admin.createPartitions(Collections.singletonMap(topic1, NewPartitions.increaseTo(3))).all().get()

    // wait until leader is elected
    waitUntilLeaderIsElectedOrChangedWithAdmin(admin, topic1, 1)
    waitUntilLeaderIsElectedOrChangedWithAdmin(admin, topic1, 2)

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitForPartitionMetadata(brokers, topic1, 1)
    TestUtils.waitForPartitionMetadata(brokers, topic1, 2)
    val response = connectAndReceive[MetadataResponse](
      new MetadataRequest.Builder(Seq(topic1).asJava, false).build)
    assertEquals(1, response.topicMetadata.size)
    val partitions = response.topicMetadata.asScala.head.partitionMetadata.asScala.sortBy(_.partition)
    assertEquals(partitions.size, 3)
    assertEquals(1, partitions(1).partition)
    assertEquals(2, partitions(2).partition)

    for (partition <- partitions) {
      val replicas = partition.replicaIds
      assertEquals(2, replicas.size)
      assertTrue(partition.leaderId.isPresent)
      val leaderId = partition.leaderId.get
      assertTrue(replicas.contains(leaderId))
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testManualAssignmentOfReplicas(quorum: String): Unit = {
    // Add 2 partitions
    admin.createPartitions(Collections.singletonMap(topic2, NewPartitions.increaseTo(3,
      asList(asList(0, 1), asList(2, 3))))).all().get()
    // wait until leader is elected
    val leader1 = waitUntilLeaderIsElectedOrChangedWithAdmin(admin, topic2, 1)
    val leader2 = waitUntilLeaderIsElectedOrChangedWithAdmin(admin, topic2, 2)

    // read metadata from a broker and verify the new topic partitions exist
    val partition1Metadata = TestUtils.waitForPartitionMetadata(brokers, topic2, 1)
    assertEquals(leader1, partition1Metadata.leader())
    val partition2Metadata = TestUtils.waitForPartitionMetadata(brokers, topic2, 2)
    assertEquals(leader2, partition2Metadata.leader())
    val response = connectAndReceive[MetadataResponse](
      new MetadataRequest.Builder(Seq(topic2).asJava, false).build)
    assertEquals(1, response.topicMetadata.size)
    val topicMetadata = response.topicMetadata.asScala.head
    val partitionMetadata = topicMetadata.partitionMetadata.asScala.sortBy(_.partition)
    assertEquals(3, topicMetadata.partitionMetadata.size)
    assertEquals(0, partitionMetadata(0).partition)
    assertEquals(1, partitionMetadata(1).partition)
    assertEquals(2, partitionMetadata(2).partition)
    val replicas = partitionMetadata(1).replicaIds
    assertEquals(2, replicas.size)
    assertEquals(Set(0, 1), replicas.asScala.toSet)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testReplicaPlacementAllServers(quorum: String): Unit = {
    admin.createPartitions(Collections.singletonMap(topic3, NewPartitions.increaseTo(7))).all().get()

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitForPartitionMetadata(brokers, topic3, 1)
    TestUtils.waitForPartitionMetadata(brokers, topic3, 2)
    TestUtils.waitForPartitionMetadata(brokers, topic3, 3)
    TestUtils.waitForPartitionMetadata(brokers, topic3, 4)
    TestUtils.waitForPartitionMetadata(brokers, topic3, 5)
    TestUtils.waitForPartitionMetadata(brokers, topic3, 6)

    val response = connectAndReceive[MetadataResponse](
      new MetadataRequest.Builder(Seq(topic3).asJava, false).build)
    assertEquals(1, response.topicMetadata.size)
    val topicMetadata = response.topicMetadata.asScala.head

    assertEquals(7, topicMetadata.partitionMetadata.size)
    for (partition <- topicMetadata.partitionMetadata.asScala) {
      val replicas = partition.replicaIds.asScala.toSet
      assertEquals(4, replicas.size, s"Partition ${partition.partition} should have 4 replicas")
      assertTrue(replicas.subsetOf(Set(0, 1, 2, 3)), s"Replicas should only include brokers 0-3")
      assertTrue(partition.leaderId.isPresent, s"Partition ${partition.partition} should have a leader")
      assertTrue(replicas.contains(partition.leaderId.get), "Leader should be one of the replicas")
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testReplicaPlacementPartialServers(quorum: String): Unit = {
    admin.createPartitions(Collections.singletonMap(topic2, NewPartitions.increaseTo(3))).all().get()

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitForPartitionMetadata(brokers, topic2, 1)
    TestUtils.waitForPartitionMetadata(brokers, topic2, 2)

    val response = connectAndReceive[MetadataResponse](
      new MetadataRequest.Builder(Seq(topic2).asJava, false).build)
    assertEquals(1, response.topicMetadata.size)
    val topicMetadata = response.topicMetadata.asScala.head

    assertEquals(3, topicMetadata.partitionMetadata.size)
    for (partition <- topicMetadata.partitionMetadata.asScala) {
      val replicas = partition.replicaIds.asScala.toSet
      assertEquals(2, replicas.size, s"Partition ${partition.partition} should have 2 replicas")
      assertTrue(replicas.subsetOf(Set(0, 1, 2, 3)), s"Replicas should only include brokers 0-3")
      assertTrue(partition.leaderId.isPresent, s"Partition ${partition.partition} should have a leader")
      assertTrue(replicas.contains(partition.leaderId.get), "Leader should be one of the replicas")
    }
  }

}
