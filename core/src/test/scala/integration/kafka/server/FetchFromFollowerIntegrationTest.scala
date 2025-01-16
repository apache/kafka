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
package kafka.server

import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin.NewPartitionReassignment
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, RangeAssignor}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.config.ServerLogConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{Disabled, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.{Executors, TimeUnit}
import scala.jdk.CollectionConverters._

class FetchFromFollowerIntegrationTest extends BaseFetchRequestTest {
  val numNodes = 2
  val numParts = 1

  val topic = "test-fetch-from-follower"
  val leaderBrokerId = 0
  val followerBrokerId = 1

  def overridingProps: Properties = {
    val props = new Properties
    props.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, numParts.toString)
    props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, numNodes.toString)
    props
  }

  override def generateConfigs: collection.Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(numNodes, enableControlledShutdown = false, enableFetchFromFollower = true)
      .map(KafkaConfig.fromProps(_, overridingProps))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  @Timeout(15)
  def testFollowerCompleteDelayedFetchesOnReplication(quorum: String, groupProtocol: String): Unit = {
    // Create a topic with 2 replicas where broker 0 is the leader and 1 is the follower.
    val admin = createAdminClient()
    val partitionLeaders = TestUtils.createTopicWithAdmin(
      admin,
      topic,
      brokers,
      controllerServers,
      replicaAssignment = Map(0 -> Seq(leaderBrokerId, followerBrokerId))
    )
    TestUtils.waitUntilLeaderIsKnown(brokers, new TopicPartition(topic, 0))
    assertTrue(partitionLeaders.values.forall(_ == leaderBrokerId))

    val version = ApiKeys.FETCH.latestVersion()
    val topicPartition = new TopicPartition(topic, 0)
    val offsetMap = Map(topicPartition -> 0L)

    // Set fetch.max.wait.ms to a value (20 seconds) greater than the timeout (15 seconds).
    // Send a fetch request before the record is replicated to ensure that the replication
    // triggers purgatory completion.
    val fetchRequest = createConsumerFetchRequest(
      maxResponseBytes = 1000,
      maxPartitionBytes = 1000,
      Seq(topicPartition),
      offsetMap,
      version,
      maxWaitMs = 20000,
      minBytes = 1
    )

    val socket = connect(brokerSocketServer(followerBrokerId))
    try {
      send(fetchRequest, socket)
      TestUtils.generateAndProduceMessages(brokers, topic, numMessages = 1)
      val response = receive[FetchResponse](socket, ApiKeys.FETCH, version)
      assertEquals(Errors.NONE, response.error)
      assertEquals(Map(Errors.NONE -> 2).asJava, response.errorCounts)
    } finally {
      socket.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testFetchFromLeaderWhilePreferredReadReplicaIsUnavailable(quorum: String, groupProtocol: String): Unit = {
    // Create a topic with 2 replicas where broker 0 is the leader and 1 is the follower.
    val admin = createAdminClient()
    TestUtils.createTopicWithAdmin(
      admin,
      topic,
      brokers,
      controllerServers,
      replicaAssignment = Map(0 -> Seq(leaderBrokerId, followerBrokerId))
    )

    TestUtils.generateAndProduceMessages(brokers, topic, numMessages = 10)

    assertEquals(1, getPreferredReplica)

    // Shutdown follower broker.
    brokers(followerBrokerId).shutdown()
    val topicPartition = new TopicPartition(topic, 0)
    TestUtils.waitUntilTrue(() => {
      val endpoints = brokers(leaderBrokerId).metadataCache.getPartitionReplicaEndpoints(topicPartition, listenerName)
      !endpoints.contains(followerBrokerId)
    }, "follower is still reachable.")

    assertEquals(-1, getPreferredReplica)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testFetchFromFollowerWithRoll(quorum: String, groupProtocol: String): Unit = {
    // Create a topic with 2 replicas where broker 0 is the leader and 1 is the follower.
    val admin = createAdminClient()
    TestUtils.createTopicWithAdmin(
      admin,
      topic,
      brokers,
      controllerServers,
      replicaAssignment = Map(0 -> Seq(leaderBrokerId, followerBrokerId))
    )

    // Create consumer with client.rack = follower id.
    val consumerProps = new Properties
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(ConsumerConfig.CLIENT_RACK_CONFIG, followerBrokerId.toString)
    consumerProps.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol)
    val consumer = new KafkaConsumer(consumerProps, new ByteArrayDeserializer, new ByteArrayDeserializer)
    try {
      consumer.subscribe(List(topic).asJava)

      // Wait until preferred replica is set to follower.
      TestUtils.waitUntilTrue(() => {
        getPreferredReplica == 1
      }, "Preferred replica is not set")

      // Produce and consume.
      TestUtils.generateAndProduceMessages(brokers, topic, numMessages = 1)
      TestUtils.pollUntilAtLeastNumRecords(consumer, 1)

      // Shutdown follower, produce and consume should work.
      brokers(followerBrokerId).shutdown()
      TestUtils.generateAndProduceMessages(brokers, topic, numMessages = 1)
      TestUtils.pollUntilAtLeastNumRecords(consumer, 1)

      // Start the follower and wait until preferred replica is set to follower.
      brokers(followerBrokerId).startup()
      TestUtils.waitUntilTrue(() => {
        getPreferredReplica == 1
      }, "Preferred replica is not set")

      // Produce and consume should still work.
      TestUtils.generateAndProduceMessages(brokers, topic, numMessages = 1)
      TestUtils.pollUntilAtLeastNumRecords(consumer, 1)
    } finally {
      consumer.close()
    }
  }

  @Disabled
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testRackAwareRangeAssignor(quorum: String, groupProtocol: String): Unit = {
    val partitionList = brokers.indices.toList

    val topicWithAllPartitionsOnAllRacks = "topicWithAllPartitionsOnAllRacks"
    createTopic(topicWithAllPartitionsOnAllRacks, brokers.size, brokers.size)

    // Racks are in order of broker ids, assign leaders in reverse order
    val topicWithSingleRackPartitions = "topicWithSingleRackPartitions"
    createTopicWithAssignment(topicWithSingleRackPartitions, partitionList.map(i => (i, Seq(brokers.size - i - 1))).toMap)

    // Create consumers with instance ids in ascending order, with racks in the same order.
    consumerConfig.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[RangeAssignor].getName)
    val consumers = brokers.map { server =>
      consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      consumerConfig.setProperty(ConsumerConfig.CLIENT_RACK_CONFIG, server.config.rack.orNull)
      consumerConfig.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, s"instance-${server.config.brokerId}")
      consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000")
      createConsumer()
    }

    val producer = createProducer()
    val executor = Executors.newFixedThreadPool(consumers.size)

    def verifyAssignments(partitionOrder: List[Int], topics: String*): Unit = {
      val assignments = partitionOrder.map { p =>
        topics.map(topic => new TopicPartition(topic, p)).toSet
      }

      val assignmentFutures = consumers.zipWithIndex.map { case (consumer, i) =>
        executor.submit(() => {
          val expectedAssignment = assignments(i)
          TestUtils.pollUntilTrue(consumer, () => consumer.assignment() == expectedAssignment.asJava,
            s"Timed out while awaiting expected assignment $expectedAssignment. The current assignment is ${consumer.assignment()}",
            waitTimeMs = 30000)
        }, 0)
      }
      assignmentFutures.foreach(future => assertEquals(0, future.get(30, TimeUnit.SECONDS)))

      assignments.flatten.foreach { tp =>
        producer.send(new ProducerRecord(tp.topic, tp.partition, s"key-$tp".getBytes, s"value-$tp".getBytes))
      }

      val recordFutures = consumers.zipWithIndex.map { case (consumer, i) =>
        executor.submit(() => {
          TestUtils.pollUntilAtLeastNumRecords(consumer, assignments(i).size, waitTimeMs = 30000)
        })
      }
      recordFutures.zipWithIndex.foreach { case (future, i) =>
        val records = future.get(30, TimeUnit.SECONDS)
        assertEquals(assignments(i), records.map(r => new TopicPartition(r.topic, r.partition)).toSet)
      }
    }


    try {
      // Rack-based assignment results in partitions assigned in reverse order since partition racks are in the reverse order.
      consumers.foreach(_.subscribe(Collections.singleton(topicWithSingleRackPartitions)))
      verifyAssignments(partitionList.reverse, topicWithSingleRackPartitions)

      // Non-rack-aware assignment results in ordered partitions.
      consumers.foreach(_.subscribe(Collections.singleton(topicWithAllPartitionsOnAllRacks)))
      verifyAssignments(partitionList, topicWithAllPartitionsOnAllRacks)

      // Rack-aware assignment with co-partitioning results in reverse assignment for both topics.
      consumers.foreach(_.subscribe(Set(topicWithSingleRackPartitions, topicWithAllPartitionsOnAllRacks).asJava))
      verifyAssignments(partitionList.reverse, topicWithAllPartitionsOnAllRacks, topicWithSingleRackPartitions)

      // Perform reassignment for topicWithSingleRackPartitions to reverse the replica racks and
      // verify that change in replica racks results in re-assignment based on new racks.
      val admin = createAdminClient()
      val reassignments = new util.HashMap[TopicPartition, util.Optional[NewPartitionReassignment]]()
      partitionList.foreach { p =>
        val newAssignment = new NewPartitionReassignment(Collections.singletonList(p))
        reassignments.put(new TopicPartition(topicWithSingleRackPartitions, p), util.Optional.of(newAssignment))
      }
      admin.alterPartitionReassignments(reassignments).all().get(30, TimeUnit.SECONDS)
      verifyAssignments(partitionList, topicWithAllPartitionsOnAllRacks, topicWithSingleRackPartitions)

    } finally {
      executor.shutdownNow()
    }
  }

  private def getPreferredReplica: Int = {
    val topicPartition = new TopicPartition(topic, 0)
    val offsetMap = Map(topicPartition -> 0L)

    val request = createConsumerFetchRequest(
      maxResponseBytes = 1000,
      maxPartitionBytes = 1000,
      Seq(topicPartition),
      offsetMap,
      ApiKeys.FETCH.latestVersion,
      maxWaitMs = 500,
      minBytes = 1,
      rackId = followerBrokerId.toString
    )
    val response = connectAndReceive[FetchResponse](request, brokers(leaderBrokerId).socketServer)
    assertEquals(Errors.NONE, response.error)
    assertEquals(Map(Errors.NONE -> 2).asJava, response.errorCounts)
    assertEquals(1, response.data.responses.size)
    val topicResponse = response.data.responses.get(0)
    assertEquals(1, topicResponse.partitions.size)

    topicResponse.partitions.get(0).preferredReadReplica
  }
}
