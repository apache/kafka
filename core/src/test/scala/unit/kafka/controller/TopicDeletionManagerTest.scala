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
package kafka.controller

import kafka.cluster.{Broker, EndPoint}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito._

class TopicDeletionManagerTest {

  private val brokerId = 1
  private val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId, "zkConnect"))
  private val deletionClient = mock(classOf[DeletionClient])

  @Test
  def testInitialization(): Unit = {
    val controllerContext = initContext(
      brokers = Seq(1, 2, 3),
      topics = Set("foo", "bar", "baz"),
      numPartitions = 2,
      replicationFactor = 3)

    val replicaStateMachine = new MockReplicaStateMachine(controllerContext)
    replicaStateMachine.startup()

    val partitionStateMachine = new MockPartitionStateMachine(controllerContext, uncleanLeaderElectionEnabled = false)
    partitionStateMachine.startup()

    val deletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
      partitionStateMachine, deletionClient)

    assertTrue(deletionManager.isDeleteTopicEnabled)
    deletionManager.init(initialTopicsToBeDeleted = Set("foo", "bar"), initialTopicsIneligibleForDeletion = Set("bar", "baz"))

    assertEquals(Set("foo", "bar"), controllerContext.topicsToBeDeleted.toSet)
    assertEquals(Set("bar"), controllerContext.topicsIneligibleForDeletion.toSet)
  }

  @Test
  def testBasicDeletion(): Unit = {
    val controllerContext = initContext(
      brokers = Seq(1, 2, 3),
      topics = Set("foo", "bar"),
      numPartitions = 2,
      replicationFactor = 3)
    val replicaStateMachine = new MockReplicaStateMachine(controllerContext)
    replicaStateMachine.startup()

    val partitionStateMachine = new MockPartitionStateMachine(controllerContext, uncleanLeaderElectionEnabled = false)
    partitionStateMachine.startup()

    val deletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
      partitionStateMachine, deletionClient)
    assertTrue(deletionManager.isDeleteTopicEnabled)
    deletionManager.init(Set.empty, Set.empty)

    val fooPartitions = controllerContext.partitionsForTopic("foo")
    val fooReplicas = controllerContext.replicasForPartition(fooPartitions).toSet
    val barPartitions = controllerContext.partitionsForTopic("bar")
    val barReplicas = controllerContext.replicasForPartition(barPartitions).toSet

    // Clean up state changes before starting the deletion
    replicaStateMachine.clear()
    partitionStateMachine.clear()

    // Queue the topic for deletion
    deletionManager.enqueueTopicsForDeletion(Set("foo", "bar"))

    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    assertEquals(fooReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))
    assertEquals(barPartitions, controllerContext.partitionsInState("bar", NonExistentPartition))
    assertEquals(barReplicas, controllerContext.replicasInState("bar", ReplicaDeletionStarted))
    verify(deletionClient).sendMetadataUpdate(fooPartitions ++ barPartitions)
    assertEquals(Set("foo", "bar"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo", "bar"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)

    // Complete the deletion
    deletionManager.completeReplicaDeletion(fooReplicas ++ barReplicas)

    assertEquals(Set.empty, controllerContext.partitionsForTopic("foo"))
    assertEquals(Set.empty[PartitionAndReplica], controllerContext.replicaStates.keySet.filter(_.topic == "foo"))
    assertEquals(Set.empty, controllerContext.partitionsForTopic("bar"))
    assertEquals(Set.empty[PartitionAndReplica], controllerContext.replicaStates.keySet.filter(_.topic == "bar"))
    assertEquals(Set(), controllerContext.topicsToBeDeleted)
    assertEquals(Set(), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)

    assertEquals(1, partitionStateMachine.stateChangesCalls(OfflinePartition))
    assertEquals(1, partitionStateMachine.stateChangesCalls(NonExistentPartition))

    assertEquals(1, replicaStateMachine.stateChangesCalls(ReplicaDeletionIneligible))
    assertEquals(1, replicaStateMachine.stateChangesCalls(OfflineReplica))
    assertEquals(1, replicaStateMachine.stateChangesCalls(ReplicaDeletionStarted))
    assertEquals(1, replicaStateMachine.stateChangesCalls(ReplicaDeletionSuccessful))
  }

  @Test
  def testDeletionWithBrokerOffline(): Unit = {
    val controllerContext = initContext(
      brokers = Seq(1, 2, 3),
      topics = Set("foo", "bar"),
      numPartitions = 2,
      replicationFactor = 3)

    val replicaStateMachine = new MockReplicaStateMachine(controllerContext)
    replicaStateMachine.startup()

    val partitionStateMachine = new MockPartitionStateMachine(controllerContext, uncleanLeaderElectionEnabled = false)
    partitionStateMachine.startup()

    val deletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
      partitionStateMachine, deletionClient)
    assertTrue(deletionManager.isDeleteTopicEnabled)
    deletionManager.init(Set.empty, Set.empty)

    val fooPartitions = controllerContext.partitionsForTopic("foo")
    val fooReplicas = controllerContext.replicasForPartition(fooPartitions).toSet

    // Broker 2 is taken offline
    val failedBrokerId = 2
    val offlineBroker = controllerContext.liveOrShuttingDownBroker(failedBrokerId).get
    val lastEpoch = controllerContext.liveBrokerIdAndEpochs(failedBrokerId)
    controllerContext.removeLiveBrokers(Set(failedBrokerId))
    assertEquals(Set(1, 3), controllerContext.liveBrokerIds)

    val (offlineReplicas, onlineReplicas) = fooReplicas.partition(_.replica == failedBrokerId)
    replicaStateMachine.handleStateChanges(offlineReplicas.toSeq, OfflineReplica)

    // Start topic deletion
    deletionManager.enqueueTopicsForDeletion(Set("foo"))
    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    verify(deletionClient).sendMetadataUpdate(fooPartitions)
    assertEquals(onlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))
    assertEquals(offlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionIneligible))

    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set("foo"), controllerContext.topicsIneligibleForDeletion)

    // Deletion succeeds for online replicas
    deletionManager.completeReplicaDeletion(onlineReplicas)

    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set("foo"), controllerContext.topicsIneligibleForDeletion)
    assertEquals(onlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionSuccessful))
    assertEquals(offlineReplicas, controllerContext.replicasInState("foo", OfflineReplica))

    // Broker 2 comes back online and deletion is resumed
    controllerContext.addLiveBrokers(Map(offlineBroker -> (lastEpoch + 1L)))
    deletionManager.resumeDeletionForTopics(Set("foo"))

    assertEquals(onlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionSuccessful))
    assertEquals(offlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))

    deletionManager.completeReplicaDeletion(offlineReplicas)
    assertEquals(Set.empty, controllerContext.partitionsForTopic("foo"))
    assertEquals(Set.empty[PartitionAndReplica], controllerContext.replicaStates.keySet.filter(_.topic == "foo"))
    assertEquals(Set(), controllerContext.topicsToBeDeleted)
    assertEquals(Set(), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)
  }

  @Test
  def testBrokerFailureAfterDeletionStarted(): Unit = {
    val controllerContext = initContext(
      brokers = Seq(1, 2, 3),
      topics = Set("foo", "bar"),
      numPartitions = 2,
      replicationFactor = 3)

    val replicaStateMachine = new MockReplicaStateMachine(controllerContext)
    replicaStateMachine.startup()

    val partitionStateMachine = new MockPartitionStateMachine(controllerContext, uncleanLeaderElectionEnabled = false)
    partitionStateMachine.startup()

    val deletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
      partitionStateMachine, deletionClient)
    deletionManager.init(Set.empty, Set.empty)

    val fooPartitions = controllerContext.partitionsForTopic("foo")
    val fooReplicas = controllerContext.replicasForPartition(fooPartitions).toSet

    // Queue the topic for deletion
    deletionManager.enqueueTopicsForDeletion(Set("foo"))
    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    assertEquals(fooReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))

    // Broker 2 fails
    val failedBrokerId = 2
    val offlineBroker = controllerContext.liveOrShuttingDownBroker(failedBrokerId).get
    val lastEpoch = controllerContext.liveBrokerIdAndEpochs(failedBrokerId)
    controllerContext.removeLiveBrokers(Set(failedBrokerId))
    assertEquals(Set(1, 3), controllerContext.liveBrokerIds)
    val (offlineReplicas, onlineReplicas) = fooReplicas.partition(_.replica == failedBrokerId)

    // Fail replica deletion
    deletionManager.failReplicaDeletion(offlineReplicas)
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set("foo"), controllerContext.topicsIneligibleForDeletion)
    assertEquals(offlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionIneligible))
    assertEquals(onlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))

    // Broker 2 is restarted. The offline replicas remain ineligable
    // (TODO: this is probably not desired)
    controllerContext.addLiveBrokers(Map(offlineBroker -> (lastEpoch + 1L)))
    deletionManager.resumeDeletionForTopics(Set("foo"))
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)
    assertEquals(onlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))
    assertEquals(offlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionIneligible))

    // When deletion completes for the replicas which started, then deletion begins for the remaining ones
    deletionManager.completeReplicaDeletion(onlineReplicas)
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)
    assertEquals(onlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionSuccessful))
    assertEquals(offlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))

  }

  def initContext(brokers: Seq[Int],
                  topics: Set[String],
                  numPartitions: Int,
                  replicationFactor: Int): ControllerContext = {
    val context = new ControllerContext
    val brokerEpochs = brokers.map { brokerId =>
      val endpoint = new EndPoint("localhost", 9900 + brokerId, new ListenerName("blah"),
        SecurityProtocol.PLAINTEXT)
      Broker(brokerId, Seq(endpoint), rack = None) -> 1L
    }.toMap
    context.setLiveBrokers(brokerEpochs)

    // Simple round-robin replica assignment
    var leaderIndex = 0
    for (topic <- topics; partitionId <- 0 until numPartitions) {
      val partition = new TopicPartition(topic, partitionId)
      val replicas = (0 until replicationFactor).map { i =>
        val replica = brokers((i + leaderIndex) % brokers.size)
        replica
      }
      context.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(replicas))
      leaderIndex += 1
    }
    context
  }

}
