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

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito._

class TopicDeletionManagerTest {

  private val brokerId = 1
  private val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId, "zkConnect"))
  private val deletionClient = mock(classOf[DeletionClient])

  @Test
  def testInitialization(): Unit = {
    val controllerContext = TestUtils.initContext(
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
    val controllerContext = TestUtils.initContext(
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

    // Queue the topic for deletion
    deletionManager.enqueueTopicsForDeletion(Set("foo"))

    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    assertEquals(fooReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))
    verify(deletionClient).sendMetadataUpdate(fooPartitions)
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)

    // Complete the deletion
    deletionManager.completeReplicaDeletion(fooReplicas)

    assertEquals(Set.empty, controllerContext.partitionsForTopic("foo"))
    assertEquals(Set.empty[PartitionAndReplica], controllerContext.replicaStates.keySet.filter(_.topic == "foo"))
    assertEquals(Set(), controllerContext.topicsToBeDeleted)
    assertEquals(Set(), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)
  }

  @Test
  def testDeletionWithBrokerOffline(): Unit = {
    val controllerContext = TestUtils.initContext(
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
    controllerContext.addLiveBrokersAndEpochs(Map(offlineBroker -> (lastEpoch + 1L)))
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
    val controllerContext = TestUtils.initContext(
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
    controllerContext.addLiveBrokersAndEpochs(Map(offlineBroker -> (lastEpoch + 1L)))
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
}
