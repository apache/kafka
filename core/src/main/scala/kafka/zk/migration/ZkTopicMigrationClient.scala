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

package kafka.zk.migration

import kafka.api.LeaderAndIsr
import kafka.controller.{LeaderIsrAndControllerEpoch, ReplicaAssignment}
import kafka.server.ConfigType
import kafka.utils.Logging
import kafka.zk.TopicZNode.TopicIdReplicaAssignment
import kafka.zk.ZkMigrationClient.{logAndRethrow, wrapZkException}
import kafka.zk._
import kafka.zookeeper.{CreateRequest, DeleteRequest, GetChildrenRequest, SetDataRequest}
import org.apache.kafka.common.metadata.PartitionRecord
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.metadata.migration.TopicMigrationClient.TopicVisitorInterest
import org.apache.kafka.metadata.migration.{MigrationClientException, TopicMigrationClient, ZkMigrationLeadershipState}
import org.apache.kafka.metadata.{LeaderRecoveryState, PartitionRegistration}
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.Code

import java.util
import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._


class ZkTopicMigrationClient(zkClient: KafkaZkClient) extends TopicMigrationClient with Logging {
  override def iterateTopics(
    interests: util.EnumSet[TopicVisitorInterest],
    visitor: TopicMigrationClient.TopicVisitor,
  ): Unit = wrapZkException {
    if (!interests.contains(TopicVisitorInterest.TOPICS)) {
      throw new IllegalArgumentException("Must specify at least TOPICS in topic visitor interests.")
    }
    val allTopics = zkClient.getAllTopicsInCluster()
    val topicDeletions = readPendingTopicDeletions().asScala
    val topicsToMigrated = allTopics -- topicDeletions
    if (topicDeletions.nonEmpty) {
      warn(s"Found ${topicDeletions.size} pending topic deletions. These will be not migrated " +
        s"to KRaft. After the migration, the brokers will reconcile their logs with these pending topic deletions.")
    }
    topicDeletions.foreach {
      deletion => logger.info(s"Not migrating pending deleted topic: $deletion")
    }
    val replicaAssignmentAndTopicIds = zkClient.getReplicaAssignmentAndTopicIdForTopics(topicsToMigrated)
    replicaAssignmentAndTopicIds.foreach { case TopicIdReplicaAssignment(topic, topicIdOpt, partitionAssignments) =>
      val topicAssignment = partitionAssignments.map { case (partition, assignment) =>
        partition.partition().asInstanceOf[Integer] -> assignment.replicas.map(Integer.valueOf).asJava
      }.toMap.asJava
      logAndRethrow(this, s"Error in topic consumer. Topic was $topic.") {
        visitor.visitTopic(topic, topicIdOpt.get, topicAssignment)
      }
      if (interests.contains(TopicVisitorInterest.PARTITIONS)) {
        val partitions = partitionAssignments.keys.toSeq
        val leaderIsrAndControllerEpochs = zkClient.getTopicPartitionStates(partitions)
        partitionAssignments.foreach { case (topicPartition, replicaAssignment) =>
          val replicaList = replicaAssignment.replicas.map(Integer.valueOf).asJava
          val record = new PartitionRecord()
            .setTopicId(topicIdOpt.get)
            .setPartitionId(topicPartition.partition)
            .setReplicas(replicaList)
            .setAddingReplicas(replicaAssignment.addingReplicas.map(Integer.valueOf).asJava)
            .setRemovingReplicas(replicaAssignment.removingReplicas.map(Integer.valueOf).asJava)
          leaderIsrAndControllerEpochs.get(topicPartition) match {
            case Some(leaderIsrAndEpoch) =>
              record
                .setIsr(leaderIsrAndEpoch.leaderAndIsr.isr.map(Integer.valueOf).asJava)
                .setLeader(leaderIsrAndEpoch.leaderAndIsr.leader)
                .setLeaderEpoch(leaderIsrAndEpoch.leaderAndIsr.leaderEpoch)
                .setPartitionEpoch(leaderIsrAndEpoch.leaderAndIsr.partitionEpoch)
                .setLeaderRecoveryState(leaderIsrAndEpoch.leaderAndIsr.leaderRecoveryState.value())
            case None =>
              warn(s"Could not find partition state in ZK for $topicPartition. Initializing this partition " +
                s"with ISR={$replicaList} and leaderEpoch=0.")
              record
                .setIsr(replicaList)
                .setLeader(replicaList.get(0))
                .setLeaderEpoch(0)
                .setPartitionEpoch(0)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value())
          }
          logAndRethrow(this, s"Error in partition consumer. TopicPartition was $topicPartition.") {
            visitor.visitPartition(new TopicIdPartition(topicIdOpt.get, topicPartition), new PartitionRegistration(record))
          }
        }
      }
    }
  }

  override def createTopic(
    topicName: String,
    topicId: Uuid,
    partitions: util.Map[Integer, PartitionRegistration],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {

    val assignments = partitions.asScala.map { case (partitionId, partition) =>
      new TopicPartition(topicName, partitionId) ->
        ReplicaAssignment(partition.replicas, partition.addingReplicas, partition.removingReplicas)
    }

    val createTopicZNode = {
      val path = TopicZNode.path(topicName)
      CreateRequest(
        path,
        TopicZNode.encode(Some(topicId), assignments),
        zkClient.defaultAcls(path),
        CreateMode.PERSISTENT)
    }
    val createPartitionZNodeReqs = createTopicPartitionZNodesRequests(topicName, partitions, state)

    val requests = Seq(createTopicZNode) ++ createPartitionZNodeReqs
    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests, state)
    val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
    if (resultCodes(TopicZNode.path(topicName)).equals(Code.NODEEXISTS)) {
      // topic already created, just return
      state
    } else if (resultCodes.forall { case (_, code) => code.equals(Code.OK) }) {
      // ok
      state.withMigrationZkVersion(migrationZkVersion)
    } else {
      // not ok
      throw new MigrationClientException(s"Failed to create or update topic $topicName. ZK operations had results $resultCodes")
    }
  }

  private def createTopicPartitionZNodesRequests(
    topicName: String,
    partitions: util.Map[Integer, PartitionRegistration],
    state: ZkMigrationLeadershipState
  ): Seq[CreateRequest] = {
    val createPartitionsZNode = {
      val path = TopicPartitionsZNode.path(topicName)
      CreateRequest(
        path,
        null,
        zkClient.defaultAcls(path),
        CreateMode.PERSISTENT)
    }

    val createPartitionZNodeReqs = partitions.asScala.toSeq.flatMap { case (partitionId, partition) =>
      val topicPartition = new TopicPartition(topicName, partitionId)
      Seq(
        createTopicPartition(topicPartition),
        createTopicPartitionState(topicPartition, partition, state.kraftControllerEpoch())
      )
    }

    Seq(createPartitionsZNode) ++ createPartitionZNodeReqs
  }

  private def recursiveChildren(path: String, acc: ArrayBuffer[String]): Unit = {
    val topicChildZNodes = zkClient.retryRequestUntilConnected(GetChildrenRequest(path, registerWatch = false))
    topicChildZNodes.children.foreach { child =>
      recursiveChildren(s"$path/$child", acc)
      acc.append(s"$path/$child")
    }
  }

  private def recursiveChildren(path: String): Seq[String] = {
    val buffer = new ArrayBuffer[String]()
    recursiveChildren(path, buffer)
    buffer.toSeq
  }

  override def updateTopic(
    topicName: String,
    topicId: Uuid,
    partitions: util.Map[Integer, PartitionRegistration],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    val assignments = partitions.asScala.map { case (partitionId, partition) =>
      new TopicPartition(topicName, partitionId) ->
        ReplicaAssignment(partition.replicas, partition.addingReplicas, partition.removingReplicas)
    }
    val request = SetDataRequest(
      TopicZNode.path(topicName),
      TopicZNode.encode(Some(topicId), assignments),
      ZkVersion.MatchAnyVersion
    )
    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(Seq(request), state)
    val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
    if (resultCodes.forall { case (_, code) => code.equals(Code.OK) } ) {
      state.withMigrationZkVersion(migrationZkVersion)
    } else {
      throw new MigrationClientException(s"Failed to update topic metadata: $topicName. ZK transaction had results $resultCodes")
    }
  }

  override def deleteTopic(
    topicName: String,
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    // Delete the partition state ZNodes recursively, then topic config, and finally the topic znode
    val topicPath = TopicZNode.path(topicName)
    val topicChildZNodes = recursiveChildren(topicPath)
    val deleteRequests = topicChildZNodes.map { childPath =>
      DeleteRequest(childPath, ZkVersion.MatchAnyVersion)
    } ++ Seq(
      DeleteRequest(ConfigEntityZNode.path(ConfigType.Topic, topicName), ZkVersion.MatchAnyVersion),
      DeleteRequest(TopicZNode.path(topicName), ZkVersion.MatchAnyVersion)
    )

    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(deleteRequests, state)
    val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
    if (responses.last.resultCode.equals(Code.OK)) {
      state.withMigrationZkVersion(migrationZkVersion)
    } else {
      throw new MigrationClientException(s"Failed to delete topic $topicName. ZK operations had results $resultCodes")
    }
  }

  override def createTopicPartitions(topicPartitions: util.Map[String, util.Map[Integer, PartitionRegistration]], state: ZkMigrationLeadershipState)
  :ZkMigrationLeadershipState = wrapZkException {
    val requests = topicPartitions.asScala.toSeq.flatMap { case (topicName, partitions) =>
      createTopicPartitionZNodesRequests(topicName, partitions, state)
    }

    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests, state)
    val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
    if (resultCodes.forall { case (_, code) => code.equals(Code.OK) || code.equals(Code.NODEEXISTS) }) {
      state.withMigrationZkVersion(migrationZkVersion)
    } else {
      throw new MigrationClientException(s"Failed to create partition states: $topicPartitions. ZK transaction had results $resultCodes")
    }
  }

  override def updateTopicPartitions(
    topicPartitions: util.Map[String, util.Map[Integer, PartitionRegistration]],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    val requests = topicPartitions.asScala.flatMap { case (topicName, partitionRegistrations) =>
      partitionRegistrations.asScala.flatMap { case (partitionId, partitionRegistration) =>
        val topicPartition = new TopicPartition(topicName, partitionId)
        Seq(updateTopicPartitionState(topicPartition, partitionRegistration, state.kraftControllerEpoch()))
      }
    }
    if (requests.isEmpty) {
      state
    } else {
      val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests.toSeq, state)
      val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
      if (resultCodes.forall { case (_, code) => code.equals(Code.OK) }) {
        state.withMigrationZkVersion(migrationZkVersion)
      } else {
        throw new MigrationClientException(s"Failed to update partition states: $topicPartitions. ZK transaction had results $resultCodes")
      }
    }
  }

  override def deleteTopicPartitions(
    topicPartitions: util.Map[String, util.Set[Integer]],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = {
    val requests = topicPartitions.asScala.flatMap { case (topicName, partitionIds) =>
      partitionIds.asScala.map { partitionId =>
        val topicPartition = new TopicPartition(topicName, partitionId)
        val path = TopicPartitionZNode.path(topicPartition)
        DeleteRequest(path, ZkVersion.MatchAnyVersion)
      }
    }
    if (requests.isEmpty) {
      state
    } else {
      val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests.toSeq, state)
      val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
      if (resultCodes.forall { case (_, code) => code.equals(Code.OK) }) {
        state.withMigrationZkVersion(migrationZkVersion)
      } else {
        throw new MigrationClientException(s"Failed to delete partition states: $topicPartitions. ZK transaction had results $resultCodes")
      }
    }
  }

  private def createTopicPartition(
    topicPartition: TopicPartition
  ): CreateRequest = wrapZkException {
    val path = TopicPartitionZNode.path(topicPartition)
    CreateRequest(path, null, zkClient.defaultAcls(path), CreateMode.PERSISTENT, Some(topicPartition))
  }

  private def partitionStatePathAndData(
    topicPartition: TopicPartition,
    partitionRegistration: PartitionRegistration,
    controllerEpoch: Int
  ): (String, Array[Byte]) = {
    val path = TopicPartitionStateZNode.path(topicPartition)
    val data = TopicPartitionStateZNode.encode(LeaderIsrAndControllerEpoch(LeaderAndIsr(
      partitionRegistration.leader,
      partitionRegistration.leaderEpoch,
      partitionRegistration.isr.toList,
      partitionRegistration.leaderRecoveryState,
      partitionRegistration.partitionEpoch), controllerEpoch))
    (path, data)
  }

  private def createTopicPartitionState(
    topicPartition: TopicPartition,
    partitionRegistration: PartitionRegistration,
    controllerEpoch: Int
  ): CreateRequest = {
    val (path, data) = partitionStatePathAndData(topicPartition, partitionRegistration, controllerEpoch)
    CreateRequest(path, data, zkClient.defaultAcls(path), CreateMode.PERSISTENT, Some(topicPartition))
  }

  private def updateTopicPartitionState(
    topicPartition: TopicPartition,
    partitionRegistration: PartitionRegistration,
    controllerEpoch: Int
  ): SetDataRequest = {
    val (path, data) = partitionStatePathAndData(topicPartition, partitionRegistration, controllerEpoch)
    SetDataRequest(path, data, ZkVersion.MatchAnyVersion, Some(topicPartition))
  }

  override def readPendingTopicDeletions(): util.Set[String] = {
    zkClient.getTopicDeletions.toSet.asJava
  }

  override def clearPendingTopicDeletions(
    pendingTopicDeletions: util.Set[String],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = {
    val deleteRequests = pendingTopicDeletions.asScala.map { topicName =>
      DeleteRequest(DeleteTopicsTopicZNode.path(topicName), ZkVersion.MatchAnyVersion)
    }.toSeq

    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(deleteRequests.toSeq, state)
    val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
    if (resultCodes.forall { case (_, code) => code.equals(Code.OK) }) {
      state.withMigrationZkVersion(migrationZkVersion)
    } else {
      throw new MigrationClientException(s"Failed to delete pending topic deletions: $pendingTopicDeletions. ZK transaction had results $resultCodes")
    }
  }
}
