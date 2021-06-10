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

package kafka.controller

import kafka.cluster.Broker
import kafka.utils.Implicits._
import org.apache.kafka.common.{TopicPartition, Uuid}

import scala.collection.{Map, Seq, Set, mutable}

object ReplicaAssignment {
  def apply(replicas: Seq[Int]): ReplicaAssignment = {
    apply(replicas, Seq.empty, Seq.empty)
  }

  val empty: ReplicaAssignment = apply(Seq.empty)
}


/**
 * @param replicas         the sequence of brokers assigned to the partition. It includes the set of brokers
 *                         that were added (`addingReplicas`) and removed (`removingReplicas`).
 * @param addingReplicas   the replicas that are being added if there is a pending reassignment
 * @param removingReplicas the replicas that are being removed if there is a pending reassignment
 */

/**
 * 分区副本详情(正常+非正常)
 *
 * @param replicas         完成的副本列表     1,2,3
 * @param addingReplicas   正在添加的副本列表 2,
 * @param removingReplicas 正在移除的副本列表 3
 */
case class ReplicaAssignment private(replicas: Seq[Int],
                                     addingReplicas: Seq[Int],
                                     removingReplicas: Seq[Int]) {
  // 1 3
  lazy val originReplicas: Seq[Int] = replicas.diff(addingReplicas)

  // 1 2
  lazy val targetReplicas: Seq[Int] = replicas.diff(removingReplicas)

  def isBeingReassigned: Boolean = {
    addingReplicas.nonEmpty || removingReplicas.nonEmpty
  }

  def reassignTo(target: Seq[Int]): ReplicaAssignment = {
    val fullReplicaSet = (target ++ originReplicas).distinct
    ReplicaAssignment(
      fullReplicaSet,
      fullReplicaSet.diff(originReplicas),
      fullReplicaSet.diff(target)
    )
  }

  def removeReplica(replica: Int): ReplicaAssignment = {
    ReplicaAssignment(
      replicas.filterNot(_ == replica),
      addingReplicas.filterNot(_ == replica),
      removingReplicas.filterNot(_ == replica)
    )
  }

  override def toString: String = s"ReplicaAssignment(" +
    s"replicas=${replicas.mkString(",")}, " +
    s"addingReplicas=${addingReplicas.mkString(",")}, " +
    s"removingReplicas=${removingReplicas.mkString(",")})"
}

/**
 * 保存Controller元数据的容器，所有的元数据信息都封装在这个类中
 *
 * ① Broker节点
 * ② 新增、删除主题
 * ③ 主题下的分区
 *
 */
class ControllerContext {
  // Controller统计信息类
  val stats = new ControllerStats

  // 离线分区计数器（统计类参数）
  var offlinePartitionCount = 0

  // 满足Preferred Leader选举条件的总分区数
  // Preferred Leader是指Leader ID 是否和ISR集合的第一个ID相等
  var preferredReplicaImbalanceCount = 0

  // 处于关闭中Broker的ID集合
  val shuttingDownBrokerIds = mutable.Set.empty[Int]

  // 处于正常运行中Broker集合
  private val liveBrokers = mutable.Set.empty[Broker]

  /**
   * 处于正常运行中Broker的Epoch列表，
   */
  private val liveBrokerEpochs = mutable.Map.empty[Int, Long]

  // Controller当前Epoch值，实际就是Zookeeper节点/controller_epoch节点的值
  // 可以认为是Controller版本号，可以防止Zombie
  var epoch: Int = KafkaController.InitialControllerEpoch

  // Controller对应Zookeeper节点的Epoch，实际上是Zookeeper节点/controller_epoch节点的dataVersion值
  // Kafka 使用 epochZkVersion 来判断和防止 Zombie Controller。
  // 这也就是说，原先在老 Controller 任期内的 Controller 操作在新 Controller 不能成功执行，
  // 因为新 Controller 的 epochZkVersion 要比老 Controller 的大。
  var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion

  // 集群中所有主题列表
  val allTopics = mutable.Set.empty[String]

  // 集群中所有主题的ID
  var topicIds = mutable.Map.empty[String, Uuid]

  // 集群中所有主题的名称
  var topicNames = mutable.Map.empty[Uuid, String]

  // 分区的副本列表 <主题名称,<分区ID, 该分区所有副本详情>>
  val partitionAssignments = mutable.Map.empty[String, mutable.Map[Int, ReplicaAssignment]]

  // 分区的Leader/ISR副本信息 <分区, Leader副本、ISR集合、Controller版本号>
  private val partitionLeadershipInfo = mutable.Map.empty[TopicPartition, LeaderIsrAndControllerEpoch]

  // 集群正处于「分区重平衡」过程的主题分区列表
  val partitionsBeingReassigned = mutable.Set.empty[TopicPartition]

  // 主题分区状态列表
  val partitionStates = mutable.Map.empty[TopicPartition, PartitionState]

  // 主题分区的副本状态列表
  val replicaStates = mutable.Map.empty[PartitionAndReplica, ReplicaState]

  // key：broker id，value：分区详情。表示某个broker中不可用的分区列表
  val replicasOnOfflineDirs = mutable.Map.empty[Int, Set[TopicPartition]]

  // 待删除主题列表
  val topicsToBeDeleted = mutable.Set.empty[String]

  /**
   * 已开启删除的主题列表
   * The following topicsWithDeletionStarted variable is used to properly update the offlinePartitionCount metric.
   * When a topic is going through deletion, we don't want to keep track of its partition state
   * changes in the offlinePartitionCount metric. This goal means if some partitions of a topic are already
   * in OfflinePartition state when deletion starts, we need to change the corresponding partition
   * states to NonExistentPartition first before starting the deletion.
   *
   * However we can NOT change partition states to NonExistentPartition at the time of enqueuing topics
   * for deletion. The reason is that when a topic is enqueued for deletion, it may be ineligible for
   * deletion due to ongoing partition reassignments. Hence there might be a delay between enqueuing
   * a topic for deletion and the actual start of deletion. In this delayed interval, partitions may still
   * transition to or out of the OfflinePartition state.
   *
   * Hence we decide to change partition states to NonExistentPartition only when the actual deletion have started.
   * For topics whose deletion have actually started, we keep track of them in the following topicsWithDeletionStarted
   * variable. And once a topic is in the topicsWithDeletionStarted set, we are sure there will no longer
   * be partition reassignments to any of its partitions, and only then it's safe to move its partitions to
   * NonExistentPartition state. Once a topic is in the topicsWithDeletionStarted set, we will stop monitoring
   * its partition state changes in the offlinePartitionCount metric
   */
  val topicsWithDeletionStarted = mutable.Set.empty[String]

  // 暂时无法执行删除操作的主题列表
  val topicsIneligibleForDeletion = mutable.Set.empty[String]

  private def clearTopicsState(): Unit = {
    allTopics.clear()
    topicIds.clear()
    topicNames.clear()
    partitionAssignments.clear()
    partitionLeadershipInfo.clear()
    partitionsBeingReassigned.clear()
    replicasOnOfflineDirs.clear()
    partitionStates.clear()
    offlinePartitionCount = 0
    preferredReplicaImbalanceCount = 0
    replicaStates.clear()
  }

  def addTopicId(topic: String, id: Uuid): Unit = {
    if (!allTopics.contains(topic))
      throw new IllegalStateException(s"topic $topic is not contained in all topics.")

    topicIds.get(topic).foreach { existingId =>
      if (!existingId.equals(id))
        throw new IllegalStateException(s"topic ID map already contained ID for topic " +
          s"$topic and new ID $id did not match existing ID $existingId")
    }
    topicNames.get(id).foreach { existingName =>
      if (!existingName.equals(topic))
        throw new IllegalStateException(s"topic name map already contained ID " +
          s"$id and new name $topic did not match existing name $existingName")
    }
    topicIds.put(topic, id)
    topicNames.put(id, topic)
  }

  def partitionReplicaAssignment(topicPartition: TopicPartition): Seq[Int] = {
    partitionAssignments.getOrElse(topicPartition.topic, mutable.Map.empty).get(topicPartition.partition) match {
      case Some(partitionAssignment) => partitionAssignment.replicas
      case None => Seq.empty
    }
  }

  def partitionFullReplicaAssignment(topicPartition: TopicPartition): ReplicaAssignment = {
    partitionAssignments.getOrElse(topicPartition.topic, mutable.Map.empty)
      .getOrElse(topicPartition.partition, ReplicaAssignment.empty)
  }

  /**
   *
   * @param topicPartition 分区详情
   * @param newAssignment  分区副本元数据
   */
  def updatePartitionFullReplicaAssignment(topicPartition: TopicPartition, newAssignment: ReplicaAssignment): Unit = {
    // #1 从缓存中获取已存在的分区副本详情（Map）
    val assignments = partitionAssignments.getOrElseUpdate(topicPartition.topic, mutable.Map.empty)

    // #2 更新缓存
    val previous = assignments.put(topicPartition.partition, newAssignment)
    val leadershipInfo = partitionLeadershipInfo.get(topicPartition)
    updatePreferredReplicaImbalanceMetric(topicPartition, previous, leadershipInfo,
      Some(newAssignment), leadershipInfo)
  }

  def partitionReplicaAssignmentForTopic(topic: String): Map[TopicPartition, Seq[Int]] = {
    partitionAssignments.getOrElse(topic, Map.empty).map {
      case (partition, assignment) => (new TopicPartition(topic, partition), assignment.replicas)
    }.toMap
  }

  def partitionFullReplicaAssignmentForTopic(topic: String): Map[TopicPartition, ReplicaAssignment] = {
    partitionAssignments.getOrElse(topic, Map.empty).map {
      case (partition, assignment) => (new TopicPartition(topic, partition), assignment)
    }.toMap
  }

  def allPartitions: Set[TopicPartition] = {
    partitionAssignments.flatMap {
      case (topic, topicReplicaAssignment) => topicReplicaAssignment.map {
        case (partition, _) => new TopicPartition(topic, partition)
      }
    }.toSet
  }

  def setLiveBrokers(brokerAndEpochs: Map[Broker, Long]): Unit = {
    clearLiveBrokers()
    addLiveBrokers(brokerAndEpochs)
  }

  private def clearLiveBrokers(): Unit = {
    liveBrokers.clear()
    liveBrokerEpochs.clear()
  }

  def addLiveBrokers(brokerAndEpochs: Map[Broker, Long]): Unit = {
    liveBrokers ++= brokerAndEpochs.keySet
    liveBrokerEpochs ++= brokerAndEpochs.map { case (broker, brokerEpoch) => (broker.id, brokerEpoch) }
  }

  def removeLiveBrokers(brokerIds: Set[Int]): Unit = {
    liveBrokers --= liveBrokers.filter(broker => brokerIds.contains(broker.id))
    liveBrokerEpochs --= brokerIds
  }

  /**
   * 更新Broker的元数据，Broker元数据是由<Id, EndPoint, 机架信息>的三元组，
   * 新增/移除Broker->Zookeeper更新其保存的Broker数据->引发Controler修改元数据->
   * updateBrokerMetadata
   *
   * @param oldMetadata
   * @param newMetadata
   */
  def updateBrokerMetadata(oldMetadata: Broker, newMetadata: Broker): Unit = {
    liveBrokers -= oldMetadata
    liveBrokers += newMetadata
  }

  // getter
  def liveBrokerIds: Set[Int] = liveBrokerEpochs.keySet.diff(shuttingDownBrokerIds)

  def liveOrShuttingDownBrokerIds: Set[Int] = liveBrokerEpochs.keySet

  def liveOrShuttingDownBrokers: Set[Broker] = liveBrokers

  def liveBrokerIdAndEpochs: Map[Int, Long] = liveBrokerEpochs

  def liveOrShuttingDownBroker(brokerId: Int): Option[Broker] = liveOrShuttingDownBrokers.find(_.id == brokerId)

  def partitionsOnBroker(brokerId: Int): Set[TopicPartition] = {
    partitionAssignments.flatMap {
      case (topic, topicReplicaAssignment) => topicReplicaAssignment.filter {
        case (_, partitionAssignment) => partitionAssignment.replicas.contains(brokerId)
      }.map {
        case (partition, _) => new TopicPartition(topic, partition)
      }
    }.toSet
  }

  /**
   * 判断分区副本是否处于「在线」状态，
   *
   * @param brokerId                   Broker ID
   * @param topicPartition             分区详情
   * @param includeShuttingDownBrokers 是否包含「正在关闭中」的Broker
   * @return
   */
  def isReplicaOnline(brokerId: Int, topicPartition: TopicPartition, includeShuttingDownBrokers: Boolean = false): Boolean = {
    val brokerOnline = {
      if (includeShuttingDownBrokers) liveOrShuttingDownBrokerIds.contains(brokerId)
      else liveBrokerIds.contains(brokerId)
    }
    // Broker在线且replicasOnOfflineDirs不包含分区，说明分区所在的副本是在线的
    brokerOnline && !replicasOnOfflineDirs.getOrElse(brokerId, Set.empty).contains(topicPartition)
  }

  def replicasOnBrokers(brokerIds: Set[Int]): Set[PartitionAndReplica] = {
    brokerIds.flatMap { brokerId =>
      partitionAssignments.flatMap {
        case (topic, topicReplicaAssignment) => topicReplicaAssignment.collect {
          case (partition, partitionAssignment) if partitionAssignment.replicas.contains(brokerId) =>
            PartitionAndReplica(new TopicPartition(topic, partition), brokerId)
        }
      }
    }
  }

  def replicasForTopic(topic: String): Set[PartitionAndReplica] = {
    partitionAssignments.getOrElse(topic, mutable.Map.empty).flatMap {
      case (partition, assignment) => assignment.replicas.map { r =>
        PartitionAndReplica(new TopicPartition(topic, partition), r)
      }
    }.toSet
  }

  def partitionsForTopic(topic: String): collection.Set[TopicPartition] = {
    partitionAssignments.getOrElse(topic, mutable.Map.empty).map {
      case (partition, _) => new TopicPartition(topic, partition)
    }.toSet
  }

  /**
   * 获取集群中所有在线（online）和离线（offline）的副本集合
   *
   * @return 一个Tuple对象，第一个参数是集群中所有的在线副本集合，第二个参数是所有的离线副本集合
   */
  def onlineAndOfflineReplicas: (Set[PartitionAndReplica], Set[PartitionAndReplica]) = {
    // 保存在线副本
    val onlineReplicas = mutable.Set.empty[PartitionAndReplica]
    // 保存离线副本
    val offlineReplicas = mutable.Set.empty[PartitionAndReplica]

    // 遍历「partitionAssignments」缓存，获取以上两个集合
    for ((topic, partitionAssignments) <- partitionAssignments;
         (partitionId, assignment) <- partitionAssignments) {
      // 创建分区详情对象
      val partition = new TopicPartition(topic, partitionId)

      // 遍历副本
      for (replica <- assignment.replicas) {
        // 构建分区副本对象，副本的ID号其实就是所在的Broker ID号
        val partitionAndReplica = PartitionAndReplica(partition, replica)
        // 判断副本是否在线
        if (isReplicaOnline(replica, partition))
          onlineReplicas.add(partitionAndReplica)
        else
          offlineReplicas.add(partitionAndReplica)
      }
    }
    (onlineReplicas, offlineReplicas)
  }

  def replicasForPartition(partitions: collection.Set[TopicPartition]): collection.Set[PartitionAndReplica] = {
    partitions.flatMap { p =>
      val replicas = partitionReplicaAssignment(p)
      replicas.map(PartitionAndReplica(p, _))
    }
  }

  def resetContext(): Unit = {
    topicsToBeDeleted.clear()
    topicsWithDeletionStarted.clear()
    topicsIneligibleForDeletion.clear()
    shuttingDownBrokerIds.clear()
    epoch = 0
    epochZkVersion = 0
    clearTopicsState()
    clearLiveBrokers()
  }

  def setAllTopics(topics: Set[String]): Unit = {
    allTopics.clear()
    allTopics ++= topics
  }

  def removeTopic(topic: String): Unit = {
    // Metric is cleaned when the topic is queued up for deletion so
    // we don't clean it twice. We clean it only if it is deleted
    // directly.
    if (!topicsToBeDeleted.contains(topic))
      cleanPreferredReplicaImbalanceMetric(topic)
    topicsToBeDeleted -= topic
    topicsWithDeletionStarted -= topic
    allTopics -= topic
    topicIds.remove(topic).foreach { topicId =>
      topicNames.remove(topicId)
    }
    partitionAssignments.remove(topic).foreach { assignments =>
      assignments.keys.foreach { partition =>
        partitionLeadershipInfo.remove(new TopicPartition(topic, partition))
      }
    }
  }

  def queueTopicDeletion(topics: Set[String]): Unit = {
    topicsToBeDeleted ++= topics
    topics.foreach(cleanPreferredReplicaImbalanceMetric)
  }

  def beginTopicDeletion(topics: Set[String]): Unit = {
    topicsWithDeletionStarted ++= topics
  }

  def isTopicDeletionInProgress(topic: String): Boolean = {
    topicsWithDeletionStarted.contains(topic)
  }

  def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    topicsToBeDeleted.contains(topic)
  }

  def isTopicEligibleForDeletion(topic: String): Boolean = {
    topicsToBeDeleted.contains(topic) && !topicsIneligibleForDeletion.contains(topic)
  }

  def topicsQueuedForDeletion: Set[String] = {
    topicsToBeDeleted
  }

  def replicasInState(topic: String, state: ReplicaState): Set[PartitionAndReplica] = {
    replicasForTopic(topic).filter(replica => replicaStates(replica) == state).toSet
  }

  def areAllReplicasInState(topic: String, state: ReplicaState): Boolean = {
    replicasForTopic(topic).forall(replica => replicaStates(replica) == state)
  }

  def isAnyReplicaInState(topic: String, state: ReplicaState): Boolean = {
    replicasForTopic(topic).exists(replica => replicaStates(replica) == state)
  }

  def checkValidReplicaStateChange(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): (Seq[PartitionAndReplica], Seq[PartitionAndReplica]) = {
    replicas.partition(replica => isValidReplicaStateTransition(replica, targetState))
  }

  def checkValidPartitionStateChange(partitions: Seq[TopicPartition], targetState: PartitionState): (Seq[TopicPartition], Seq[TopicPartition]) = {
    partitions.partition(p => isValidPartitionStateTransition(p, targetState))
  }

  def putReplicaState(replica: PartitionAndReplica, state: ReplicaState): Unit = {
    replicaStates.put(replica, state)
  }

  def removeReplicaState(replica: PartitionAndReplica): Unit = {
    replicaStates.remove(replica)
  }

  def putReplicaStateIfNotExists(replica: PartitionAndReplica, state: ReplicaState): Unit = {
    replicaStates.getOrElseUpdate(replica, state)
  }

  def putPartitionState(partition: TopicPartition, targetState: PartitionState): Unit = {
    val currentState = partitionStates.put(partition, targetState).getOrElse(NonExistentPartition)
    updatePartitionStateMetrics(partition, currentState, targetState)
  }

  /**
   * 更新分区状态指标，实际就是更新「offlinePartitionCount」数量
   *
   * @param partition    分区详情（主题名称+分区ID）
   * @param currentState 当前分区状态
   * @param targetState  目标状态
   */
  private def updatePartitionStateMetrics(partition: TopicPartition,
                                          currentState: PartitionState,
                                          targetState: PartitionState): Unit = {
    // 首先判断主题并不处于「删除中」状态
    if (!isTopicDeletionInProgress(partition.topic)) {
      if (currentState != OfflinePartition && targetState == OfflinePartition) {
        // 目标状态为离线状态，则offlinePartitionCount+1
        offlinePartitionCount = offlinePartitionCount + 1
      } else if (currentState == OfflinePartition && targetState != OfflinePartition) {
        // 如果原分区状态为离线状态，目标状态为「非离线状态」，则offlinePartitionCount-1
        offlinePartitionCount = offlinePartitionCount - 1
      }
    }
  }

  def putPartitionStateIfNotExists(partition: TopicPartition, state: PartitionState): Unit = {
    if (partitionStates.getOrElseUpdate(partition, state) == state)
      updatePartitionStateMetrics(partition, NonExistentPartition, state)
  }

  def replicaState(replica: PartitionAndReplica): ReplicaState = {
    replicaStates(replica)
  }

  def partitionState(partition: TopicPartition): PartitionState = {
    partitionStates(partition)
  }

  def partitionsInState(state: PartitionState): Set[TopicPartition] = {
    partitionStates.filter { case (_, s) => s == state }.keySet.toSet
  }

  def partitionsInStates(states: Set[PartitionState]): Set[TopicPartition] = {
    partitionStates.filter { case (_, s) => states.contains(s) }.keySet.toSet
  }

  def partitionsInState(topic: String, state: PartitionState): Set[TopicPartition] = {
    partitionsForTopic(topic).filter { partition => state == partitionState(partition) }.toSet
  }

  def partitionsInStates(topic: String, states: Set[PartitionState]): Set[TopicPartition] = {
    partitionsForTopic(topic).filter { partition => states.contains(partitionState(partition)) }.toSet
  }

  def putPartitionLeadershipInfo(partition: TopicPartition,
                                 leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch): Unit = {
    val previous = partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
    val replicaAssignment = partitionFullReplicaAssignment(partition)
    updatePreferredReplicaImbalanceMetric(partition, Some(replicaAssignment), previous,
      Some(replicaAssignment), Some(leaderIsrAndControllerEpoch))
  }

  def partitionLeadershipInfo(partition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    partitionLeadershipInfo.get(partition)
  }

  def partitionsLeadershipInfo: Map[TopicPartition, LeaderIsrAndControllerEpoch] =
    partitionLeadershipInfo

  def partitionsWithLeaders: Set[TopicPartition] =
    partitionLeadershipInfo.keySet.filter(tp => !isTopicQueuedUpForDeletion(tp.topic))

  /**
   *
   * @return
   */
  def partitionsWithOfflineLeader: Set[TopicPartition] = {
    partitionLeadershipInfo.filter { case (topicPartition, leaderIsrAndControllerEpoch) =>
      // 判断副本是否离线
      !isReplicaOnline(leaderIsrAndControllerEpoch.leaderAndIsr.leader, topicPartition) &&
        // 判断副本对应的主题是否处于「待删除」队列中
        !isTopicQueuedUpForDeletion(topicPartition.topic)
    }.keySet
  }

  def partitionLeadersOnBroker(brokerId: Int): Set[TopicPartition] = {
    partitionLeadershipInfo.filter { case (topicPartition, leaderIsrAndControllerEpoch) =>
      !isTopicQueuedUpForDeletion(topicPartition.topic) &&
        leaderIsrAndControllerEpoch.leaderAndIsr.leader == brokerId &&
        partitionReplicaAssignment(topicPartition).size > 1
    }.keySet
  }

  def topicName(topicId: Uuid): Option[String] = {
    topicNames.get(topicId)
  }

  def clearPartitionLeadershipInfo(): Unit = partitionLeadershipInfo.clear()

  def partitionWithLeadersCount: Int = partitionLeadershipInfo.size

  /**
   *
   * @param partition
   * @param oldReplicaAssignment
   * @param oldLeadershipInfo
   * @param newReplicaAssignment
   * @param newLeadershipInfo
   */
  private def updatePreferredReplicaImbalanceMetric(partition: TopicPartition,
                                                    oldReplicaAssignment: Option[ReplicaAssignment],
                                                    oldLeadershipInfo: Option[LeaderIsrAndControllerEpoch],
                                                    newReplicaAssignment: Option[ReplicaAssignment],
                                                    newLeadershipInfo: Option[LeaderIsrAndControllerEpoch]): Unit = {
    // #1 判断主题是否处于「正在删除中」
    if (!isTopicQueuedUpForDeletion(partition.topic)) {
      // 不处于，
      oldReplicaAssignment.foreach { replicaAssignment =>
        oldLeadershipInfo.foreach { leadershipInfo =>
          if (!hasPreferredLeader(replicaAssignment, leadershipInfo))
            preferredReplicaImbalanceCount -= 1
        }
      }

      newReplicaAssignment.foreach { replicaAssignment =>
        newLeadershipInfo.foreach { leadershipInfo =>
          if (!hasPreferredLeader(replicaAssignment, leadershipInfo))
            preferredReplicaImbalanceCount += 1
        }
      }
    }
  }

  private def cleanPreferredReplicaImbalanceMetric(topic: String): Unit = {
    partitionAssignments.getOrElse(topic, mutable.Map.empty).forKeyValue { (partition, replicaAssignment) =>
      partitionLeadershipInfo.get(new TopicPartition(topic, partition)).foreach { leadershipInfo =>
        if (!hasPreferredLeader(replicaAssignment, leadershipInfo))
          preferredReplicaImbalanceCount -= 1
      }
    }
  }

  private def hasPreferredLeader(replicaAssignment: ReplicaAssignment,
                                 leadershipInfo: LeaderIsrAndControllerEpoch): Boolean = {
    val preferredReplica = replicaAssignment.replicas.head
    if (replicaAssignment.isBeingReassigned && replicaAssignment.addingReplicas.contains(preferredReplica))
    // reassigning partitions are not counted as imbalanced until the new replica joins the ISR (completes reassignment)
      !leadershipInfo.leaderAndIsr.isr.contains(preferredReplica)
    else
      leadershipInfo.leaderAndIsr.leader == preferredReplica
  }

  private def isValidReplicaStateTransition(replica: PartitionAndReplica, targetState: ReplicaState): Boolean =
    targetState.validPreviousStates.contains(replicaStates(replica))

  private def isValidPartitionStateTransition(partition: TopicPartition, targetState: PartitionState): Boolean =
    targetState.validPreviousStates.contains(partitionStates(partition))

}
