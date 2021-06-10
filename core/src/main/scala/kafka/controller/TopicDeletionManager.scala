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
package kafka.controller

import kafka.server.KafkaConfig
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition

import scala.collection.Set
import scala.collection.mutable

/**
 * 定义主题删除操作API，比如删除主题、更新元数据等
 */
trait DeletionClient {
  /**
   * 删除主题
   * @param topic           待删除主题的名称
   * @param epochZkVersion  zk版本号
   */
  def deleteTopic(topic: String, epochZkVersion: Int): Unit

  /**
   *
   * @param topics
   * @param epochZkVersion
   */
  def deleteTopicDeletions(topics: Seq[String], epochZkVersion: Int): Unit

  /**
   *
   * @param topic
   */
  def mutePartitionModifications(topic: String): Unit

  /**
   *
   * @param partitions
   */
  def sendMetadataUpdate(partitions: Set[TopicPartition]): Unit
}

/**
 * 接口 {@link DeletionClient} 实现类
 * @param controller    Kafka controller对象
 * @param zkClient      zookeeper客户端
 */
class ControllerDeletionClient(controller: KafkaController, zkClient: KafkaZkClient)
  extends DeletionClient {

  /**
   * 删除zookeeper相关的节点数据
   * @param topic           待删除主题的名称
   * @param epochZkVersion  zk版本号
   */
  override def deleteTopic(topic: String, epochZkVersion: Int): Unit = {
    // #1 删除「/brokers/topics/<topic>」节点
    zkClient.deleteTopicZNode(topic, epochZkVersion)

    // #2 删除「/config/topic/<topic>」节点
    zkClient.deleteTopicConfigs(Seq(topic), epochZkVersion)

    // #3 删除「/admin/delete_topic/<topic>」节点
    zkClient.deleteTopicDeletions(Seq(topic), epochZkVersion)
  }

  /**
   * 删除「/admin/delete_topic」下给定的topic子结点
   * @param topics
   * @param epochZkVersion
   */
  override def deleteTopicDeletions(topics: Seq[String], epochZkVersion: Int): Unit = {
    zkClient.deleteTopicDeletions(topics, epochZkVersion)
  }

  /**
   * 取消「/brokers/topics/<topic>」节点数据变更的监听器，
   * 为是为了避免新增和删除这两类操作起冲突，避免互联干扰。比如用户A发起删除操作，用户新增该主题下的分区，为了应对这种情况，
   * 在移除主题副本和分区对象前，代码先要执行这个方法，以确保不再响应用户对该主题的其它操作，让删除操作得以安心进行。
   *
   * @param topic
   */
  override def mutePartitionModifications(topic: String): Unit = {
    controller.unregisterPartitionModificationsHandlers(Seq(topic))
  }

  /**
   * 向集群广播指定分区的元数据更新请求
   *
   * @param partitions
   */
  override def sendMetadataUpdate(partitions: Set[TopicPartition]): Unit = {
    controller.sendUpdateMetadataRequest(controller.controllerContext.liveOrShuttingDownBrokerIds.toSeq, partitions)
  }
}

/**
 * 用于主题删除的状态机。
 *
 * 在zk路径下「/admin/delete_topics/<topic>」路径下创建节点，节点值为待删除的topic名称，zk会触发监听器事件，
 * controller感知并调用相关handler处理。
 * 当然，出于某些情况，该节点上并非所有主题都可立即删除，不符合删除的场景有以下几种情况：
 * ① 主题中某个副本所在的broker宕机，该主题不可删除。
 * ② 该主题处于分区重平衡过程，因此该主题也不可删除。
 *
 * 将在以下场景中恢复主题删除动作：
 * ① 其中一个副本所在的broker启动。
 * ② 分区重平衡完成。
 *
 * 被删除的主题的每个副本都处于3种状态中的一种：
 * ① 当 {@link TopicDeletionManager.onPartitionDeletion()} 被调用时，「TopicDeletionStarted」状态的副本进入TopicDeletionStarted队列。
 *    这是由zk节点「/admin/delete_topics」监听器所触发。在这个队列，controller会向所有的副本所在的broker发送「StopReplicaRequests」请求，
 *    如果deletePartition=true，那么就会为「StopReplicaResponse」响应注册一个回调方法，因此，当成功收到所有副本的响应后就会执行删除副本操作。
 * ② TopicDeletionStarted->TopicDeletionSuccessful：这种状态变更取决于StopReplicaResponse的响应错误码。
 * ③ TopicDeletionStarted->TopicDeletionFailed：取决于响应错误码，一般来说，如果一个broker离线并且它持有待删除主题的副本，
 *    controller在 onBrokerFailure 回调方法中把相应的副本标记为「TopicDeletionFailed」状态。
 *    这是因为如果在发送请求之前、副本处于「TopicDeletionStarted」状态之后，broker 发生故障，
 *    那么副本有可能错误地保持在「TopicDeletionStarted」状态，当broker重新启动时，将不会对这个主题进行删除重试操作。
 *
 * 只有当所有副本都处于「TopicDeletionSuccessful」状态时，才标志着一个主题成功删除，这一步会将controllercontext和zookeeper相关的缓存以及元数据
 * 都删除。另一方面，如果没有副本处于「TopicDeletionStarted」状态，并且至少有一个副本处于「TopicDeletionFailed」状态，那么它就会标记该主题进行删除重试。
 *
 * @param config                      Kafka配置类，目的是获取「delete.topic.enable」的值
 * @param controllerContext           Controller端保存的元数据信息，删除主题必要要变更集群元数据信息，
 * @param replicaStateMachine         副本状态机，负责副本的状态转换，以保持副本对象在集群中的状态一致性
 * @param partitionStateMachine       分区状态机，负责分区状态转换，以保持分区对象在集群中的状态一致性
 * @param client                      用于执行Zookeeper节点操作
 */
class TopicDeletionManager(config: KafkaConfig,
                           controllerContext: ControllerContext,
                           replicaStateMachine: ReplicaStateMachine,
                           partitionStateMachine: PartitionStateMachine,
                           client: DeletionClient) extends Logging {
  this.logIdent = s"[Topic Deletion Manager ${config.brokerId}] "

  // 表明主题是否允许被删除，由「delete.topic.enable」值指定，默认值：true
  val isDeleteTopicEnabled: Boolean = config.deleteTopicEnable

  /**
   * 初始化
   * @param initialTopicsToBeDeleted              初始化待删除的主题列表
   * @param initialTopicsIneligibleForDeletion    初始化暂时无法删除的主题，它是上面集合的一部分
   */
  def init(initialTopicsToBeDeleted: Set[String], initialTopicsIneligibleForDeletion: Set[String]): Unit = {
    info(s"Initializing manager with initial deletions: $initialTopicsToBeDeleted, " +
      s"initial ineligible deletions: $initialTopicsIneligibleForDeletion")

    if (isDeleteTopicEnabled) {
      // 加入缓存
      controllerContext.queueTopicDeletion(initialTopicsToBeDeleted)
      controllerContext.topicsIneligibleForDeletion ++= initialTopicsIneligibleForDeletion & controllerContext.topicsToBeDeleted
    } else {
      // if delete topic is disabled clean the topic entries under /admin/delete_topics
      info(s"Removing $initialTopicsToBeDeleted since delete topic is disabled")
      client.deleteTopicDeletions(initialTopicsToBeDeleted.toSeq, controllerContext.epochZkVersion)
    }
  }

  def tryTopicDeletion(): Unit = {
    if (isDeleteTopicEnabled) {
      resumeDeletions()
    }
  }

  /**
   * 将待删除的主题名称放入队列中，只有当主题完全删除后才会从队列中移除，
   * 换句话说，即该主题的所有分区的所有副本被成功删除，才会从列表中删除
   * @param topics Topics that should be deleted
   */
  def enqueueTopicsForDeletion(topics: Set[String]): Unit = {
    if (isDeleteTopicEnabled) {
      controllerContext.queueTopicDeletion(topics)
      resumeDeletions()
    }
  }

  /**
   * {@link TopicDeletionManager} 核心方法：重启主题删除操作。
   * 由相关事件触发，包括：
   * ① 新的Broker启动。如果有待删除主题的分区在这个新的Broker上的话，就会重启主题删除操作。
   * ② 分区重平衡完成。
   *
   * @param topics Topics for which deletion can be resumed
   */
  def resumeDeletionForTopics(topics: Set[String] = Set.empty): Unit = {
    if (isDeleteTopicEnabled) {
      // #1 获取并集
      val topicsToResumeDeletion = topics & controllerContext.topicsToBeDeleted
      if (topicsToResumeDeletion.nonEmpty) {
        // #2
        controllerContext.topicsIneligibleForDeletion --= topicsToResumeDeletion
        resumeDeletions()
      }
    }
  }

  /**
   * Invoked when a broker that hosts replicas for topics to be deleted goes down. Also invoked when the callback for
   * StopReplicaResponse receives an error code for the replicas of a topic to be deleted. As part of this, the replicas
   * are moved from ReplicaDeletionStarted to ReplicaDeletionIneligible state. Also, the topic is added to the list of topics
   * ineligible for deletion until further notice.
   * @param replicas Replicas for which deletion has failed
   */
  def failReplicaDeletion(replicas: Set[PartitionAndReplica]): Unit = {
    if (isDeleteTopicEnabled) {
      val replicasThatFailedToDelete = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
      if (replicasThatFailedToDelete.nonEmpty) {
        val topics = replicasThatFailedToDelete.map(_.topic)
        debug(s"Deletion failed for replicas ${replicasThatFailedToDelete.mkString(",")}. Halting deletion for topics $topics")
        replicaStateMachine.handleStateChanges(replicasThatFailedToDelete.toSeq, ReplicaDeletionIneligible)
        markTopicIneligibleForDeletion(topics, reason = "replica deletion failure")
        resumeDeletions()
      }
    }
  }

  /**
   * Halt delete topic if -
   * 1. replicas being down
   * 2. partition reassignment in progress for some partitions of the topic
   * @param topics Topics that should be marked ineligible for deletion. No op if the topic is was not previously queued up for deletion
   */
  def markTopicIneligibleForDeletion(topics: Set[String], reason: => String): Unit = {
    if (isDeleteTopicEnabled) {
      val newTopicsToHaltDeletion = controllerContext.topicsToBeDeleted & topics
      controllerContext.topicsIneligibleForDeletion ++= newTopicsToHaltDeletion
      if (newTopicsToHaltDeletion.nonEmpty)
        info(s"Halted deletion of topics ${newTopicsToHaltDeletion.mkString(",")} due to $reason")
    }
  }

  private def isTopicIneligibleForDeletion(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      controllerContext.topicsIneligibleForDeletion.contains(topic)
    } else
      true
  }

  private def isTopicDeletionInProgress(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      controllerContext.isAnyReplicaInState(topic, ReplicaDeletionStarted)
    } else
      false
  }

  def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      controllerContext.isTopicQueuedUpForDeletion(topic)
    } else
      false
  }

  /**
   * Invoked by the StopReplicaResponse callback when it receives no error code for a replica of a topic to be deleted.
   * As part of this, the replicas are moved from ReplicaDeletionStarted to ReplicaDeletionSuccessful state. Tears down
   * the topic if all replicas of a topic have been successfully deleted
   * @param replicas Replicas that were successfully deleted by the broker
   */
  def completeReplicaDeletion(replicas: Set[PartitionAndReplica]): Unit = {
    val successfullyDeletedReplicas = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
    debug(s"Deletion successfully completed for replicas ${successfullyDeletedReplicas.mkString(",")}")
    replicaStateMachine.handleStateChanges(successfullyDeletedReplicas.toSeq, ReplicaDeletionSuccessful)
    resumeDeletions()
  }

  /**
   * Topic deletion can be retried if -
   * 1. Topic deletion is not already complete
   * 2. Topic deletion is currently not in progress for that topic
   * 3. Topic is currently marked ineligible for deletion
   * @param topic Topic
   * @return Whether or not deletion can be retried for the topic
   */
  private def isTopicEligibleForDeletion(topic: String): Boolean = {
    controllerContext.isTopicQueuedUpForDeletion(topic) &&
      !isTopicDeletionInProgress(topic) &&
      !isTopicIneligibleForDeletion(topic)
  }

  /**
   * 通过将对应主题副本的状态从「ReplicaDeletionIneligible」变更到「OfflineReplica」来完成的，
   * 这样，后续再调用resumeDeletions时会尝试重新删除主题
   *
   * If the topic is queued for deletion but deletion is not currently under progress,
   * then deletion is retried for that topic
   * To ensure a successful retry, reset states for respective replicas from ReplicaDeletionIneligible
   * to OfflineReplica state
   * @param topics Topics for which deletion should be retried
   */
  private def retryDeletionForIneligibleReplicas(topics: Set[String]): Unit = {
    // reset replica states from ReplicaDeletionIneligible to OfflineReplica
    val failedReplicas = topics.flatMap(controllerContext.replicasInState(_, ReplicaDeletionIneligible))
    debug(s"Retrying deletion of topics ${topics.mkString(",")} since replicas ${failedReplicas.mkString(",")} were not successfully deleted")
    replicaStateMachine.handleStateChanges(failedReplicas.toSeq, OfflineReplica)
  }

  /**
   * 完成主题删除操作，
   * 当完全收到所有副本的响应后，才会调用这个方法
   * @param topic 待删除主题名称
   */
  private def completeDeleteTopic(topic: String): Unit = {
    // deregister partition change listener on the deleted topic. This is to prevent the partition change listener
    // firing before the new topic listener when a deleted topic gets auto created

    // #1 移除监听器，避免其它请求的干扰
    client.mutePartitionModifications(topic)

    // #2 从缓存中获取已删除的副本列表
    val replicasForDeletedTopic = controllerContext.replicasInState(topic, ReplicaDeletionSuccessful)
    // controller will remove this replica from the state machine as well as its partition assignment cache

    // #3 交给副本状态机变更相关状态，=>「NonExistentReplica」状态
    replicaStateMachine.handleStateChanges(replicasForDeletedTopic.toSeq, NonExistentReplica)

    // #4 删除Zookeeper相关节点
    client.deleteTopic(topic, controllerContext.epochZkVersion)

    // #5 从Controller缓存中移除该主题缓存数据
    controllerContext.removeTopic(topic)
  }

  /**
   * Invoked with the list of topics to be deleted
   * It invokes onPartitionDeletion for all partitions of a topic.
   * The updateMetadataRequest is also going to set the leader for the topics being deleted to
   * {@link LeaderAndIsr#LeaderDuringDelete}. This lets each broker know that this topic is being deleted and can be
   * removed from their caches.
   */
  private def onTopicDeletion(topics: Set[String]): Unit = {
    // #1 找出给定待删除主题列表中那些尚未启动删除操作的所有主题
    val unseenTopicsForDeletion = topics.diff(controllerContext.topicsWithDeletionStarted)
    if (unseenTopicsForDeletion.nonEmpty) {
      // #2 获取这些主题的所有分区对象
      val unseenPartitionsForDeletion = unseenTopicsForDeletion.flatMap(controllerContext.partitionsForTopic)
      // #3 将这些分区的状态依次调整为「OfflinePartition」和「NonExistentPartition」
      partitionStateMachine.handleStateChanges(unseenPartitionsForDeletion.toSeq, OfflinePartition)
      partitionStateMachine.handleStateChanges(unseenPartitionsForDeletion.toSeq, NonExistentPartition)
      // adding of unseenTopicsForDeletion to topics with deletion started must be done after the partition
      // state changes to make sure the offlinePartitionCount metric is properly updated

      // #4 把这些主题回到「已开启删除操作」主题列表中
      controllerContext.beginTopicDeletion(unseenTopicsForDeletion)
    }

    // send update metadata so that brokers stop serving data for topics to be deleted
    // #5 给集群所有Broker发送元数据更新请求，告知它们不用再处理这些主题数据
    client.sendMetadataUpdate(topics.flatMap(controllerContext.partitionsForTopic))

    // #6 分区删除操作会执行底层的物理磁盘文件删除动作
    onPartitionDeletion(topics)
  }

  /**
   * Invoked by onTopicDeletion with the list of partitions for topics to be deleted
   * It does the following -
   * 1. Move all dead replicas directly to ReplicaDeletionIneligible state. Also mark the respective topics ineligible
   *    for deletion if some replicas are dead since it won't complete successfully anyway
   * 2. Move all replicas for the partitions to OfflineReplica state. This will send StopReplicaRequest to the replicas
   *    and LeaderAndIsrRequest to the leader with the shrunk ISR. When the leader replica itself is moved to OfflineReplica state,
   *    it will skip sending the LeaderAndIsrRequest since the leader will be updated to -1
   * 3. Move all replicas to ReplicaDeletionStarted state. This will send StopReplicaRequest with deletePartition=true. And
   *    will delete all persistent data from all replicas of the respective partitions
   */
  private def onPartitionDeletion(topicsToBeDeleted: Set[String]): Unit = {
    val allDeadReplicas = mutable.ListBuffer.empty[PartitionAndReplica]
    val allReplicasForDeletionRetry = mutable.ListBuffer.empty[PartitionAndReplica]
    val allTopicsIneligibleForDeletion = mutable.Set.empty[String]

    topicsToBeDeleted.foreach { topic =>
      val (aliveReplicas, deadReplicas) = controllerContext.replicasForTopic(topic).partition { r =>
        controllerContext.isReplicaOnline(r.replica, r.topicPartition)
      }

      val successfullyDeletedReplicas = controllerContext.replicasInState(topic, ReplicaDeletionSuccessful)
      val replicasForDeletionRetry = aliveReplicas.diff(successfullyDeletedReplicas)

      allDeadReplicas ++= deadReplicas
      allReplicasForDeletionRetry ++= replicasForDeletionRetry

      if (deadReplicas.nonEmpty) {
        debug(s"Dead Replicas (${deadReplicas.mkString(",")}) found for topic $topic")
        allTopicsIneligibleForDeletion += topic
      }
    }

    // move dead replicas directly to failed state
    replicaStateMachine.handleStateChanges(allDeadReplicas, ReplicaDeletionIneligible)
    // send stop replica to all followers that are not in the OfflineReplica state so they stop sending fetch requests to the leader
    replicaStateMachine.handleStateChanges(allReplicasForDeletionRetry, OfflineReplica)
    replicaStateMachine.handleStateChanges(allReplicasForDeletionRetry, ReplicaDeletionStarted)

    if (allTopicsIneligibleForDeletion.nonEmpty) {
      markTopicIneligibleForDeletion(allTopicsIneligibleForDeletion, reason = "offline replicas")
    }
  }

  /**
   * 核心方法：恢复删除主题操作
   */
  private def resumeDeletions(): Unit = {
    // #1 从元数据缓存中获取待删除的主题列表。
    //    并非集合中所有的主题都满足删除条件，下面需要进行一一判断
    val topicsQueuedForDeletion = Set.empty[String] ++ controllerContext.topicsToBeDeleted

    // #2 待重试主题列表
    val topicsEligibleForRetry = mutable.Set.empty[String]

    // #3 待删除主题列表
    val topicsEligibleForDeletion = mutable.Set.empty[String]

    if (topicsQueuedForDeletion.nonEmpty)
      info(s"Handling deletion for topics ${topicsQueuedForDeletion.mkString(",")}")

    // #4 遍历每个待删除主题，根据主题所有的副本状态进行判断并分类
    topicsQueuedForDeletion.foreach { topic =>
      // #4-1 如果该主题所有副本已经是「ReplicaDeletionSuccessful」状态，那么这个主题就可以被删除了
      if (controllerContext.areAllReplicasInState(topic, ReplicaDeletionSuccessful)) {
        // #4-2 清理关于这个主题的所有缓存以及Zookeeper中的数据
        completeDeleteTopic(topic)
        info(s"Deletion of topic $topic successfully completed")
      } else if (!controllerContext.isAnyReplicaInState(topic, ReplicaDeletionStarted)) {
        // #4-3 说明没有副本处于「TopicDeletionStarted」状态且并非所有副本都处于「TopicDeletionSuccessful」，
        //      说明还需要等待未处于「TopicDeletionSuccessful」状态的副本成功变更状态。
        //      这意味着可能给定的主题还未初始化删除主题操作或至少有一个副本删除失败，等待重试。
        if (controllerContext.isAnyReplicaInState(topic, ReplicaDeletionIneligible)) {
          // 加入重试列表中
          topicsEligibleForRetry += topic
        }
      }

      // #4-4 如果该主题能够被删除，则添加到「topicsEligibleForDeletion」列表中
      if (isTopicEligibleForDeletion(topic)) {
        info(s"Deletion of topic $topic (re)started")
        topicsEligibleForDeletion += topic
      }
    }

    // #5 重试「topicsEligibleForRetry」重试动作
    if (topicsEligibleForRetry.nonEmpty) {
      retryDeletionForIneligibleReplicas(topicsEligibleForRetry)
    }

    // #6 对「topicsEligibleForDeletion」列表中的主题进行删除操作
    if (topicsEligibleForDeletion.nonEmpty) {
      onTopicDeletion(topicsEligibleForDeletion)
    }
  }
}
