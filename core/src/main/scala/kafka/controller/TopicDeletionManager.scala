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

//负责实现删除主题以及后续的动作，比如更新元数据等。
// 这个接口里定义了 4 个方法，分别是 deleteTopic、deleteTopicDeletions、mutePartitionModifications 和 sendMetadataUpdate。
trait DeletionClient {
  def deleteTopic(topic: String, epochZkVersion: Int): Unit
  def deleteTopicDeletions(topics: Seq[String], epochZkVersion: Int): Unit
  def mutePartitionModifications(topic: String): Unit
  def sendMetadataUpdate(partitions: Set[TopicPartition]): Unit
}

//实现 DeletionClient 接口的类，分别实现了刚刚说到的那 4 个方法。
// 构造函数的两个字段：KafkaController 实例和 KafkaZkClient 实例。KafkaController 实例，我们已经很熟悉了，就是 Controller 组件对象；
// 而 KafkaZkClient 实例，就是 Kafka 与 ZooKeeper 交互的客户端对象。
class ControllerDeletionClient(controller: KafkaController, zkClient: KafkaZkClient) extends DeletionClient {
  // 删除给定主题
  // 它用于删除主题在 ZooKeeper 上的所有“痕迹”。
  // 具体方法是，分别调用 KafkaZkClient 的 3 个方法去删除 ZooKeeper 下 /brokers/topics/节点、/config/topics/节点和 /admin/delete_topics/节点。
  override def deleteTopic(topic: String, epochZkVersion: Int): Unit = {
    // 删除/brokers/topics/节点
    zkClient.deleteTopicZNode(topic, epochZkVersion)
    // 删除/config/topics/节点
    zkClient.deleteTopicConfigs(Seq(topic), epochZkVersion)
    // 删除/admin/delete_topics/节点
    zkClient.deleteTopicDeletions(Seq(topic), epochZkVersion)
  }
  /**
   * 删除/admin/delete_topics下的给定topic子节点
   * 它用于删除 ZooKeeper 下待删除主题的标记节点。
   * 具体方法是，调用 KafkaZkClient 的 deleteTopicDeletions 方法，批量删除一组主题在 /admin/delete_topics 下的子节点。
   * 注意，deleteTopicDeletions 这个方法名结尾的 Deletions，表示 /admin/delete_topics 下的子节点。
   * 所以，deleteTopic 是删除主题，deleteTopicDeletions 是删除 /admin/delete_topics 下的对应子节点。
   * 到这里，我们还要注意的一点是，这两个方法里都有一个 epochZkVersion 的字段，代表期望的 Controller Epoch 版本号。
   * 如果你使用一个旧的 Epoch 版本号执行这些方法，ZooKeeper 会拒绝，因为和它自己保存的版本号不匹配。
   * 如果一个 Controller 的 Epoch 值小于 ZooKeeper 中保存的，那么这个 Controller 很可能是已经过期的 Controller。
   * 这种 Controller 就被称为 Zombie Controller。epochZkVersion 字段的作用，就是隔离 Zombie Controller 发送的操作。
   * */
  override def deleteTopicDeletions(topics: Seq[String], epochZkVersion: Int): Unit = {
    zkClient.deleteTopicDeletions(topics, epochZkVersion)
  }

  /**
   * 取消/brokers/topics/节点数据变更的监听
   * 它的作用是屏蔽主题分区数据变更监听器，具体实现原理其实就是取消 /brokers/topics/节点数据变更的监听。
   * 这样当该主题的分区数据发生变更后，由于对应的 ZooKeeper 监听器已经被取消了，因此不会触发 Controller 相应的处理逻辑。
   * 为什么要取消这个监听器呢？主要是为了避免操作之间的相互干扰。设想下，用户 A 发起了主题删除，而同时用户 B 为这个主题新增了分区。
   * 此时，这两个操作就会相互冲突，如果允许 Controller 同时处理这两个操作，势必会造成逻辑上的混乱以及状态的不一致。
   * 为了应对这种情况，在移除主题副本和分区对象前，代码要先执行这个方法，以确保不再响应用户对该主题的其他操作。
   * mutePartitionModifications 方法的实现原理很简单，它会调用 unregisterPartitionModificationsHandlers，
   * 并接着调用 KafkaZkClient 的 unregisterZNodeChangeHandler 方法，取消 ZooKeeper 上对给定主题的分区节点数据变更的监听。
   * @param topic
   */
  override def mutePartitionModifications(topic: String): Unit = {
    controller.unregisterPartitionModificationsHandlers(Seq(topic))
  }
  // 向集群Broker发送指定分区的元数据更新请求
  override def sendMetadataUpdate(partitions: Set[TopicPartition]): Unit = {
    // 给集群所有Broker发送UpdateMetadataRequest
    // 通知它们给定partitions的状态变化
    controller.sendUpdateMetadataRequest(controller.controllerContext.liveOrShuttingDownBrokerIds.toSeq, partitions)
  }
}

/**
 * This manages the state machine for topic deletion.
 * 1. TopicCommand issues topic deletion by creating a new admin path /admin/delete_topics/<topic>
 * 2. The controller listens for child changes on /admin/delete_topic and starts topic deletion for the respective topics
 * 3. The controller's ControllerEventThread handles topic deletion. A topic will be ineligible
 *    for deletion in the following scenarios -
  *   3.1 broker hosting one of the replicas for that topic goes down
  *   3.2 partition reassignment for partitions of that topic is in progress
 * 4. Topic deletion is resumed when -
 *    4.1 broker hosting one of the replicas for that topic is started
 *    4.2 partition reassignment for partitions of that topic completes
 * 5. Every replica for a topic being deleted is in either of the 3 states -
 *    5.1 TopicDeletionStarted Replica enters TopicDeletionStarted phase when onPartitionDeletion is invoked.
 *        This happens when the child change watch for /admin/delete_topics fires on the controller. As part of this state
 *        change, the controller sends StopReplicaRequests to all replicas. It registers a callback for the
 *        StopReplicaResponse when deletePartition=true thereby invoking a callback when a response for delete replica
 *        is received from every replica)
 *    5.2 TopicDeletionSuccessful moves replicas from
 *        TopicDeletionStarted->TopicDeletionSuccessful depending on the error codes in StopReplicaResponse
 *    5.3 TopicDeletionFailed moves replicas from
 *        TopicDeletionStarted->TopicDeletionFailed depending on the error codes in StopReplicaResponse.
 *        In general, if a broker dies and if it hosted replicas for topics being deleted, the controller marks the
 *        respective replicas in TopicDeletionFailed state in the onBrokerFailure callback. The reason is that if a
 *        broker fails before the request is sent and after the replica is in TopicDeletionStarted state,
 *        it is possible that the replica will mistakenly remain in TopicDeletionStarted state and topic deletion
 *        will not be retried when the broker comes back up.
 * 6. A topic is marked successfully deleted only if all replicas are in TopicDeletionSuccessful
 *    state. Topic deletion teardown mode deletes all topic state from the controllerContext
 *    as well as from zookeeper. This is the only time the /brokers/topics/<topic> path gets deleted. On the other hand,
 *    if no replica is in TopicDeletionStarted state and at least one replica is in TopicDeletionFailed state, then
 *    it marks the topic for deletion retry.
 * @param controller
 * 负责对指定 Kafka 主题执行删除操作，清除待删除主题在集群上的各类“痕迹”。
 * 主题删除管理器类，定义了若干个方法维护主题删除前后集群状态的正确性。
 * ControllerDeletionClient比如，什么时候才能删除主题、什么时候主题不能被删除、主题删除过程中要规避哪些操作，等等。
 * config：KafkaConfig 实例，可以用作获取 Broker 端参数 delete.topic.enable 的值。该参数用于控制是否允许删除主题，默认值是 true，即 Kafka 默认允许用户删除主题。
 * controllerContext：Controller 端保存的元数据信息。删除主题必然要变更集群元数据信息，因此 TopicDeletionManager 需要用到 controllerContext 的方法，去更新它保存的数据。
 * replicaStateMachine 和 partitionStateMachine：副本状态机和分区状态机。它们各自负责副本和分区的状态转换，以保持副本对象和分区对象在集群上的一致性状态。这两个状态机是后面两讲的重要知识点。
 * client：DeletionClient 接口。TopicDeletionManager 通过该接口执行 ZooKeeper 上节点的相应更新。
 * isDeleteTopicEnabled：表明主题是否允许被删除。它是 Broker 端参数 delete.topic.enable 的值，默认是 true，表示 Kafka 允许删除主题。源码中大量使用这个字段判断主题的可删除性。前面的 config 参数的主要目的就是设置这个字段的值。被设定之后，config 就不再被源码使用了。
 */
class TopicDeletionManager(config: KafkaConfig, // KafkaConfig类，保存Broker端参数
                           controllerContext: ControllerContext, // 集群元数据
                           replicaStateMachine: ReplicaStateMachine, // 副本状态机，用于设置副本状态
                           partitionStateMachine: PartitionStateMachine, // 分区状态机，用于设置分区状态
                           client: DeletionClient) extends Logging { // DeletionClient接口，实现主题删除
  this.logIdent = s"[Topic Deletion Manager ${config.brokerId}] "
  val isDeleteTopicEnabled: Boolean = config.deleteTopicEnable // 是否允许删除主题

  def init(initialTopicsToBeDeleted: Set[String], initialTopicsIneligibleForDeletion: Set[String]): Unit = {
    info(s"Initializing manager with initial deletions: $initialTopicsToBeDeleted, " +
      s"initial ineligible deletions: $initialTopicsIneligibleForDeletion")

    if (isDeleteTopicEnabled) {
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
   * Invoked by the child change listener on /admin/delete_topics to queue up the topics for deletion. The topic gets added
   * to the topicsToBeDeleted list and only gets removed from the list when the topic deletion has completed successfully
   * i.e. all replicas of all partitions of that topic are deleted successfully.
   * @param topics Topics that should be deleted
   */
  def enqueueTopicsForDeletion(topics: Set[String]): Unit = {
    if (isDeleteTopicEnabled) {
      controllerContext.queueTopicDeletion(topics)
      resumeDeletions()
    }
  }

  /**
   * Invoked when any event that can possibly resume topic deletion occurs. These events include -
   * 1. New broker starts up. Any replicas belonging to topics queued up for deletion can be deleted since the broker is up
   * 2. Partition reassignment completes. Any partitions belonging to topics queued up for deletion finished reassignment
   * @param topics Topics for which deletion can be resumed
   */
  def resumeDeletionForTopics(topics: Set[String] = Set.empty): Unit = {
    if (isDeleteTopicEnabled) {
      val topicsToResumeDeletion = topics & controllerContext.topicsToBeDeleted
      if (topicsToResumeDeletion.nonEmpty) {
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
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * To ensure a successful retry, reset states for respective replicas from ReplicaDeletionIneligible to OfflineReplica state
   * @param topics Topics for which deletion should be retried
   */
  private def retryDeletionForIneligibleReplicas(topics: Set[String]): Unit = {
    // reset replica states from ReplicaDeletionIneligible to OfflineReplica
    val failedReplicas = topics.flatMap(controllerContext.replicasInState(_, ReplicaDeletionIneligible))
    debug(s"Retrying deletion of topics ${topics.mkString(",")} since replicas ${failedReplicas.mkString(",")} were not successfully deleted")
    replicaStateMachine.handleStateChanges(failedReplicas.toSeq, OfflineReplica)
  }

  private def completeDeleteTopic(topic: String): Unit = {
    // deregister partition change listener on the deleted topic. This is to prevent the partition change listener
    // firing before the new topic listener when a deleted topic gets auto created
    // 第1步：注销分区变更监听器，防止删除过程中因分区数据变更导致监听器被触发，引起状态不一致
    client.mutePartitionModifications(topic)
    // 第2步：获取该主题下处于ReplicaDeletionSuccessful状态的所有副本对象，即所有已经被成功删除的副本对象
    val replicasForDeletedTopic = controllerContext.replicasInState(topic, ReplicaDeletionSuccessful)
    // controller will remove this replica from the state machine as well as its partition assignment cache
    // 第3步：利用副本状态机将这些副本对象转换成NonExistentReplica状态。  等同于在状态机中删除这些副本
    replicaStateMachine.handleStateChanges(replicasForDeletedTopic.toSeq, NonExistentReplica)
    // 第4步：移除ZooKeeper上关于该主题的一切“痕迹”
    client.deleteTopic(topic, controllerContext.epochZkVersion)
    // 第5步：移除元数据缓存中关于该主题的一切“痕迹”
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
    // 找出给定待删除主题列表中那些尚未开启删除操作的所有主题
    val unseenTopicsForDeletion = topics.diff(controllerContext.topicsWithDeletionStarted)
    if (unseenTopicsForDeletion.nonEmpty) {
      // 获取到这些主题的所有分区对象
      val unseenPartitionsForDeletion = unseenTopicsForDeletion.flatMap(controllerContext.partitionsForTopic)
      // 将这些分区的状态依次调整成OfflinePartition和NonExistentPartition  等同于将这些分区从分区状态机中删除
      partitionStateMachine.handleStateChanges(unseenPartitionsForDeletion.toSeq, OfflinePartition)
      partitionStateMachine.handleStateChanges(unseenPartitionsForDeletion.toSeq, NonExistentPartition)
      // adding of unseenTopicsForDeletion to topics with deletion started must be done after the partition
      // state changes to make sure the offlinePartitionCount metric is properly updated
      // 把这些主题加到“已开启删除操作”主题列表中
      controllerContext.beginTopicDeletion(unseenTopicsForDeletion)
    }

    // send update metadata so that brokers stop serving data for topics to be deleted
    // 给集群所有Broker发送元数据更新请求，告诉它们不要再为这些主题处理数据了
    client.sendMetadataUpdate(topics.flatMap(controllerContext.partitionsForTopic))
    // 分区删除操作会执行底层的物理磁盘文件删除动作
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
   * 重启主题删除操作过程的方法
   * 这个方法首先从元数据缓存中获取要删除的主题列表，之后定义了两个空的主题列表，分别保存待重试删除主题和待删除主题。
   * 然后，代码遍历每个要删除的主题，去看它所有副本的状态。如果副本状态都是 ReplicaDeletionSuccessful，就表明该主题已经被成功删除，
   * 此时，再调用 completeDeleteTopic 方法，完成后续的操作就可以了。对于那些删除操作尚未开始，并且暂时无法执行删除的主题，
   * 源码会把这类主题加到待重试主题列表中，用于后续重试；如果主题是能够被删除的，就将其加入到待删除列表中。
   * 最后，该方法调用 retryDeletionForIneligibleReplicas 方法，来重试待重试主题列表中的主题删除操作。
   * 对于待删除主题列表中的主题则调用 onTopicDeletion 删除之。
   * 值得一提的是，retryDeletionForIneligibleReplicas 方法用于重试主题删除。
   * 这是通过将对应主题副本的状态，从 ReplicaDeletionIneligible 变更到 OfflineReplica 来完成的。
   * 这样，后续再次调用 resumeDeletions 时，会尝试重新删除主题。
   */
  private def resumeDeletions(): Unit = {
    // 从元数据缓存中获取要删除的主题列表
    val topicsQueuedForDeletion = Set.empty[String] ++ controllerContext.topicsToBeDeleted
    // 待重试主题列表
    val topicsEligibleForRetry = mutable.Set.empty[String]
    // 待删除主题列表
    val topicsEligibleForDeletion = mutable.Set.empty[String]

    if (topicsQueuedForDeletion.nonEmpty)
      info(s"Handling deletion for topics ${topicsQueuedForDeletion.mkString(",")}")
    // 遍历每个待删除主题
    topicsQueuedForDeletion.foreach { topic =>
      // if all replicas are marked as deleted successfully, then topic deletion is done
      // 如果该主题所有副本已经是ReplicaDeletionSuccessful状态， 即该主题已经被删除
      if (controllerContext.areAllReplicasInState(topic, ReplicaDeletionSuccessful)) {
        // clear up all state for this topic from controller cache and zookeeper
        // 调用completeDeleteTopic方法完成后续操作即可
        completeDeleteTopic(topic)
        info(s"Deletion of topic $topic successfully completed")
        // 如果主题删除尚未开始并且主题当前无法执行删除的话
      } else if (!controllerContext.isAnyReplicaInState(topic, ReplicaDeletionStarted)) {
        // if you come here, then no replica is in TopicDeletionStarted and all replicas are not in
        // TopicDeletionSuccessful. That means, that either given topic haven't initiated deletion
        // or there is at least one failed replica (which means topic deletion should be retried).
        if (controllerContext.isAnyReplicaInState(topic, ReplicaDeletionIneligible)) {
          // 把该主题加到待重试主题列表中用于后续重试
          topicsEligibleForRetry += topic
        }
      }

      // Add topic to the eligible set if it is eligible for deletion.
      // 如果该主题能够被删除
      if (isTopicEligibleForDeletion(topic)) {
        info(s"Deletion of topic $topic (re)started")
        topicsEligibleForDeletion += topic
      }
    }

    // topic deletion retry will be kicked off
    // 重试待重试主题列表中的主题删除操作
    if (topicsEligibleForRetry.nonEmpty) {
      retryDeletionForIneligibleReplicas(topicsEligibleForRetry)
    }

    // topic deletion will be kicked off
    // 调用onTopicDeletion方法，对待删除主题列表中的主题执行删除操作
    if (topicsEligibleForDeletion.nonEmpty) {
      onTopicDeletion(topicsEligibleForDeletion)
    }
  }
}
