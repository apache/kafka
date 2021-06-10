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

import kafka.api.LeaderAndIsr
import kafka.common.StateChangeFailedException
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicPartitionStateZNode
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.zookeeper.KeeperException.Code
import scala.collection.{Seq, mutable}

/**
 * 副本状态机抽象类
 *
 * @param controllerContext Controller上下文，里面包含集群详细信息
 */
abstract class ReplicaStateMachine(controllerContext: ControllerContext) extends Logging {
  /**
   * Invoked on successful controller election.
   */
  def startup(): Unit = {
    info("Initializing replica state")
    initializeReplicaState()
    info("Triggering online replica state changes")
    val (onlineReplicas, offlineReplicas) = controllerContext.onlineAndOfflineReplicas
    handleStateChanges(onlineReplicas.toSeq, OnlineReplica)
    info("Triggering offline replica state changes")
    handleStateChanges(offlineReplicas.toSeq, OfflineReplica)
    debug(s"Started replica state machine with initial state -> ${controllerContext.replicaStates}")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown(): Unit = {
    info("Stopped replica state machine")
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   */
  private def initializeReplicaState(): Unit = {
    controllerContext.allPartitions.foreach { partition =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      replicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(partition, replicaId)
        if (controllerContext.isReplicaOnline(replicaId, partition)) {
          controllerContext.putReplicaState(partitionAndReplica, OnlineReplica)
        } else {
          // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
          // This is required during controller failover since during controller failover a broker can go down,
          // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.
          controllerContext.putReplicaState(partitionAndReplica, ReplicaDeletionIneligible)
        }
      }
    }
  }

  /**
   *
   * @param replicas    待变更的副本列表
   * @param targetState 目标状态
   */
  def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit
}

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 * replica can only get become follower state change request.  Valid previous
 * state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 * state. In this state, it can get either become leader or become follower state change requests.
 * Valid previous state are NewReplica, OnlineReplica, OfflineReplica and ReplicaDeletionIneligible
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 * is down. Valid previous state are NewReplica, OnlineReplica, OfflineReplica and ReplicaDeletionIneligible
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 * moved to this state. Valid previous state is ReplicaDeletionStarted
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous states are
 * ReplicaDeletionStarted and OfflineReplica
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 * ReplicaDeletionSuccessful
 */
class ZkReplicaStateMachine(config: KafkaConfig,
                            stateChangeLogger: StateChangeLogger,
                            controllerContext: ControllerContext,
                            zkClient: KafkaZkClient,
                            controllerBrokerRequestBatch: ControllerBrokerRequestBatch)
  extends ReplicaStateMachine(controllerContext) with Logging {

  private val controllerId = config.brokerId
  this.logIdent = s"[ReplicaStateMachine controllerId=$controllerId] "

  /**
   * 统一处理副本变更状态操作，同一分区的不同副本需要变更到不同的状态，而且不同副本所在的Broker不一样，所以处理起来还是稍有些麻烦。
   * 1.先统计处理变更状态逻辑，根据状态变更组装不同的请求。比如有些副本需要关闭，那么就需要组装StopReplicaRequet、有些新副本需要上线，
   *   那么需要组装LeaderAndIsrRequest等等。将这些请求放入缓存中。
   * 2.更新本地缓存中的状态
   * 3.将缓存的请求全部发送到目标Broker
   *
   * @param replicas    待变更的副本列表
   * @param targetState 目标状态
   */
  override def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit = {
    if (replicas.nonEmpty) {
      try {
        // #1 清空controller待发送请求集合
        controllerBrokerRequestBatch.newBatch()

        // #2 将所有副本对象按照broker id进行分组，依次执行状态转换操作
        replicas.groupBy(_.replica).forKeyValue { (replicaId, replicas) =>
          // #2-1 根据副本要变更到不同的状态做不同处理，
          // 本质就是组装leaderAndIsrRequest、UpdateMetadataRequest和StopReplicaRequest对象
          // 此时还没有执行I/O操作
          doHandleStateChanges(replicaId, replicas, targetState)
        }

        // #3 执行I/O操作，将缓存的请求全部发送到Broker端
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
      } catch {
        // #4 controller易主，是记录错误日志后抛出异常
        case e: ControllerMovedException =>
          error(s"Controller moved to another broker when moving some replicas to $targetState state", e)
          throw e
        case e: Throwable => error(s"Error while moving some replicas to $targetState state", e)
      }
    }
  }

  /**
   * 分区状态变更的主要逻辑，是副本状态机最重要的逻辑处理代码。
   *
   * It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   * partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   * partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   * UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache
   * 需要告诉broker端有哪些主题的分区副本需要进行状态变更
   *
   * @param replicaId   待转换的副本所在的broker id
   *                    The replica for which the state transition is invoked
   * @param replicas    The partitions on this replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */
  private def doHandleStateChanges(replicaId: Int, replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit = {
    val stateLogger = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    val traceEnabled = stateLogger.isTraceEnabled
    replicas.foreach(replica => controllerContext.putReplicaStateIfNotExists(replica, NonExistentReplica))
    val (validReplicas, invalidReplicas) = controllerContext.checkValidReplicaStateChange(replicas, targetState)
    invalidReplicas.foreach(replica => logInvalidTransition(replica, targetState))

    targetState match {
      // #1 目标状态为「NewReplica」，是从No变nExistentReplica状态更过来的
      case NewReplica =>
        // #1-1 遍历合法的可进行状态变更的副本
        validReplicas.foreach { replica =>
          // #1-2 获取该副本对象的分区对象，即<主题名，分区号>
          val partition = replica.topicPartition
          // #1-3 从Controller缓存中获取副本原状态
          val currentState = controllerContext.replicaState(replica)

          // #1-4 获取该分区副本元数据信息（比如Leader副本在哪里、ISR集合）
          controllerContext.partitionLeadershipInfo(partition) match {
            // #1-5 成功获取到元数据信息
            case Some(leaderIsrAndControllerEpoch) =>
              // #1-6 判断该副本是否为Leader副本
              if (leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId) {
                // 如果是Leader，则记录错误日志：Leader副本是不能被设置成NewReplica状态
                val exception = new StateChangeFailedException(s"Replica $replicaId for partition $partition cannot be moved to NewReplica state as it is being requested to become leader")
                logFailedStateChange(replica, currentState, OfflineReplica, exception)
              } else {
                // #1-7 非Leader副本，则给该副本所属的Broker发送「LeaderAndIsrRequest」请求，进行分区数据同步。
                //      后续会向集群其它broker广播「UpdateMetadataRequest」请求：通知所有存活的broker该分区数据发生变更
                controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                  replica.topicPartition,
                  leaderIsrAndControllerEpoch,
                  controllerContext.partitionFullReplicaAssignment(replica.topicPartition),
                  isNew = true)
                if (traceEnabled)
                  logSuccessfulTransition(stateLogger, replicaId, partition, currentState, NewReplica)

                // #1-8 更新Controller缓存：将该副本对象的当前状态设置为「NewReplica」状态
                controllerContext.putReplicaState(replica, NewReplica)
              }

            // #1-9 Controller缓存没有数据，那么将它的状态设置为「NewReplica」即可（因为其它Broker也没有数据，所以不需要通知）
            case None =>
              if (traceEnabled) {
                logSuccessfulTransition(stateLogger, replicaId, partition, currentState, NewReplica)
              }
              controllerContext.putReplicaState(replica, NewReplica)
          }
        }

      // #2 目标状态为「OnlineReplica」，这是副本能正常对外提供服务的状态
      case OnlineReplica =>
        validReplicas.foreach { replica =>
          // 副本对应的分区
          val partition = replica.topicPartition
          // 副本当前状态
          val currentState = controllerContext.replicaState(replica)

          currentState match {
            // #2-1 副本当前状态为「NewReplica」
            case NewReplica =>
              // #2-2 从controller缓存中获取分区副本元数据（Leader、ISR集合等）
              val assignment = controllerContext.partitionFullReplicaAssignment(partition)
              // #2-2 判断集合中是否包含当前副本，如果不包含，输出错误日志
              if (!assignment.replicas.contains(replicaId)) {
                error(s"Adding replica ($replicaId) that is not part of the assignment $assignment")
                // #2-3 将该副本加入到副本列表中，并更新元数据缓存中该分区的副本列表
                val newAssignment = assignment.copy(replicas = assignment.replicas :+ replicaId)
                controllerContext.updatePartitionFullReplicaAssignment(partition, newAssignment)
              }
            // #2-4 其它状态
            case _ =>
              // #2-5 尝试获取该分区当前信息数据
              controllerContext.partitionLeadershipInfo(partition) match {
                // #2-6 如果存在分区信息，向该副本对象所在的Broker发送请求，令其同步该分区数据
                case Some(leaderIsrAndControllerEpoch) =>
                  controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                    replica.topicPartition,
                    leaderIsrAndControllerEpoch,
                    controllerContext.partitionFullReplicaAssignment(partition), isNew = false)
                case None =>
              }
          }
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, partition, currentState, OnlineReplica)

          // #2-7 变更副本的状态为「OnlineReplica」
          controllerContext.putReplicaState(replica, OnlineReplica)
        }
      // #3 目标状态为「OfflineReplica」
      case OfflineReplica =>
        validReplicas.foreach { replica =>
          // #3-1 向副本所在的Broker发送「StopReplicaRequest」请求：表示停止副本
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = false)
        }

        // #3-2 将副本对象集合划分成有Leader副本和无Leader副本
        val (replicasWithLeadershipInfo, replicasWithoutLeadershipInfo) = validReplicas.partition { replica =>
          controllerContext.partitionLeadershipInfo(replica.topicPartition).isDefined
        }

        // #3-4 对于有Leader的副本，从ISR中移除副本对象并更新Zookeeper节点，得到已成功更新的
        val updatedLeaderIsrAndControllerEpochs =
          removeReplicasFromIsr(replicaId, replicasWithLeadershipInfo.map(_.topicPartition))

        // #3-5 遍历每个已完成更新的分区
        updatedLeaderIsrAndControllerEpochs.forKeyValue { (partition, leaderIsrAndControllerEpoch) =>
          stateLogger.info(s"Partition $partition state changed to $leaderIsrAndControllerEpoch after removing replica $replicaId from the ISR as part of transition to $OfflineReplica")
          // #3-6 如果分区对应主题并未删除
          if (!controllerContext.isTopicQueuedUpForDeletion(partition.topic)) {
            // #3-7 获取该分区除给定副本以外的其它副本所在的Broker
            val recipients = controllerContext.partitionReplicaAssignment(partition).filterNot(_ == replicaId)
            // #3-8 向这些Broker发送「LeaderAndIsrRequest」请求以更新该分区元数据
            controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipients,
              partition,
              leaderIsrAndControllerEpoch,
              controllerContext.partitionFullReplicaAssignment(partition), isNew = false)
          }

          val replica = PartitionAndReplica(partition, replicaId)
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, partition, currentState, OfflineReplica)

          // #3-9 变更副本的状态为「OfflineReplica」
          controllerContext.putReplicaState(replica, OfflineReplica)
        }

        // #3-10 遍历无Leader的所有副本对象
        replicasWithoutLeadershipInfo.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, OfflineReplica)
          // #3-11 向集群所有Broker发送请求，更新对应分区的元数据
          controllerBrokerRequestBatch.addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(replica.topicPartition))
          // #3-12 变更副本的状态为「OfflineReplica」
          controllerContext.putReplicaState(replica, OfflineReplica)
        }
      case ReplicaDeletionStarted =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionStarted)
          controllerContext.putReplicaState(replica, ReplicaDeletionStarted)
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = true)
        }
      case ReplicaDeletionIneligible =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionIneligible)
          controllerContext.putReplicaState(replica, ReplicaDeletionIneligible)
        }
      case ReplicaDeletionSuccessful =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionSuccessful)
          controllerContext.putReplicaState(replica, ReplicaDeletionSuccessful)
        }
      case NonExistentReplica =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          val newAssignedReplicas = controllerContext
            .partitionFullReplicaAssignment(replica.topicPartition)
            .removeReplica(replica.replica)

          controllerContext.updatePartitionFullReplicaAssignment(replica.topicPartition, newAssignedReplicas)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, NonExistentReplica)
          controllerContext.removeReplicaState(replica)
        }
    }
  }


  /**
   * 调用 {@link ZkReplicaStateMachine.doRemoveReplicasFromIsr()} 方法，
   * 实现将给定的副本对象从给定的ISR集合中移除的功能
   *
   * @param replicaId  将要被移除的副本ID
   * @param partitions 分区集合，这什么意思呢?我们知道，副本ID并非是全局唯一，而是在单个主题内部是唯一值。
   *                   所以这里传入主题集合是确定删除哪些主题的副本ID为 replicaId 的副本
   * @return           所有分区已更新的「LeaderIsrAndControllerEpochs」对象，成功将指定副本从ISR集合中移除
   */
  private def removeReplicasFromIsr(replicaId: Int,
                                    partitions: Seq[TopicPartition]
                                   ): Map[TopicPartition, LeaderIsrAndControllerEpoch] = {
    var results = Map.empty[TopicPartition, LeaderIsrAndControllerEpoch]
    var remaining = partitions
    while (remaining.nonEmpty) {
      //
      val (finishedRemoval, removalsToRetry) = doRemoveReplicasFromIsr(replicaId, remaining)
      remaining = removalsToRetry

      finishedRemoval.foreach {
        case (partition, Left(e)) =>
          val replica = PartitionAndReplica(partition, replicaId)
          val currentState = controllerContext.replicaState(replica)
          logFailedStateChange(replica, currentState, OfflineReplica, e)
        case (partition, Right(leaderIsrAndEpoch)) =>
          results += partition -> leaderIsrAndEpoch
      }
    }
    results
  }

  /**
   * 从ISR集合中移除副本对象。包括删除Zookeeper元数据和更新controller缓存
   *
   * Try to remove a replica from the isr of multiple partitions.
   * Removing a replica from isr updates partition state in zookeeper.
   *
   * @param replicaId  The replica being removed from isr of multiple partitions
   * @param partitions The partitions from which we're trying to remove the replica from isr
   * @return A tuple of two elements:
   *         1. The updated Right[LeaderIsrAndControllerEpochs] of all partitions for which we successfully
   *            removed the replica from isr. Or Left[Exception] corresponding to failed removals that should
   *            not be retried
   *            2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *            the partition leader updated partition state while the controller attempted to update partition state.
   */
  private def doRemoveReplicasFromIsr(
                                       replicaId: Int,
                                       partitions: Seq[TopicPartition]
                                     ): (Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]], Seq[TopicPartition]) = {
    // #1 获取ZK节点 /brokers/topics/<topic name>/partitions/<replicaId>/state节点的值
    // Data	{"controller_epoch":17,"leader":-1,"version":1,"leader_epoch":11,"isr":[1]}
    val (leaderAndIsrs, partitionsWithNoLeaderAndIsrInZk) = getTopicPartitionStatesFromZk(partitions)

    // #2 将副本对象集合划分成有Leader信息的副本集合和无Leader信息的副本集合
    val (leaderAndIsrsWithReplica, leaderAndIsrsWithoutReplica) = leaderAndIsrs.partition { case (_, result) =>
      result.map { leaderAndIsr =>
        leaderAndIsr.isr.contains(replicaId)
      }.getOrElse(false)
    }

    // #3
    val adjustedLeaderAndIsrs: Map[TopicPartition, LeaderAndIsr] = leaderAndIsrsWithReplica.flatMap {
      case (partition, result) =>
        result.toOption.map { leaderAndIsr =>
          val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader
          val adjustedIsr = if (leaderAndIsr.isr.size == 1) leaderAndIsr.isr else leaderAndIsr.isr.filter(_ != replicaId)
          partition -> leaderAndIsr.newLeaderAndIsr(newLeader, adjustedIsr)
        }
    }

    val UpdateLeaderAndIsrResult(finishedPartitions, updatesToRetry) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch, controllerContext.epochZkVersion)

    val exceptionsForPartitionsWithNoLeaderAndIsrInZk: Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]] =
      partitionsWithNoLeaderAndIsrInZk.iterator.flatMap { partition =>
        if (!controllerContext.isTopicQueuedUpForDeletion(partition.topic)) {
          val exception = new StateChangeFailedException(
            s"Failed to change state of replica $replicaId for partition $partition since the leader and isr " +
              "path in zookeeper is empty"
          )
          Option(partition -> Left(exception))
        } else None
      }.toMap

    val leaderIsrAndControllerEpochs: Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]] =
      (leaderAndIsrsWithoutReplica ++ finishedPartitions).map { case (partition, result) =>
        (partition, result.map { leaderAndIsr =>
          val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
          controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
          leaderIsrAndControllerEpoch
        })
      }

    (leaderIsrAndControllerEpochs ++ exceptionsForPartitionsWithNoLeaderAndIsrInZk, updatesToRetry)
  }

  /**
   * 从Zookeeper中获取分区的状态信息（最准确），包括每个分区的Leader副本、ISR集合等数据
   * @param partitions 等待从ZK中获取数据的分区列表
   * @return A tuple of two values:
   *         1. The Right(LeaderAndIsrs) of partitions whose state we successfully read from zookeeper.
   *            The Left(Exception) to failed zookeeper lookups or states whose controller epoch exceeds our current epoch
   *            2. The partitions that had no leader and isr state in zookeeper. This happens if the controller
   *            didn't finish partition initialization.
   */
  private def getTopicPartitionStatesFromZk(partitions: Seq[TopicPartition]
                                           ): (Map[TopicPartition, Either[Exception, LeaderAndIsr]], Seq[TopicPartition]) = {
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (partitions.iterator.map(_ -> Left(e)).toMap, Seq.empty)
    }

    val partitionsWithNoLeaderAndIsrInZk = mutable.Buffer.empty[TopicPartition]
    val result = mutable.Map.empty[TopicPartition, Either[Exception, LeaderAndIsr]]

    getDataResponses.foreach[Unit] { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      if (getDataResponse.resultCode == Code.OK) {
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat) match {
          case None =>
            partitionsWithNoLeaderAndIsrInZk += partition
          case Some(leaderIsrAndControllerEpoch) =>
            if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
              val exception = new StateChangeFailedException(
                "Leader and isr path written by another controller. This probably " +
                  s"means the current controller with epoch ${controllerContext.epoch} went through a soft failure and " +
                  s"another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}. Aborting " +
                  "state change by this controller"
              )
              result += (partition -> Left(exception))
            } else {
              result += (partition -> Right(leaderIsrAndControllerEpoch.leaderAndIsr))
            }
        }
      } else if (getDataResponse.resultCode == Code.NONODE) {
        partitionsWithNoLeaderAndIsrInZk += partition
      } else {
        result += (partition -> Left(getDataResponse.resultException.get))
      }
    }

    (result.toMap, partitionsWithNoLeaderAndIsrInZk)
  }

  /**
   * 记录一条成功日志，表明执行了一次「成功」的状态变更操作
   * @param logger
   * @param replicaId
   * @param partition
   * @param currState
   * @param targetState
   */
  private def logSuccessfulTransition(logger: StateChangeLogger, replicaId: Int, partition: TopicPartition,
                                      currState: ReplicaState, targetState: ReplicaState): Unit = {
    logger.trace(s"Changed state of replica $replicaId for partition $partition from $currState to $targetState")
  }

  /**
   * 记录一条错误日志，表明执行了一次「非法」的状态变更操作
   * @param replica
   * @param targetState
   */
  private def logInvalidTransition(replica: PartitionAndReplica, targetState: ReplicaState): Unit = {
    val currState = controllerContext.replicaState(replica)
    val e = new IllegalStateException(s"Replica $replica should be in the ${targetState.validPreviousStates.mkString(",")} " +
      s"states before moving to $targetState state. Instead it is in $currState state")
    logFailedStateChange(replica, currState, targetState, e)
  }

  /**
   * 记录一条错误日志，表明执行了一次「无效」的状态变更操作
   * @param replica       进行状态变更的副本集合
   * @param currState     当前状态
   * @param targetState   所要变更的目标状态
   * @param t             异常
   */
  private def logFailedStateChange(replica: PartitionAndReplica, currState: ReplicaState, targetState: ReplicaState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} initiated state change of replica ${replica.replica} " +
        s"for partition ${replica.topicPartition} from $currState to $targetState failed", t)
  }
}

sealed trait ReplicaState {
  def state: Byte

  def validPreviousStates: Set[ReplicaState]
}

case object NewReplica extends ReplicaState {
  val state: Byte = 1
  val validPreviousStates: Set[ReplicaState] = Set(NonExistentReplica)
}

case object OnlineReplica extends ReplicaState {
  val state: Byte = 2
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object OfflineReplica extends ReplicaState {
  val state: Byte = 3
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object ReplicaDeletionStarted extends ReplicaState {
  val state: Byte = 4
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica)
}

case object ReplicaDeletionSuccessful extends ReplicaState {
  val state: Byte = 5
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionStarted)
}

case object ReplicaDeletionIneligible extends ReplicaState {
  val state: Byte = 6
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica, ReplicaDeletionStarted)
}

case object NonExistentReplica extends ReplicaState {
  val state: Byte = 7
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionSuccessful)
}
