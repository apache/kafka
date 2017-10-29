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
package kafka.zk

import java.util.Properties

import kafka.api.LeaderAndIsr
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.log.LogConfig
import kafka.server.ConfigType
import kafka.utils._
import kafka.zookeeper._
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, KeeperException}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Provides higher level Kafka-specific operations on top of the pipelined [[kafka.zookeeper.ZooKeeperClient]].
 *
 * This performs better than [[kafka.utils.ZkUtils]] and should replace it completely, eventually.
 *
 * Implementation note: this class includes methods for various components (Controller, Configs, Old Consumer, etc.)
 * and returns instances of classes from the calling packages in some cases. This is not ideal, but it makes it
 * easier to quickly migrate away from `ZkUtils`. We should revisit this once the migration is completed and tests are
 * in place. We should also consider whether a monolithic [[kafka.zk.ZkData]] is the way to go.
 */
class KafkaZkClient(zooKeeperClient: ZooKeeperClient, isSecure: Boolean) extends Logging {
  import KafkaZkClient._

  /**
   * Gets topic partition states for the given partitions.
   * @param partitions the partitions for which we want ot get states.
   * @return sequence of GetDataResponses whose contexts are the partitions they are associated with.
   */
  def getTopicPartitionStatesRaw(partitions: Seq[TopicAndPartition]): Seq[GetDataResponse] = {
    val getDataRequests = partitions.map { partition =>
      GetDataRequest(TopicPartitionStateZNode.path(partition), ctx = Some(partition))
    }
    retryRequestsUntilConnected(getDataRequests)
  }

  /**
   * Sets topic partition states for the given partitions.
   * @param leaderIsrAndControllerEpochs the partition states of each partition whose state we wish to set.
   * @return sequence of SetDataResponse whose contexts are the partitions they are associated with.
   */
  def setTopicPartitionStatesRaw(leaderIsrAndControllerEpochs: Map[TopicAndPartition, LeaderIsrAndControllerEpoch]): Seq[SetDataResponse] = {
    val setDataRequests = leaderIsrAndControllerEpochs.map { case (partition, leaderIsrAndControllerEpoch) =>
      val path = TopicPartitionStateZNode.path(partition)
      val data = TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch)
      SetDataRequest(path, data, leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion, Some(partition))
    }
    retryRequestsUntilConnected(setDataRequests.toSeq)
  }

  /**
   * Creates topic partition state znodes for the given partitions.
   * @param leaderIsrAndControllerEpochs the partition states of each partition whose state we wish to set.
   * @return sequence of CreateResponse whose contexts are the partitions they are associated with.
   */
  def createTopicPartitionStatesRaw(leaderIsrAndControllerEpochs: Map[TopicAndPartition, LeaderIsrAndControllerEpoch]): Seq[CreateResponse] = {
    createTopicPartitions(leaderIsrAndControllerEpochs.keys.map(_.topic).toSet.toSeq)
    createTopicPartition(leaderIsrAndControllerEpochs.keys.toSeq)
    val createRequests = leaderIsrAndControllerEpochs.map { case (partition, leaderIsrAndControllerEpoch) =>
      val path = TopicPartitionStateZNode.path(partition)
      val data = TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch)
      CreateRequest(path, data, acls(path), CreateMode.PERSISTENT, Some(partition))
    }
    retryRequestsUntilConnected(createRequests.toSeq)
  }

  /**
   * Sets the controller epoch conditioned on the given epochZkVersion.
   * @param epoch the epoch to set
   * @param epochZkVersion the expected version number of the epoch znode.
   * @return SetDataResponse
   */
  def setControllerEpochRaw(epoch: Int, epochZkVersion: Int): SetDataResponse = {
    val setDataRequest = SetDataRequest(ControllerEpochZNode.path, ControllerEpochZNode.encode(epoch), epochZkVersion)
    retryRequestUntilConnected(setDataRequest)
  }

  /**
   * Creates the controller epoch znode.
   * @param epoch the epoch to set
   * @return CreateResponse
   */
  def createControllerEpochRaw(epoch: Int): CreateResponse = {
    val createRequest = CreateRequest(ControllerEpochZNode.path, ControllerEpochZNode.encode(epoch),
      acls(ControllerEpochZNode.path), CreateMode.PERSISTENT)
    retryRequestUntilConnected(createRequest)
  }

  /**
   * Try to update the partition states of multiple partitions in zookeeper.
   * @param leaderAndIsrs The partition states to update.
   * @param controllerEpoch The current controller epoch.
   * @return UpdateLeaderAndIsrResult instance containing per partition results.
   */
  def updateLeaderAndIsr(leaderAndIsrs: Map[TopicAndPartition, LeaderAndIsr], controllerEpoch: Int): UpdateLeaderAndIsrResult = {
    val successfulUpdates = mutable.Map.empty[TopicAndPartition, LeaderAndIsr]
    val updatesToRetry = mutable.Buffer.empty[TopicAndPartition]
    val failed = mutable.Map.empty[TopicAndPartition, Exception]
    val leaderIsrAndControllerEpochs = leaderAndIsrs.map { case (partition, leaderAndIsr) => partition -> LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch) }
    val setDataResponses = try {
      setTopicPartitionStatesRaw(leaderIsrAndControllerEpochs)
    } catch {
      case e: Exception =>
        leaderAndIsrs.keys.foreach(partition => failed.put(partition, e))
        return UpdateLeaderAndIsrResult(successfulUpdates.toMap, updatesToRetry, failed.toMap)
    }
    setDataResponses.foreach { setDataResponse =>
      val partition = setDataResponse.ctx.get.asInstanceOf[TopicAndPartition]
      if (setDataResponse.resultCode == Code.OK) {
        val updatedLeaderAndIsr = leaderAndIsrs(partition).withZkVersion(setDataResponse.stat.getVersion)
        successfulUpdates.put(partition, updatedLeaderAndIsr)
      } else if (setDataResponse.resultCode == Code.BADVERSION) {
        updatesToRetry += partition
      } else {
        failed.put(partition, setDataResponse.resultException.get)
      }
    }
    UpdateLeaderAndIsrResult(successfulUpdates.toMap, updatesToRetry, failed.toMap)
  }

  /**
   * Get log configs that merge local configs with topic-level configs in zookeeper.
   * @param topics The topics to get log configs for.
   * @param config The local configs.
   * @return A tuple of two values:
   *         1. The successfully gathered log configs
   *         2. Exceptions corresponding to failed log config lookups.
   */
  def getLogConfigs(topics: Seq[String], config: java.util.Map[String, AnyRef]):
  (Map[String, LogConfig], Map[String, Exception]) = {
    val logConfigs = mutable.Map.empty[String, LogConfig]
    val failed = mutable.Map.empty[String, Exception]
    val configResponses = try {
      getTopicConfigs(topics)
    } catch {
      case e: Exception =>
        topics.foreach(topic => failed.put(topic, e))
        return (logConfigs.toMap, failed.toMap)
    }
    configResponses.foreach { configResponse =>
      val topic = configResponse.ctx.get.asInstanceOf[String]
      if (configResponse.resultCode == Code.OK) {
        val overrides = ConfigEntityZNode.decode(configResponse.data)
        val logConfig = LogConfig.fromProps(config, overrides.getOrElse(new Properties))
        logConfigs.put(topic, logConfig)
      } else if (configResponse.resultCode == Code.NONODE) {
        val logConfig = LogConfig.fromProps(config, new Properties)
        logConfigs.put(topic, logConfig)
      } else {
        failed.put(topic, configResponse.resultException.get)
      }
    }
    (logConfigs.toMap, failed.toMap)
  }

  /**
   * Gets all brokers in the cluster.
   * @return sequence of brokers in the cluster.
   */
  def getAllBrokersInCluster: Seq[Broker] = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(BrokerIdsZNode.path))
    if (getChildrenResponse.resultCode == Code.OK) {
      val brokerIds = getChildrenResponse.children.map(_.toInt)
      val getDataRequests = brokerIds.map(brokerId => GetDataRequest(BrokerIdZNode.path(brokerId), ctx = Some(brokerId)))
      val getDataResponses = retryRequestsUntilConnected(getDataRequests)
      getDataResponses.flatMap { getDataResponse =>
        val brokerId = getDataResponse.ctx.get.asInstanceOf[Int]
        if (getDataResponse.resultCode == Code.OK) {
          Option(BrokerIdZNode.decode(brokerId, getDataResponse.data))
        } else if (getDataResponse.resultCode == Code.NONODE) {
          None
        } else {
          throw getDataResponse.resultException.get
        }
      }
    } else if (getChildrenResponse.resultCode == Code.NONODE) {
      Seq.empty
    } else {
      throw getChildrenResponse.resultException.get
    }
  }

  /**
   * Gets all topics in the cluster.
   * @return sequence of topics in the cluster.
   */
  def getAllTopicsInCluster: Seq[String] = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(TopicsZNode.path))
    if (getChildrenResponse.resultCode == Code.OK) {
      getChildrenResponse.children
    } else if (getChildrenResponse.resultCode == Code.NONODE) {
      Seq.empty
    } else {
      throw getChildrenResponse.resultException.get
    }
  }

  /**
   * Sets the topic znode with the given assignment.
   * @param topic the topic whose assignment is being set.
   * @param assignment the partition to replica mapping to set for the given topic
   * @return SetDataResponse
   */
  def setTopicAssignmentRaw(topic: String, assignment: Map[TopicAndPartition, Seq[Int]]): SetDataResponse = {
    val setDataRequest = SetDataRequest(TopicZNode.path(topic), TopicZNode.encode(assignment), -1)
    retryRequestUntilConnected(setDataRequest)
  }

  /**
   * Gets the log dir event notifications as strings. These strings are the znode names and not the absolute znode path.
   * @return sequence of znode names and not the absolute znode path.
   */
  def getAllLogDirEventNotifications: Seq[String] = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(LogDirEventNotificationZNode.path))
    if (getChildrenResponse.resultCode == Code.OK) {
      getChildrenResponse.children.map(LogDirEventNotificationSequenceZNode.sequenceNumber)
    } else if (getChildrenResponse.resultCode == Code.NONODE) {
      Seq.empty
    } else {
      throw getChildrenResponse.resultException.get
    }
  }

  /**
   * Reads each of the log dir event notifications associated with the given sequence numbers and extracts the broker ids.
   * @param sequenceNumbers the sequence numbers associated with the log dir event notifications.
   * @return broker ids associated with the given log dir event notifications.
   */
  def getBrokerIdsFromLogDirEvents(sequenceNumbers: Seq[String]): Seq[Int] = {
    val getDataRequests = sequenceNumbers.map { sequenceNumber =>
      GetDataRequest(LogDirEventNotificationSequenceZNode.path(sequenceNumber))
    }
    val getDataResponses = retryRequestsUntilConnected(getDataRequests)
    getDataResponses.flatMap { getDataResponse =>
      if (getDataResponse.resultCode == Code.OK) {
        LogDirEventNotificationSequenceZNode.decode(getDataResponse.data)
      } else if (getDataResponse.resultCode == Code.NONODE) {
        None
      } else {
        throw getDataResponse.resultException.get
      }
    }
  }

  /**
   * Deletes all log dir event notifications.
   */
  def deleteLogDirEventNotifications(): Unit = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(LogDirEventNotificationZNode.path))
    if (getChildrenResponse.resultCode == Code.OK) {
      deleteLogDirEventNotifications(getChildrenResponse.children)
    } else if (getChildrenResponse.resultCode != Code.NONODE) {
      throw getChildrenResponse.resultException.get
    }
  }

  /**
   * Deletes the log dir event notifications associated with the given sequence numbers.
   * @param sequenceNumbers the sequence numbers associated with the log dir event notifications to be deleted.
   */
  def deleteLogDirEventNotifications(sequenceNumbers: Seq[String]): Unit = {
    val deleteRequests = sequenceNumbers.map { sequenceNumber =>
      DeleteRequest(LogDirEventNotificationSequenceZNode.path(sequenceNumber), -1)
    }
    retryRequestsUntilConnected(deleteRequests)
  }

  /**
   * Gets the assignments for the given topics.
   * @param topics the topics whose partitions we wish to get the assignments for.
   * @return the replica assignment for each partition from the given topics.
   */
  def getReplicaAssignmentForTopics(topics: Set[String]): Map[TopicAndPartition, Seq[Int]] = {
    val getDataRequests = topics.map(topic => GetDataRequest(TopicZNode.path(topic), ctx = Some(topic)))
    val getDataResponses = retryRequestsUntilConnected(getDataRequests.toSeq)
    getDataResponses.flatMap { getDataResponse =>
      val topic = getDataResponse.ctx.get.asInstanceOf[String]
      if (getDataResponse.resultCode == Code.OK) {
        TopicZNode.decode(topic, getDataResponse.data)
      } else if (getDataResponse.resultCode == Code.NONODE) {
        Map.empty[TopicAndPartition, Seq[Int]]
      } else {
        throw getDataResponse.resultException.get
      }
    }.toMap
  }

  /**
   * Get all topics marked for deletion.
   * @return sequence of topics marked for deletion.
   */
  def getTopicDeletions: Seq[String] = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(DeleteTopicsZNode.path))
    if (getChildrenResponse.resultCode == Code.OK) {
      getChildrenResponse.children
    } else if (getChildrenResponse.resultCode == Code.NONODE) {
      Seq.empty
    } else {
      throw getChildrenResponse.resultException.get
    }
  }

  /**
   * Remove the given topics from the topics marked for deletion.
   * @param topics the topics to remove.
   */
  def deleteTopicDeletions(topics: Seq[String]): Unit = {
    val deleteRequests = topics.map(topic => DeleteRequest(DeleteTopicsTopicZNode.path(topic), -1))
    retryRequestsUntilConnected(deleteRequests)
  }

  /**
   * Returns all reassignments.
   * @return the reassignments for each partition.
   */
  def getPartitionReassignment: Map[TopicAndPartition, Seq[Int]] = {
    val getDataRequest = GetDataRequest(ReassignPartitionsZNode.path)
    val getDataResponse = retryRequestUntilConnected(getDataRequest)
    if (getDataResponse.resultCode == Code.OK) {
      ReassignPartitionsZNode.decode(getDataResponse.data)
    } else if (getDataResponse.resultCode == Code.NONODE) {
      Map.empty[TopicAndPartition, Seq[Int]]
    } else {
      throw getDataResponse.resultException.get
    }
  }

  /**
   * Sets the partition reassignment znode with the given reassignment.
   * @param reassignment the reassignment to set on the reassignment znode.
   * @return SetDataResponse
   */
  def setPartitionReassignmentRaw(reassignment: Map[TopicAndPartition, Seq[Int]]): SetDataResponse = {
    val setDataRequest = SetDataRequest(ReassignPartitionsZNode.path, ReassignPartitionsZNode.encode(reassignment), -1)
    retryRequestUntilConnected(setDataRequest)
  }

  /**
   * Creates the partition reassignment znode with the given reassignment.
   * @param reassignment the reassignment to set on the reassignment znode.
   * @return CreateResponse
   */
  def createPartitionReassignment(reassignment: Map[TopicAndPartition, Seq[Int]]): CreateResponse = {
    val createRequest = CreateRequest(ReassignPartitionsZNode.path, ReassignPartitionsZNode.encode(reassignment),
      acls(ReassignPartitionsZNode.path), CreateMode.PERSISTENT)
    retryRequestUntilConnected(createRequest)
  }

  /**
   * Deletes the partition reassignment znode.
   */
  def deletePartitionReassignment(): Unit = {
    val deleteRequest = DeleteRequest(ReassignPartitionsZNode.path, -1)
    retryRequestUntilConnected(deleteRequest)
  }

  /**
   * Gets topic partition states for the given partitions.
   * @param partitions the partitions for which we want ot get states.
   * @return map containing LeaderIsrAndControllerEpoch of each partition for we were able to lookup the partition state.
   */
  def getTopicPartitionStates(partitions: Seq[TopicAndPartition]): Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = {
    val getDataResponses = getTopicPartitionStatesRaw(partitions)
    getDataResponses.flatMap { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicAndPartition]
      if (getDataResponse.resultCode == Code.OK) {
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat).map(partition -> _)
      } else if (getDataResponse.resultCode == Code.NONODE) {
        None
      } else {
        throw getDataResponse.resultException.get
      }
    }.toMap
  }

  /**
   * Gets the isr change notifications as strings. These strings are the znode names and not the absolute znode path.
   * @return sequence of znode names and not the absolute znode path.
   */
  def getAllIsrChangeNotifications: Seq[String] = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(IsrChangeNotificationZNode.path))
    if (getChildrenResponse.resultCode == Code.OK) {
      getChildrenResponse.children.map(IsrChangeNotificationSequenceZNode.sequenceNumber)
    } else if (getChildrenResponse.resultCode == Code.NONODE) {
      Seq.empty
    } else {
      throw getChildrenResponse.resultException.get
    }
  }

  /**
   * Reads each of the isr change notifications associated with the given sequence numbers and extracts the partitions.
   * @param sequenceNumbers the sequence numbers associated with the isr change notifications.
   * @return partitions associated with the given isr change notifications.
   */
  def getPartitionsFromIsrChangeNotifications(sequenceNumbers: Seq[String]): Seq[TopicAndPartition] = {
    val getDataRequests = sequenceNumbers.map { sequenceNumber =>
      GetDataRequest(IsrChangeNotificationSequenceZNode.path(sequenceNumber))
    }
    val getDataResponses = retryRequestsUntilConnected(getDataRequests)
    getDataResponses.flatMap { getDataResponse =>
      if (getDataResponse.resultCode == Code.OK) {
        IsrChangeNotificationSequenceZNode.decode(getDataResponse.data)
      } else if (getDataResponse.resultCode == Code.NONODE) {
        None
      } else {
        throw getDataResponse.resultException.get
      }
    }
  }

  /**
   * Deletes all isr change notifications.
   */
  def deleteIsrChangeNotifications(): Unit = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(IsrChangeNotificationZNode.path))
    if (getChildrenResponse.resultCode == Code.OK) {
      deleteIsrChangeNotifications(getChildrenResponse.children)
    } else if (getChildrenResponse.resultCode != Code.NONODE) {
      throw getChildrenResponse.resultException.get
    }
  }

  /**
   * Deletes the isr change notifications associated with the given sequence numbers.
   * @param sequenceNumbers the sequence numbers associated with the isr change notifications to be deleted.
   */
  def deleteIsrChangeNotifications(sequenceNumbers: Seq[String]): Unit = {
    val deleteRequests = sequenceNumbers.map { sequenceNumber =>
      DeleteRequest(IsrChangeNotificationSequenceZNode.path(sequenceNumber), -1)
    }
    retryRequestsUntilConnected(deleteRequests)
  }

  /**
   * Gets the partitions marked for preferred replica election.
   * @return sequence of partitions.
   */
  def getPreferredReplicaElection: Set[TopicAndPartition] = {
    val getDataRequest = GetDataRequest(PreferredReplicaElectionZNode.path)
    val getDataResponse = retryRequestUntilConnected(getDataRequest)
    if (getDataResponse.resultCode == Code.OK) {
      PreferredReplicaElectionZNode.decode(getDataResponse.data)
    } else if (getDataResponse.resultCode == Code.NONODE) {
      Set.empty[TopicAndPartition]
    } else {
      throw getDataResponse.resultException.get
    }
  }

  /**
   * Deletes the preferred replica election znode.
   */
  def deletePreferredReplicaElection(): Unit = {
    val deleteRequest = DeleteRequest(PreferredReplicaElectionZNode.path, -1)
    retryRequestUntilConnected(deleteRequest)
  }

  /**
   * Gets the controller id.
   * @return optional integer that is Some if the controller znode exists and can be parsed and None otherwise.
   */
  def getControllerId: Option[Int] = {
    val getDataRequest = GetDataRequest(ControllerZNode.path)
    val getDataResponse = retryRequestUntilConnected(getDataRequest)
    if (getDataResponse.resultCode == Code.OK) {
      ControllerZNode.decode(getDataResponse.data)
    } else if (getDataResponse.resultCode == Code.NONODE) {
      None
    } else {
      throw getDataResponse.resultException.get
    }
  }

  /**
   * Deletes the controller znode.
   */
  def deleteController(): Unit = {
    val deleteRequest = DeleteRequest(ControllerZNode.path, -1)
    retryRequestUntilConnected(deleteRequest)
  }

  /**
   * Gets the controller epoch.
   * @return optional (Int, Stat) that is Some if the controller epoch path exists and None otherwise.
   */
  def getControllerEpoch: Option[(Int, Stat)] = {
    val getDataRequest = GetDataRequest(ControllerEpochZNode.path)
    val getDataResponse = retryRequestUntilConnected(getDataRequest)
    if (getDataResponse.resultCode == Code.OK) {
      val epoch = ControllerEpochZNode.decode(getDataResponse.data)
      Option(epoch, getDataResponse.stat)
    } else if (getDataResponse.resultCode == Code.NONODE) {
      None
    } else {
      throw getDataResponse.resultException.get
    }
  }

  /**
   * Recursively deletes the topic znode.
   * @param topic the topic whose topic znode we wish to delete.
   */
  def deleteTopicZNode(topic: String): Unit = {
    deleteRecursive(TopicZNode.path(topic))
  }

  /**
   * Deletes the topic configs for the given topics.
   * @param topics the topics whose configs we wish to delete.
   */
  def deleteTopicConfigs(topics: Seq[String]): Unit = {
    val deleteRequests = topics.map(topic => DeleteRequest(ConfigEntityZNode.path(ConfigType.Topic, topic), -1))
    retryRequestsUntilConnected(deleteRequests)
  }

  /**
   * This registers a ZNodeChangeHandler and attempts to register a watcher with an ExistsRequest, which allows data watcher
   * registrations on paths which might not even exist.
   *
   * @param zNodeChangeHandler
   */
  def registerZNodeChangeHandlerAndCheckExistence(zNodeChangeHandler: ZNodeChangeHandler): Unit = {
    zooKeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
    val existsResponse = retryRequestUntilConnected(ExistsRequest(zNodeChangeHandler.path))
    if (existsResponse.resultCode != Code.OK && existsResponse.resultCode != Code.NONODE) {
      throw existsResponse.resultException.get
    }
  }

  /**
   * See ZooKeeperClient.registerZNodeChangeHandler
   * @param zNodeChangeHandler
   */
  def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): Unit = {
    zooKeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
  }

  /**
   * See ZooKeeperClient.unregisterZNodeChangeHandler
   * @param path
   */
  def unregisterZNodeChangeHandler(path: String): Unit = {
    zooKeeperClient.unregisterZNodeChangeHandler(path)
  }

  /**
   * See ZooKeeperClient.registerZNodeChildChangeHandler
   * @param zNodeChildChangeHandler
   */
  def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): Unit = {
    zooKeeperClient.registerZNodeChildChangeHandler(zNodeChildChangeHandler)
  }

  /**
   * See ZooKeeperClient.unregisterZNodeChildChangeHandler
   * @param path
   */
  def unregisterZNodeChildChangeHandler(path: String): Unit = {
    zooKeeperClient.unregisterZNodeChildChangeHandler(path)
  }

  /**
   * Close the underlying ZooKeeperClient.
   */
  def close(): Unit = {
    zooKeeperClient.close()
  }

  private def deleteRecursive(path: String): Unit = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(path))
    if (getChildrenResponse.resultCode == Code.OK) {
      getChildrenResponse.children.foreach(child => deleteRecursive(s"$path/$child"))
      val deleteResponse = retryRequestUntilConnected(DeleteRequest(path, -1))
      if (deleteResponse.resultCode != Code.OK && deleteResponse.resultCode != Code.NONODE) {
        throw deleteResponse.resultException.get
      }
    } else if (getChildrenResponse.resultCode != Code.NONODE) {
      throw getChildrenResponse.resultException.get
    }
  }
  private def createTopicPartition(partitions: Seq[TopicAndPartition]) = {
    val createRequests = partitions.map { partition =>
      val path = TopicPartitionZNode.path(partition)
      CreateRequest(path, null, acls(path), CreateMode.PERSISTENT, Some(partition))
    }
    retryRequestsUntilConnected(createRequests)
  }

  private def createTopicPartitions(topics: Seq[String]) = {
    val createRequests = topics.map { topic =>
      val path = TopicPartitionsZNode.path(topic)
      CreateRequest(path, null, acls(path), CreateMode.PERSISTENT, Some(topic))
    }
    retryRequestsUntilConnected(createRequests)
  }

  private def getTopicConfigs(topics: Seq[String]) = {
    val getDataRequests = topics.map { topic =>
      GetDataRequest(ConfigEntityZNode.path(ConfigType.Topic, topic), ctx = Some(topic))
    }
    retryRequestsUntilConnected(getDataRequests)
  }

  private def acls(path: String) = {
    import scala.collection.JavaConverters._
    ZkUtils.defaultAcls(isSecure, path).asScala
  }

  private def retryRequestUntilConnected[Req <: AsyncRequest](request: Req): Req#Response = {
    retryRequestsUntilConnected(Seq(request)).head
  }

  private def retryRequestsUntilConnected[Req <: AsyncRequest](requests: Seq[Req]): Seq[Req#Response] = {
    val remainingRequests = ArrayBuffer(requests: _*)
    val responses = new ArrayBuffer[Req#Response]
    while (remainingRequests.nonEmpty) {
      val batchResponses = zooKeeperClient.handleRequests(remainingRequests)

      // Only execute slow path if we find a response with CONNECTIONLOSS
      if (batchResponses.exists(_.resultCode == Code.CONNECTIONLOSS)) {
        val requestResponsePairs = remainingRequests.zip(batchResponses)

        remainingRequests.clear()
        requestResponsePairs.foreach { case (request, response) =>
          if (response.resultCode == Code.CONNECTIONLOSS)
            remainingRequests += request
          else
            responses += response
        }

        if (remainingRequests.nonEmpty)
          zooKeeperClient.waitUntilConnected()
      } else {
        remainingRequests.clear()
        responses ++= batchResponses
      }
    }
    responses
  }

  def checkedEphemeralCreate(path: String, data: Array[Byte]): Unit = {
    val checkedEphemeral = new CheckedEphemeral(path, data)
    info(s"Creating $path (is it secure? $isSecure)")
    val code = checkedEphemeral.create()
    info(s"Result of znode creation at $path is: $code")
    code match {
      case Code.OK =>
      case _ => throw KeeperException.create(code)
    }
  }

  private class CheckedEphemeral(path: String, data: Array[Byte]) extends Logging {
    def create(): Code = {
      val createRequest = CreateRequest(path, data, acls(path), CreateMode.EPHEMERAL)
      val createResponse = retryRequestUntilConnected(createRequest)
      val code = createResponse.resultCode
      if (code == Code.OK) {
        code
      } else if (code == Code.NODEEXISTS) {
        get()
      } else {
        error(s"Error while creating ephemeral at $path with return code: $code")
        code
      }
    }

    private def get(): Code = {
      val getDataRequest = GetDataRequest(path)
      val getDataResponse = retryRequestUntilConnected(getDataRequest)
      val code = getDataResponse.resultCode
      if (code == Code.OK) {
        if (getDataResponse.stat.getEphemeralOwner != zooKeeperClient.sessionId) {
          error(s"Error while creating ephemeral at $path with return code: $code")
          Code.NODEEXISTS
        } else {
          code
        }
      } else if (code == Code.NONODE) {
        info(s"The ephemeral node at $path went away while reading it")
        create()
      } else {
        error(s"Error while creating ephemeral at $path with return code: $code")
        code
      }
    }
  }
}

object KafkaZkClient {

  /**
   * @param successfulPartitions The successfully updated partition states with adjusted znode versions.
   * @param partitionsToRetry The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts
   *                      can occur if the partition leader updated partition state while the controller attempted to
   *                      update partition state.
   * @param failedPartitions Exceptions corresponding to failed partition state updates.
   */
  case class UpdateLeaderAndIsrResult(successfulPartitions: Map[TopicAndPartition, LeaderAndIsr],
                                      partitionsToRetry: Seq[TopicAndPartition],
                                      failedPartitions: Map[TopicAndPartition, Exception])
}
