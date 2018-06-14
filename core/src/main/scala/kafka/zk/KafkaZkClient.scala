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

import com.yammer.metrics.core.MetricName
import kafka.api.LeaderAndIsr
import kafka.cluster.Broker
import kafka.common.KafkaException
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.log.LogConfig
import kafka.metrics.KafkaMetricsGroup
import kafka.security.auth.SimpleAclAuthorizer.VersionedAcls
import kafka.security.auth.{Acl, Resource, ResourceType}
import kafka.server.ConfigType
import kafka.utils.Logging
import kafka.zookeeper._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.zookeeper.KeeperException.{Code, NodeExistsException}
import org.apache.zookeeper.data.{ACL, Stat}
import org.apache.zookeeper.{CreateMode, KeeperException, ZooKeeper}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, mutable}

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
class KafkaZkClient private (zooKeeperClient: ZooKeeperClient, isSecure: Boolean, time: Time) extends AutoCloseable with
  Logging with KafkaMetricsGroup {

  override def metricName(name: String, metricTags: scala.collection.Map[String, String]): MetricName = {
    explicitMetricName("kafka.server", "ZooKeeperClientMetrics", name, metricTags)
  }

  private val latencyMetric = newHistogram("ZooKeeperRequestLatencyMs")

  import KafkaZkClient._

  // Only for testing
  private[kafka] def currentZooKeeper: ZooKeeper = zooKeeperClient.currentZooKeeper

  /**
   * Create a sequential persistent path. That is, the znode will not be automatically deleted upon client's disconnect
   * and a monotonically increasing number will be appended to its name.
   *
   * @param path the path to create (with the monotonically increasing number appended)
   * @param data the znode data
   * @return the created path (including the appended monotonically increasing number)
   */
  private[zk] def createSequentialPersistentPath(path: String, data: Array[Byte]): String = {
    val createRequest = CreateRequest(path, data, acls(path), CreateMode.PERSISTENT_SEQUENTIAL)
    val createResponse = retryRequestUntilConnected(createRequest)
    createResponse.maybeThrow
    createResponse.name
  }

  def registerBrokerInZk(brokerInfo: BrokerInfo): Unit = {
    val path = brokerInfo.path
    checkedEphemeralCreate(path, brokerInfo.toJsonBytes)
    info(s"Registered broker ${brokerInfo.broker.id} at path $path with addresses: ${brokerInfo.broker.endPoints}")
  }

  def updateBrokerInfoInZk(brokerInfo: BrokerInfo): Unit = {
    val brokerIdPath = brokerInfo.path
    val setDataRequest = SetDataRequest(brokerIdPath, brokerInfo.toJsonBytes, ZkVersion.NoVersion)
    val response = retryRequestUntilConnected(setDataRequest)
    response.maybeThrow()
    info("Updated broker %d at path %s with addresses: %s".format(brokerInfo.broker.id, brokerIdPath, brokerInfo.broker.endPoints))
  }

  /**
   * Gets topic partition states for the given partitions.
   * @param partitions the partitions for which we want ot get states.
   * @return sequence of GetDataResponses whose contexts are the partitions they are associated with.
   */
  def getTopicPartitionStatesRaw(partitions: Seq[TopicPartition]): Seq[GetDataResponse] = {
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
  def setTopicPartitionStatesRaw(leaderIsrAndControllerEpochs: Map[TopicPartition, LeaderIsrAndControllerEpoch]): Seq[SetDataResponse] = {
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
  def createTopicPartitionStatesRaw(leaderIsrAndControllerEpochs: Map[TopicPartition, LeaderIsrAndControllerEpoch]): Seq[CreateResponse] = {
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
   * Update the partition states of multiple partitions in zookeeper.
   * @param leaderAndIsrs The partition states to update.
   * @param controllerEpoch The current controller epoch.
   * @return UpdateLeaderAndIsrResult instance containing per partition results.
   */
  def updateLeaderAndIsr(leaderAndIsrs: Map[TopicPartition, LeaderAndIsr], controllerEpoch: Int): UpdateLeaderAndIsrResult = {
    val successfulUpdates = mutable.Map.empty[TopicPartition, LeaderAndIsr]
    val updatesToRetry = mutable.Buffer.empty[TopicPartition]
    val failed = mutable.Map.empty[TopicPartition, Exception]
    val leaderIsrAndControllerEpochs = leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      partition -> LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    }
    val setDataResponses = try {
      setTopicPartitionStatesRaw(leaderIsrAndControllerEpochs)
    } catch {
      case e: Exception =>
        leaderAndIsrs.keys.foreach(partition => failed.put(partition, e))
        return UpdateLeaderAndIsrResult(successfulUpdates.toMap, updatesToRetry, failed.toMap)
    }
    setDataResponses.foreach { setDataResponse =>
      val partition = setDataResponse.ctx.get.asInstanceOf[TopicPartition]
      setDataResponse.resultCode match {
        case Code.OK =>
          val updatedLeaderAndIsr = leaderAndIsrs(partition).withZkVersion(setDataResponse.stat.getVersion)
          successfulUpdates.put(partition, updatedLeaderAndIsr)
        case Code.BADVERSION => updatesToRetry += partition
        case _ => failed.put(partition, setDataResponse.resultException.get)
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
      configResponse.resultCode match {
        case Code.OK =>
          val overrides = ConfigEntityZNode.decode(configResponse.data)
          val logConfig = LogConfig.fromProps(config, overrides)
          logConfigs.put(topic, logConfig)
        case Code.NONODE =>
          val logConfig = LogConfig.fromProps(config, new Properties)
          logConfigs.put(topic, logConfig)
        case _ => failed.put(topic, configResponse.resultException.get)
      }
    }
    (logConfigs.toMap, failed.toMap)
  }

  /**
   * Get entity configs for a given entity name
   * @param rootEntityType entity type
   * @param sanitizedEntityName entity name
   * @return The successfully gathered log configs
   */
  def getEntityConfigs(rootEntityType: String, sanitizedEntityName: String): Properties = {
    val getDataRequest = GetDataRequest(ConfigEntityZNode.path(rootEntityType, sanitizedEntityName))
    val getDataResponse = retryRequestUntilConnected(getDataRequest)

    getDataResponse.resultCode match {
      case Code.OK =>
        ConfigEntityZNode.decode(getDataResponse.data)
      case Code.NONODE => new Properties()
      case _ => throw getDataResponse.resultException.get
    }
  }

  /**
   * Sets or creates the entity znode path with the given configs depending
   * on whether it already exists or not.
   * @param rootEntityType entity type
   * @param sanitizedEntityName entity name
   * @throws KeeperException if there is an error while setting or creating the znode
   */
  def setOrCreateEntityConfigs(rootEntityType: String, sanitizedEntityName: String, config: Properties) = {

    def set(configData: Array[Byte]): SetDataResponse = {
      val setDataRequest = SetDataRequest(ConfigEntityZNode.path(rootEntityType, sanitizedEntityName), ConfigEntityZNode.encode(config), ZkVersion.NoVersion)
      retryRequestUntilConnected(setDataRequest)
    }

    def create(configData: Array[Byte]) = {
      val path = ConfigEntityZNode.path(rootEntityType, sanitizedEntityName)
      createRecursive(path, ConfigEntityZNode.encode(config))
    }

    val configData = ConfigEntityZNode.encode(config)

    val setDataResponse = set(configData)
    setDataResponse.resultCode match {
      case Code.NONODE => create(configData)
      case _ => setDataResponse.maybeThrow
    }
  }

  /**
   * Returns all the entities for a given entityType
   * @param entityType entity type
   * @return List of all entity names
   */
  def getAllEntitiesWithConfig(entityType: String): Seq[String] = {
    getChildren(ConfigEntityTypeZNode.path(entityType))
  }

  /**
   * Creates config change notification
   * @param sanitizedEntityPath  sanitizedEntityPath path to write
   * @throws KeeperException if there is an error while setting or creating the znode
   */
  def createConfigChangeNotification(sanitizedEntityPath: String): Unit = {
    makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)
    val path = ConfigEntityChangeNotificationSequenceZNode.createPath
    val createRequest = CreateRequest(path, ConfigEntityChangeNotificationSequenceZNode.encode(sanitizedEntityPath), acls(path), CreateMode.PERSISTENT_SEQUENTIAL)
    val createResponse = retryRequestUntilConnected(createRequest)
    createResponse.maybeThrow()
  }

  /**
   * Gets all brokers in the cluster.
   * @return sequence of brokers in the cluster.
   */
  def getAllBrokersInCluster: Seq[Broker] = {
    val brokerIds = getSortedBrokerList
    val getDataRequests = brokerIds.map(brokerId => GetDataRequest(BrokerIdZNode.path(brokerId), ctx = Some(brokerId)))
    val getDataResponses = retryRequestsUntilConnected(getDataRequests)
    getDataResponses.flatMap { getDataResponse =>
      val brokerId = getDataResponse.ctx.get.asInstanceOf[Int]
      getDataResponse.resultCode match {
        case Code.OK =>
          Option(BrokerIdZNode.decode(brokerId, getDataResponse.data).broker)
        case Code.NONODE => None
        case _ => throw getDataResponse.resultException.get
      }
    }
  }

  /**
    * Get a broker from ZK
    * @return an optional Broker
    */
  def getBroker(brokerId: Int): Option[Broker] = {
    val getDataRequest = GetDataRequest(BrokerIdZNode.path(brokerId))
    val getDataResponse = retryRequestUntilConnected(getDataRequest)
    getDataResponse.resultCode match {
      case Code.OK =>
        Option(BrokerIdZNode.decode(brokerId, getDataResponse.data).broker)
      case Code.NONODE => None
      case _ => throw getDataResponse.resultException.get
    }
  }

  /**
   * Gets the list of sorted broker Ids
   */
  def getSortedBrokerList(): Seq[Int] =
    getChildren(BrokerIdsZNode.path).map(_.toInt).sorted

  /**
   * Gets all topics in the cluster.
   * @return sequence of topics in the cluster.
   */
  def getAllTopicsInCluster: Seq[String] = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(TopicsZNode.path))
    getChildrenResponse.resultCode match {
      case Code.OK => getChildrenResponse.children
      case Code.NONODE => Seq.empty
      case _ => throw getChildrenResponse.resultException.get
    }

  }

  /**
   * Checks the topic existence
   * @param topicName
   * @return true if topic exists else false
   */
  def topicExists(topicName: String): Boolean = {
    pathExists(TopicZNode.path(topicName))
  }

  /**
   * Sets the topic znode with the given assignment.
   * @param topic the topic whose assignment is being set.
   * @param assignment the partition to replica mapping to set for the given topic
   * @return SetDataResponse
   */
  def setTopicAssignmentRaw(topic: String, assignment: collection.Map[TopicPartition, Seq[Int]]): SetDataResponse = {
    val setDataRequest = SetDataRequest(TopicZNode.path(topic), TopicZNode.encode(assignment), ZkVersion.NoVersion)
    retryRequestUntilConnected(setDataRequest)
  }

  /**
   * Sets the topic znode with the given assignment.
   * @param topic the topic whose assignment is being set.
   * @param assignment the partition to replica mapping to set for the given topic
   * @throws KeeperException if there is an error while setting assignment
   */
  def setTopicAssignment(topic: String, assignment: Map[TopicPartition, Seq[Int]]) = {
    val setDataResponse = setTopicAssignmentRaw(topic, assignment)
    setDataResponse.maybeThrow
  }

  /**
   * Create the topic znode with the given assignment.
   * @param topic the topic whose assignment is being set.
   * @param assignment the partition to replica mapping to set for the given topic
   * @throws KeeperException if there is an error while creating assignment
   */
  def createTopicAssignment(topic: String, assignment: Map[TopicPartition, Seq[Int]]) = {
    createRecursive(TopicZNode.path(topic), TopicZNode.encode(assignment))
  }

  /**
   * Gets the log dir event notifications as strings. These strings are the znode names and not the absolute znode path.
   * @return sequence of znode names and not the absolute znode path.
   */
  def getAllLogDirEventNotifications: Seq[String] = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(LogDirEventNotificationZNode.path))
    getChildrenResponse.resultCode match {
      case Code.OK => getChildrenResponse.children.map(LogDirEventNotificationSequenceZNode.sequenceNumber)
      case Code.NONODE => Seq.empty
      case _ => throw getChildrenResponse.resultException.get
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
      getDataResponse.resultCode match {
        case Code.OK => LogDirEventNotificationSequenceZNode.decode(getDataResponse.data)
        case Code.NONODE => None
        case _ => throw getDataResponse.resultException.get
      }
    }
  }

  /**
   * Deletes all log dir event notifications.
   */
  def deleteLogDirEventNotifications(): Unit = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(LogDirEventNotificationZNode.path))
    if (getChildrenResponse.resultCode == Code.OK) {
      deleteLogDirEventNotifications(getChildrenResponse.children.map(LogDirEventNotificationSequenceZNode.sequenceNumber))
    } else if (getChildrenResponse.resultCode != Code.NONODE) {
      getChildrenResponse.maybeThrow
    }
  }

  /**
   * Deletes the log dir event notifications associated with the given sequence numbers.
   * @param sequenceNumbers the sequence numbers associated with the log dir event notifications to be deleted.
   */
  def deleteLogDirEventNotifications(sequenceNumbers: Seq[String]): Unit = {
    val deleteRequests = sequenceNumbers.map { sequenceNumber =>
      DeleteRequest(LogDirEventNotificationSequenceZNode.path(sequenceNumber), ZkVersion.NoVersion)
    }
    retryRequestsUntilConnected(deleteRequests)
  }

  /**
   * Gets the assignments for the given topics.
   * @param topics the topics whose partitions we wish to get the assignments for.
   * @return the replica assignment for each partition from the given topics.
   */
  def getReplicaAssignmentForTopics(topics: Set[String]): Map[TopicPartition, Seq[Int]] = {
    val getDataRequests = topics.map(topic => GetDataRequest(TopicZNode.path(topic), ctx = Some(topic)))
    val getDataResponses = retryRequestsUntilConnected(getDataRequests.toSeq)
    getDataResponses.flatMap { getDataResponse =>
      val topic = getDataResponse.ctx.get.asInstanceOf[String]
      getDataResponse.resultCode match {
        case Code.OK => TopicZNode.decode(topic, getDataResponse.data)
        case Code.NONODE => Map.empty[TopicPartition, Seq[Int]]
        case _ => throw getDataResponse.resultException.get
      }
    }.toMap
  }

  /**
   * Gets partition the assignments for the given topics.
   * @param topics the topics whose partitions we wish to get the assignments for.
   * @return the partition assignment for each partition from the given topics.
   */
  def getPartitionAssignmentForTopics(topics: Set[String]): Map[String, Map[Int, Seq[Int]]] = {
    val getDataRequests = topics.map(topic => GetDataRequest(TopicZNode.path(topic), ctx = Some(topic)))
    val getDataResponses = retryRequestsUntilConnected(getDataRequests.toSeq)
    getDataResponses.flatMap { getDataResponse =>
      val topic = getDataResponse.ctx.get.asInstanceOf[String]
       if (getDataResponse.resultCode == Code.OK) {
        val partitionMap = TopicZNode.decode(topic, getDataResponse.data).map { case (k, v) => (k.partition, v) }
        Map(topic -> partitionMap)
      } else if (getDataResponse.resultCode == Code.NONODE) {
        Map.empty[String, Map[Int, Seq[Int]]]
      } else {
        throw getDataResponse.resultException.get
      }
    }.toMap
  }

  /**
   * Gets the partition numbers for the given topics
   * @param topics the topics whose partitions we wish to get.
   * @return the partition array for each topic from the given topics.
   */
  def getPartitionsForTopics(topics: Set[String]): Map[String, Seq[Int]] = {
    getPartitionAssignmentForTopics(topics).map { topicAndPartitionMap =>
      val topic = topicAndPartitionMap._1
      val partitionMap = topicAndPartitionMap._2
      topic -> partitionMap.keys.toSeq.sortWith((s, t) => s < t)
    }
  }

  /**
   * Gets the partition count for a given topic
   * @param topic The topic to get partition count for.
   * @return  optional integer that is Some if the topic exists and None otherwise.
   */
  def getTopicPartitionCount(topic: String): Option[Int] = {
    val topicData = getReplicaAssignmentForTopics(Set(topic))
    if (topicData.nonEmpty)
      Some(topicData.size)
    else
      None
  }

  /**
   * Gets the assigned replicas for a specific topic and partition
   * @param topicPartition TopicAndPartition to get assigned replicas for .
   * @return List of assigned replicas
   */
  def getReplicasForPartition(topicPartition: TopicPartition): Seq[Int] = {
    val topicData = getReplicaAssignmentForTopics(Set(topicPartition.topic))
    topicData.getOrElse(topicPartition, Seq.empty)
  }

  /**
   * Gets all partitions in the cluster
   * @return all partitions in the cluster
   */
  def getAllPartitions(): Set[TopicPartition] = {
    val topics = getChildren(TopicsZNode.path)
    if (topics == null) Set.empty
    else {
      topics.flatMap { topic =>
        // The partitions path may not exist if the topic is in the process of being deleted
        getChildren(TopicPartitionsZNode.path(topic)).map(_.toInt).map(new TopicPartition(topic, _))
      }.toSet
    }
  }

  /**
   * Gets the data and version at the given zk path
   * @param path zk node path
   * @return A tuple of 2 elements, where first element is zk node data as an array of bytes
   *         and second element is zk node version.
   *         returns (None, ZkVersion.NoVersion) if node doesn't exists and throws exception for any error
   */
  def getDataAndVersion(path: String): (Option[Array[Byte]], Int) = {
    val (data, stat) = getDataAndStat(path)
    stat match {
      case ZkStat.NoStat => (data, ZkVersion.NoVersion)
      case _ => (data, stat.getVersion)
    }
  }

  /**
   * Gets the data and Stat at the given zk path
   * @param path zk node path
   * @return A tuple of 2 elements, where first element is zk node data as an array of bytes
   *         and second element is zk node stats.
   *         returns (None, ZkStat.NoStat) if node doesn't exists and throws exception for any error
   */
  def getDataAndStat(path: String): (Option[Array[Byte]], Stat) = {
    val getDataRequest = GetDataRequest(path)
    val getDataResponse = retryRequestUntilConnected(getDataRequest)

    getDataResponse.resultCode match {
      case Code.OK => (Option(getDataResponse.data), getDataResponse.stat)
      case Code.NONODE => (None, ZkStat.NoStat)
      case _ => throw getDataResponse.resultException.get
    }
  }

  /**
   * Gets all the child nodes at a given zk node path
   * @param path
   * @return list of child node names
   */
  def getChildren(path : String): Seq[String] = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(path))
    getChildrenResponse.resultCode match {
      case Code.OK => getChildrenResponse.children
      case Code.NONODE => Seq.empty
      case _ => throw getChildrenResponse.resultException.get
    }
  }

  /**
   * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the path doesn't
   * exist, the current version is not the expected version, etc.) return (false, ZkVersion.NoVersion)
   *
   * When there is a ConnectionLossException during the conditional update, ZookeeperClient will retry the update and may fail
   * since the previous update may have succeeded (but the stored zkVersion no longer matches the expected one).
   * In this case, we will run the optionalChecker to further check if the previous write did indeed succeeded.
   */
  def conditionalUpdatePath(path: String, data: Array[Byte], expectVersion: Int,
                            optionalChecker: Option[(KafkaZkClient, String, Array[Byte]) => (Boolean,Int)] = None): (Boolean, Int) = {

    val setDataRequest = SetDataRequest(path, data, expectVersion)
    val setDataResponse = retryRequestUntilConnected(setDataRequest)

    setDataResponse.resultCode match {
      case Code.OK =>
        debug("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d"
          .format(path, Utils.utf8(data), expectVersion, setDataResponse.stat.getVersion))
        (true, setDataResponse.stat.getVersion)

      case Code.BADVERSION =>
        optionalChecker match {
          case Some(checker) => checker(this, path, data)
          case _ =>
            debug("Checker method is not passed skipping zkData match")
            debug("Conditional update of path %s with data %s and expected version %d failed due to %s"
              .format(path, Utils.utf8(data), expectVersion, setDataResponse.resultException.get.getMessage))
            (false, ZkVersion.NoVersion)
        }

      case Code.NONODE =>
        debug("Conditional update of path %s with data %s and expected version %d failed due to %s".format(path,
          Utils.utf8(data), expectVersion, setDataResponse.resultException.get.getMessage))
        (false, ZkVersion.NoVersion)

      case _ =>
        debug("Conditional update of path %s with data %s and expected version %d failed due to %s".format(path,
          Utils.utf8(data), expectVersion, setDataResponse.resultException.get.getMessage))
        throw setDataResponse.resultException.get
    }
  }

  /**
   * Creates the delete topic znode.
   * @param topicName topic name
   * @throws KeeperException if there is an error while setting or creating the znode
   */
  def createDeleteTopicPath(topicName: String): Unit = {
    createRecursive(DeleteTopicsTopicZNode.path(topicName))
  }

  /**
   * Checks if topic is marked for deletion
   * @param topic
   * @return true if topic is marked for deletion, else false
   */
  def isTopicMarkedForDeletion(topic: String): Boolean = {
    pathExists(DeleteTopicsTopicZNode.path(topic))
  }

  /**
   * Get all topics marked for deletion.
   * @return sequence of topics marked for deletion.
   */
  def getTopicDeletions: Seq[String] = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(DeleteTopicsZNode.path))
    getChildrenResponse.resultCode match {
      case Code.OK => getChildrenResponse.children
      case Code.NONODE => Seq.empty
      case _ => throw getChildrenResponse.resultException.get
    }
  }

  /**
   * Remove the given topics from the topics marked for deletion.
   * @param topics the topics to remove.
   */
  def deleteTopicDeletions(topics: Seq[String]): Unit = {
    val deleteRequests = topics.map(topic => DeleteRequest(DeleteTopicsTopicZNode.path(topic), ZkVersion.NoVersion))
    retryRequestsUntilConnected(deleteRequests)
  }

  /**
   * Returns all reassignments.
   * @return the reassignments for each partition.
   */
  def getPartitionReassignment: collection.Map[TopicPartition, Seq[Int]] = {
    val getDataRequest = GetDataRequest(ReassignPartitionsZNode.path)
    val getDataResponse = retryRequestUntilConnected(getDataRequest)
    getDataResponse.resultCode match {
      case Code.OK =>
        ReassignPartitionsZNode.decode(getDataResponse.data) match {
          case Left(e) =>
            logger.warn(s"Ignoring partition reassignment due to invalid json: ${e.getMessage}", e)
            Map.empty[TopicPartition, Seq[Int]]
          case Right(assignments) => assignments
        }
      case Code.NONODE => Map.empty
      case _ => throw getDataResponse.resultException.get
    }
  }

  /**
   * Sets or creates the partition reassignment znode with the given reassignment depending on whether it already
   * exists or not.
   *
   * @param reassignment the reassignment to set on the reassignment znode
   * @throws KeeperException if there is an error while setting or creating the znode
   */
  def setOrCreatePartitionReassignment(reassignment: collection.Map[TopicPartition, Seq[Int]]): Unit = {

    def set(reassignmentData: Array[Byte]): SetDataResponse = {
      val setDataRequest = SetDataRequest(ReassignPartitionsZNode.path, reassignmentData, ZkVersion.NoVersion)
      retryRequestUntilConnected(setDataRequest)
    }

    def create(reassignmentData: Array[Byte]): CreateResponse = {
      val createRequest = CreateRequest(ReassignPartitionsZNode.path, reassignmentData, acls(ReassignPartitionsZNode.path),
        CreateMode.PERSISTENT)
      retryRequestUntilConnected(createRequest)
    }

    val reassignmentData = ReassignPartitionsZNode.encode(reassignment)
    val setDataResponse = set(reassignmentData)
    setDataResponse.resultCode match {
      case Code.NONODE =>
        val createDataResponse = create(reassignmentData)
        createDataResponse.maybeThrow
      case _ => setDataResponse.maybeThrow
    }
  }

  /**
   * Creates the partition reassignment znode with the given reassignment.
   * @param reassignment the reassignment to set on the reassignment znode.
   * @throws KeeperException if there is an error while creating the znode
   */
  def createPartitionReassignment(reassignment: Map[TopicPartition, Seq[Int]])  = {
    createRecursive(ReassignPartitionsZNode.path, ReassignPartitionsZNode.encode(reassignment))
  }

  /**
   * Deletes the partition reassignment znode.
   */
  def deletePartitionReassignment(): Unit = {
    val deleteRequest = DeleteRequest(ReassignPartitionsZNode.path, ZkVersion.NoVersion)
    retryRequestUntilConnected(deleteRequest)
  }

  /**
   * Checks if reassign partitions is in progress
   * @return true if reassign partitions is in progress, else false
   */
  def reassignPartitionsInProgress(): Boolean = {
    pathExists(ReassignPartitionsZNode.path)
  }

  /**
   * Gets topic partition states for the given partitions.
   * @param partitions the partitions for which we want to get states.
   * @return map containing LeaderIsrAndControllerEpoch of each partition for we were able to lookup the partition state.
   */
  def getTopicPartitionStates(partitions: Seq[TopicPartition]): Map[TopicPartition, LeaderIsrAndControllerEpoch] = {
    val getDataResponses = getTopicPartitionStatesRaw(partitions)
    getDataResponses.flatMap { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      getDataResponse.resultCode match {
        case Code.OK => TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat).map(partition -> _)
        case Code.NONODE => None
        case _ => throw getDataResponse.resultException.get
      }
    }.toMap
  }

  /**
   * Gets topic partition state for the given partition.
   * @param partition the partition for which we want to get state.
   * @return LeaderIsrAndControllerEpoch of the partition state if exists, else None
   */
  def getTopicPartitionState(partition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    val getDataResponse = getTopicPartitionStatesRaw(Seq(partition)).head
    if (getDataResponse.resultCode == Code.OK) {
      TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat)
    } else if (getDataResponse.resultCode == Code.NONODE) {
      None
    } else {
      throw getDataResponse.resultException.get
    }
  }

  /**
   * Gets the leader for a given partition
   * @param partition The partition for which we want to get leader.
   * @return optional integer if the leader exists and None otherwise.
   */
  def getLeaderForPartition(partition: TopicPartition): Option[Int] =
    getTopicPartitionState(partition).map(_.leaderAndIsr.leader)

  /**
   * Gets the in-sync replicas (ISR) for a specific topicPartition
   * @param partition The partition for which we want to get ISR.
   * @return optional ISR if exists and None otherwise
   */
  def getInSyncReplicasForPartition(partition: TopicPartition): Option[Seq[Int]] =
    getTopicPartitionState(partition).map(_.leaderAndIsr.isr)


  /**
   * Gets the leader epoch for a specific topicPartition
   * @param partition The partition for which we want to get the leader epoch
   * @return optional integer if the leader exists and None otherwise
   */
  def getEpochForPartition(partition: TopicPartition): Option[Int] = {
    getTopicPartitionState(partition).map(_.leaderAndIsr.leaderEpoch)
  }

  /**
   * Gets the isr change notifications as strings. These strings are the znode names and not the absolute znode path.
   * @return sequence of znode names and not the absolute znode path.
   */
  def getAllIsrChangeNotifications: Seq[String] = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(IsrChangeNotificationZNode.path))
    getChildrenResponse.resultCode match {
      case Code.OK => getChildrenResponse.children.map(IsrChangeNotificationSequenceZNode.sequenceNumber)
      case Code.NONODE => Seq.empty
      case _ => throw getChildrenResponse.resultException.get
    }
  }

  /**
   * Reads each of the isr change notifications associated with the given sequence numbers and extracts the partitions.
   * @param sequenceNumbers the sequence numbers associated with the isr change notifications.
   * @return partitions associated with the given isr change notifications.
   */
  def getPartitionsFromIsrChangeNotifications(sequenceNumbers: Seq[String]): Seq[TopicPartition] = {
    val getDataRequests = sequenceNumbers.map { sequenceNumber =>
      GetDataRequest(IsrChangeNotificationSequenceZNode.path(sequenceNumber))
    }
    val getDataResponses = retryRequestsUntilConnected(getDataRequests)
    getDataResponses.flatMap { getDataResponse =>
      getDataResponse.resultCode match {
        case Code.OK => IsrChangeNotificationSequenceZNode.decode(getDataResponse.data)
        case Code.NONODE => None
        case _ => throw getDataResponse.resultException.get
      }
    }
  }

  /**
   * Deletes all isr change notifications.
   */
  def deleteIsrChangeNotifications(): Unit = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(IsrChangeNotificationZNode.path))
    if (getChildrenResponse.resultCode == Code.OK) {
      deleteIsrChangeNotifications(getChildrenResponse.children.map(IsrChangeNotificationSequenceZNode.sequenceNumber))
    } else if (getChildrenResponse.resultCode != Code.NONODE) {
      getChildrenResponse.maybeThrow
    }
  }

  /**
   * Deletes the isr change notifications associated with the given sequence numbers.
   * @param sequenceNumbers the sequence numbers associated with the isr change notifications to be deleted.
   */
  def deleteIsrChangeNotifications(sequenceNumbers: Seq[String]): Unit = {
    val deleteRequests = sequenceNumbers.map { sequenceNumber =>
      DeleteRequest(IsrChangeNotificationSequenceZNode.path(sequenceNumber), ZkVersion.NoVersion)
    }
    retryRequestsUntilConnected(deleteRequests)
  }

  /**
   * Creates preferred replica election znode with partitions undergoing election
   * @param partitions
   * @throws KeeperException if there is an error while creating the znode
   */
  def createPreferredReplicaElection(partitions: Set[TopicPartition]): Unit  = {
    createRecursive(PreferredReplicaElectionZNode.path, PreferredReplicaElectionZNode.encode(partitions))
  }

  /**
   * Gets the partitions marked for preferred replica election.
   * @return sequence of partitions.
   */
  def getPreferredReplicaElection: Set[TopicPartition] = {
    val getDataRequest = GetDataRequest(PreferredReplicaElectionZNode.path)
    val getDataResponse = retryRequestUntilConnected(getDataRequest)
    getDataResponse.resultCode match {
      case Code.OK => PreferredReplicaElectionZNode.decode(getDataResponse.data)
      case Code.NONODE => Set.empty
      case _ => throw getDataResponse.resultException.get
    }
  }

  /**
   * Deletes the preferred replica election znode.
   */
  def deletePreferredReplicaElection(): Unit = {
    val deleteRequest = DeleteRequest(PreferredReplicaElectionZNode.path, ZkVersion.NoVersion)
    retryRequestUntilConnected(deleteRequest)
  }

  /**
   * Gets the controller id.
   * @return optional integer that is Some if the controller znode exists and can be parsed and None otherwise.
   */
  def getControllerId: Option[Int] = {
    val getDataRequest = GetDataRequest(ControllerZNode.path)
    val getDataResponse = retryRequestUntilConnected(getDataRequest)
    getDataResponse.resultCode match {
      case Code.OK => ControllerZNode.decode(getDataResponse.data)
      case Code.NONODE => None
      case _ => throw getDataResponse.resultException.get
    }
  }

  /**
   * Deletes the controller znode.
   */
  def deleteController(): Unit = {
    val deleteRequest = DeleteRequest(ControllerZNode.path, ZkVersion.NoVersion)
    retryRequestUntilConnected(deleteRequest)
  }

  /**
   * Gets the controller epoch.
   * @return optional (Int, Stat) that is Some if the controller epoch path exists and None otherwise.
   */
  def getControllerEpoch: Option[(Int, Stat)] = {
    val getDataRequest = GetDataRequest(ControllerEpochZNode.path)
    val getDataResponse = retryRequestUntilConnected(getDataRequest)
    getDataResponse.resultCode match {
      case Code.OK =>
        val epoch = ControllerEpochZNode.decode(getDataResponse.data)
        Option(epoch, getDataResponse.stat)
      case Code.NONODE => None
      case _ => throw getDataResponse.resultException.get
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
    val deleteRequests = topics.map(topic => DeleteRequest(ConfigEntityZNode.path(ConfigType.Topic, topic), ZkVersion.NoVersion))
    retryRequestsUntilConnected(deleteRequests)
  }

  //Acl management methods

  /**
   * Creates the required zk nodes for Acl storage and Acl change storage.
   */
  def createAclPaths(): Unit = {
    ZkAclStore.stores.foreach(store => {
      createRecursive(store.aclPath, throwIfPathExists = false)
      ResourceType.values.foreach(resourceType => createRecursive(store.path(resourceType), throwIfPathExists = false))
    })

    ZkAclChangeStore.stores.foreach(store => createRecursive(store.aclChangePath, throwIfPathExists = false))
  }

  /**
   * Gets VersionedAcls for a given Resource
   * @param resource Resource to get VersionedAcls for
   * @return  VersionedAcls
   */
  def getVersionedAclsForResource(resource: Resource): VersionedAcls = {
    val getDataRequest = GetDataRequest(ResourceZNode.path(resource))
    val getDataResponse = retryRequestUntilConnected(getDataRequest)
    getDataResponse.resultCode match {
      case Code.OK => ResourceZNode.decode(getDataResponse.data, getDataResponse.stat)
      case Code.NONODE => VersionedAcls(Set(), -1)
      case _ => throw getDataResponse.resultException.get
    }
  }

  /**
   * Sets or creates the resource znode path with the given acls and expected zk version depending
   * on whether it already exists or not.
   * @param resource
   * @param aclsSet
   * @param expectedVersion
   * @return true if the update was successful and the new version
   */
  def conditionalSetOrCreateAclsForResource(resource: Resource, aclsSet: Set[Acl], expectedVersion: Int): (Boolean, Int) = {
    def set(aclData: Array[Byte],  expectedVersion: Int): SetDataResponse = {
      val setDataRequest = SetDataRequest(ResourceZNode.path(resource), aclData, expectedVersion)
      retryRequestUntilConnected(setDataRequest)
    }

    def create(aclData: Array[Byte]): CreateResponse = {
      val path = ResourceZNode.path(resource)
      val createRequest = CreateRequest(path, aclData, acls(path), CreateMode.PERSISTENT)
      retryRequestUntilConnected(createRequest)
    }

    val aclData = ResourceZNode.encode(aclsSet)

    val setDataResponse = set(aclData, expectedVersion)
    setDataResponse.resultCode match {
      case Code.OK => (true, setDataResponse.stat.getVersion)
      case Code.NONODE => {
        val createResponse = create(aclData)
        createResponse.resultCode match {
          case Code.OK => (true, 0)
          case Code.NODEEXISTS => (false, 0)
          case _ => throw createResponse.resultException.get
        }
      }
      case Code.BADVERSION => (false, 0)
      case _ => throw setDataResponse.resultException.get
    }
  }

  /**
   * Creates an Acl change notification message.
   * @param resource resource pattern that has changed
   */
  def createAclChangeNotification(resource: Resource): Unit = {
    val aclChange = ZkAclStore(resource.patternType).changeStore.createChangeNode(resource)
    val createRequest = CreateRequest(aclChange.path, aclChange.bytes, acls(aclChange.path), CreateMode.PERSISTENT_SEQUENTIAL)
    val createResponse = retryRequestUntilConnected(createRequest)
    createResponse.maybeThrow
  }

  def propagateLogDirEvent(brokerId: Int) {
    val logDirEventNotificationPath: String = createSequentialPersistentPath(
      LogDirEventNotificationZNode.path + "/" + LogDirEventNotificationSequenceZNode.SequenceNumberPrefix,
      LogDirEventNotificationSequenceZNode.encode(brokerId))
    debug(s"Added $logDirEventNotificationPath for broker $brokerId")
  }

  def propagateIsrChanges(isrChangeSet: collection.Set[TopicPartition]): Unit = {
    val isrChangeNotificationPath: String = createSequentialPersistentPath(IsrChangeNotificationSequenceZNode.path(),
      IsrChangeNotificationSequenceZNode.encode(isrChangeSet))
    debug(s"Added $isrChangeNotificationPath for $isrChangeSet")
  }

  /**
   * Deletes all Acl change notifications.
   * @throws KeeperException if there is an error while deleting Acl change notifications
   */
  def deleteAclChangeNotifications(): Unit = {
    ZkAclChangeStore.stores.foreach(store => {
      val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(store.aclChangePath))
      if (getChildrenResponse.resultCode == Code.OK) {
        deleteAclChangeNotifications(store.aclChangePath, getChildrenResponse.children)
      } else if (getChildrenResponse.resultCode != Code.NONODE) {
        getChildrenResponse.maybeThrow
      }
    })
  }

  /**
    * Deletes the Acl change notifications associated with the given sequence nodes
    *
    * @param aclChangePath the root path
    * @param sequenceNodes the name of the node to delete.
    */
  private def deleteAclChangeNotifications(aclChangePath: String, sequenceNodes: Seq[String]): Unit = {
    val deleteRequests = sequenceNodes.map { sequenceNode =>
      DeleteRequest(s"$aclChangePath/$sequenceNode", ZkVersion.NoVersion)
    }

    val deleteResponses = retryRequestsUntilConnected(deleteRequests)
    deleteResponses.foreach { deleteResponse =>
      if (deleteResponse.resultCode != Code.NONODE) {
        deleteResponse.maybeThrow
      }
    }
  }

  /**
   * Gets the resource types, for which ACLs are stored, for the supplied resource pattern type.
   * @param patternType The resource pattern type to retrieve the names for.
   * @return list of resource type names
   */
  def getResourceTypes(patternType: PatternType): Seq[String] = {
    getChildren(ZkAclStore(patternType).aclPath)
  }

  /**
   * Gets the resource names, for which ACLs are stored, for a given resource type and pattern type
   * @param patternType The resource pattern type to retrieve the names for.
   * @param resourceType Resource type to retrieve the names for.
   * @return list of resource names
   */
  def getResourceNames(patternType: PatternType, resourceType: ResourceType): Seq[String] = {
    getChildren(ZkAclStore(patternType).path(resourceType))
  }

  /**
   * Deletes the given Resource node
   * @param resource
   * @return delete status
   */
  def deleteResource(resource: Resource): Boolean = {
    deleteRecursive(ResourceZNode.path(resource))
  }

  /**
   * checks the resource existence
   * @param resource
   * @return existence status
   */
  def resourceExists(resource: Resource): Boolean = {
    pathExists(ResourceZNode.path(resource))
  }

  /**
   * Conditional delete the resource node
   * @param resource
   * @param expectedVersion
   * @return return true if it succeeds, false otherwise (the current version is not the expected version)
   */
  def conditionalDelete(resource: Resource, expectedVersion: Int): Boolean = {
    val deleteRequest = DeleteRequest(ResourceZNode.path(resource), expectedVersion)
    val deleteResponse = retryRequestUntilConnected(deleteRequest)
    deleteResponse.resultCode match {
      case Code.OK | Code.NONODE => true
      case Code.BADVERSION => false
      case _ => throw deleteResponse.resultException.get
    }
  }

  /**
   * Deletes the zk node recursively
   * @param path
   * @return  return true if it succeeds, false otherwise
   */
  def deletePath(path: String): Boolean = {
    deleteRecursive(path)
  }

  /**
   * Creates the required zk nodes for Delegation Token storage
   */
  def createDelegationTokenPaths(): Unit = {
    createRecursive(DelegationTokenChangeNotificationZNode.path, throwIfPathExists = false)
    createRecursive(DelegationTokensZNode.path, throwIfPathExists = false)
  }

  /**
   * Creates Delegation Token change notification message
   * @param tokenId token Id
   */
  def createTokenChangeNotification(tokenId: String): Unit = {
    val path = DelegationTokenChangeNotificationSequenceZNode.createPath
    val createRequest = CreateRequest(path, DelegationTokenChangeNotificationSequenceZNode.encode(tokenId), acls(path), CreateMode.PERSISTENT_SEQUENTIAL)
    val createResponse = retryRequestUntilConnected(createRequest)
    createResponse.resultException.foreach(e => throw e)
  }

  /**
   * Sets or creates token info znode with the given token details depending on whether it already
   * exists or not.
   *
   * @param token the token to set on the token znode
   * @throws KeeperException if there is an error while setting or creating the znode
   */
  def setOrCreateDelegationToken(token: DelegationToken): Unit = {

    def set(tokenData: Array[Byte]): SetDataResponse = {
      val setDataRequest = SetDataRequest(DelegationTokenInfoZNode.path(token.tokenInfo().tokenId()), tokenData, ZkVersion.NoVersion)
      retryRequestUntilConnected(setDataRequest)
    }

    def create(tokenData: Array[Byte]): CreateResponse = {
      val path = DelegationTokenInfoZNode.path(token.tokenInfo().tokenId())
      val createRequest = CreateRequest(path, tokenData, acls(path), CreateMode.PERSISTENT)
      retryRequestUntilConnected(createRequest)
    }

    val tokenInfo = DelegationTokenInfoZNode.encode(token)
    val setDataResponse = set(tokenInfo)
    setDataResponse.resultCode match {
      case Code.NONODE =>
        val createDataResponse = create(tokenInfo)
        createDataResponse.maybeThrow
      case _ => setDataResponse.maybeThrow
    }
  }

  /**
   * Gets the Delegation Token Info
   * @return optional TokenInfo that is Some if the token znode exists and can be parsed and None otherwise.
   */
  def getDelegationTokenInfo(delegationTokenId: String): Option[TokenInformation] = {
    val getDataRequest = GetDataRequest(DelegationTokenInfoZNode.path(delegationTokenId))
    val getDataResponse = retryRequestUntilConnected(getDataRequest)
    getDataResponse.resultCode match {
      case Code.OK => DelegationTokenInfoZNode.decode(getDataResponse.data)
      case Code.NONODE => None
      case _ => throw getDataResponse.resultException.get
    }
  }

  /**
   * Deletes the given Delegation token node
   * @param delegationTokenId
   * @return delete status
   */
  def deleteDelegationToken(delegationTokenId: String): Boolean = {
    deleteRecursive(DelegationTokenInfoZNode.path(delegationTokenId))
  }

  /**
   * This registers a ZNodeChangeHandler and attempts to register a watcher with an ExistsRequest, which allows data
   * watcher registrations on paths which might not even exist.
   *
   * @param zNodeChangeHandler
   * @return `true` if the path exists or `false` if it does not
   * @throws KeeperException if an error is returned by ZooKeeper
   */
  def registerZNodeChangeHandlerAndCheckExistence(zNodeChangeHandler: ZNodeChangeHandler): Boolean = {
    zooKeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
    val existsResponse = retryRequestUntilConnected(ExistsRequest(zNodeChangeHandler.path))
    existsResponse.resultCode match {
      case Code.OK => true
      case Code.NONODE => false
      case _ => throw existsResponse.resultException.get
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
   *
   * @param stateChangeHandler
   */
  def registerStateChangeHandler(stateChangeHandler: StateChangeHandler): Unit = {
    zooKeeperClient.registerStateChangeHandler(stateChangeHandler)
  }

  /**
   *
   * @param name
   */
  def unregisterStateChangeHandler(name: String): Unit = {
    zooKeeperClient.unregisterStateChangeHandler(name)
  }

  /**
   * Close the underlying ZooKeeperClient.
   */
  def close(): Unit = {
    removeMetric("ZooKeeperRequestLatencyMs")
    zooKeeperClient.close()
  }

  /**
   * Get the committed offset for a topic partition and group
   * @param group the group we wish to get offset for
   * @param topicPartition the topic partition we wish to get the offset for
   * @return optional long that is Some if there was an offset committed for topic partition, group and None otherwise.
   */
  def getConsumerOffset(group: String, topicPartition: TopicPartition): Option[Long] = {
    val getDataRequest = GetDataRequest(ConsumerOffset.path(group, topicPartition.topic, topicPartition.partition))
    val getDataResponse = retryRequestUntilConnected(getDataRequest)
    getDataResponse.resultCode match {
      case Code.OK => ConsumerOffset.decode(getDataResponse.data)
      case Code.NONODE => None
      case _ => throw getDataResponse.resultException.get
    }
  }

  /**
   * Set the committed offset for a topic partition and group
   * @param group the group whose offset is being set
   * @param topicPartition the topic partition whose offset is being set
   * @param offset the offset value
   */
  def setOrCreateConsumerOffset(group: String, topicPartition: TopicPartition, offset: Long): Unit = {
    val setDataResponse = setConsumerOffset(group, topicPartition, offset)
    if (setDataResponse.resultCode == Code.NONODE) {
      createConsumerOffset(group, topicPartition, offset)
    } else {
      setDataResponse.maybeThrow
    }
  }

  /**
    * Get the cluster id.
    * @return optional cluster id in String.
    */
  def getClusterId: Option[String] = {
    val getDataRequest = GetDataRequest(ClusterIdZNode.path)
    val getDataResponse = retryRequestUntilConnected(getDataRequest)
    getDataResponse.resultCode match {
      case Code.OK => Some(ClusterIdZNode.fromJson(getDataResponse.data))
      case Code.NONODE => None
      case _ => throw getDataResponse.resultException.get
    }
  }

  /**
    * Create the cluster Id. If the cluster id already exists, return the current cluster id.
    * @return  cluster id
    */
  def createOrGetClusterId(proposedClusterId: String): String = {
    try {
      createRecursive(ClusterIdZNode.path, ClusterIdZNode.toJson(proposedClusterId))
      proposedClusterId
    } catch {
      case _: NodeExistsException => getClusterId.getOrElse(
        throw new KafkaException("Failed to get cluster id from Zookeeper. This can happen if /cluster/id is deleted from Zookeeper."))
    }
  }

  /**
    * Generate a broker id by updating the broker sequence id path in ZK and return the version of the path.
    * The version is incremented by one on every update starting from 1.
    * @return sequence number as the broker id
    */
  def generateBrokerSequenceId(): Int = {
    val setDataRequest = SetDataRequest(BrokerSequenceIdZNode.path, Array.empty[Byte], -1)
    val setDataResponse = retryRequestUntilConnected(setDataRequest)
    setDataResponse.resultCode match {
      case Code.OK => setDataResponse.stat.getVersion
      case Code.NONODE =>
        // maker sure the path exists
        createRecursive(BrokerSequenceIdZNode.path, Array.empty[Byte], throwIfPathExists = false)
        generateBrokerSequenceId()
      case _ => throw setDataResponse.resultException.get
    }
  }

  /**
    * Pre-create top level paths in ZK if needed.
    */
  def createTopLevelPaths(): Unit = {
    ZkData.PersistentZkPaths.foreach(makeSurePersistentPathExists(_))
  }

  /**
    * Make sure a persistent path exists in ZK.
    * @param path
    */
  def makeSurePersistentPathExists(path: String): Unit = {
    createRecursive(path, data = null, throwIfPathExists = false)
  }

  private def setConsumerOffset(group: String, topicPartition: TopicPartition, offset: Long): SetDataResponse = {
    val setDataRequest = SetDataRequest(ConsumerOffset.path(group, topicPartition.topic, topicPartition.partition),
      ConsumerOffset.encode(offset), ZkVersion.NoVersion)
    retryRequestUntilConnected(setDataRequest)
  }

  private def createConsumerOffset(group: String, topicPartition: TopicPartition, offset: Long) = {
    val path = ConsumerOffset.path(group, topicPartition.topic, topicPartition.partition)
    createRecursive(path, ConsumerOffset.encode(offset))
  }

  /**
   * Deletes the given zk path recursively
   * @param path
   * @return true if path gets deleted successfully, false if root path doesn't exist
   * @throws KeeperException if there is an error while deleting the znodes
   */
  def deleteRecursive(path: String): Boolean = {
    val getChildrenResponse = retryRequestUntilConnected(GetChildrenRequest(path))
    getChildrenResponse.resultCode match {
      case Code.OK =>
        getChildrenResponse.children.foreach(child => deleteRecursive(s"$path/$child"))
        val deleteResponse = retryRequestUntilConnected(DeleteRequest(path, ZkVersion.NoVersion))
        if (deleteResponse.resultCode != Code.OK && deleteResponse.resultCode != Code.NONODE) {
          throw deleteResponse.resultException.get
        }
        true
      case Code.NONODE => false
      case _ => throw getChildrenResponse.resultException.get
    }
  }

  def pathExists(path: String): Boolean = {
    val existsRequest = ExistsRequest(path)
    val existsResponse = retryRequestUntilConnected(existsRequest)
    existsResponse.resultCode match {
      case Code.OK => true
      case Code.NONODE => false
      case _ => throw existsResponse.resultException.get
    }
  }

  private[zk] def createRecursive(path: String, data: Array[Byte] = null, throwIfPathExists: Boolean = true) = {

    def parentPath(path: String): String = {
      val indexOfLastSlash = path.lastIndexOf("/")
      if (indexOfLastSlash == -1) throw new IllegalArgumentException(s"Invalid path ${path}")
      path.substring(0, indexOfLastSlash)
    }

    def createRecursive0(path: String): Unit = {
      val createRequest = CreateRequest(path, null, acls(path), CreateMode.PERSISTENT)
      var createResponse = retryRequestUntilConnected(createRequest)
      if (createResponse.resultCode == Code.NONODE) {
        createRecursive0(parentPath(path))
        createResponse = retryRequestUntilConnected(createRequest)
        if (createResponse.resultCode != Code.OK && createResponse.resultCode != Code.NODEEXISTS) {
          throw createResponse.resultException.get
        }
      } else if (createResponse.resultCode != Code.OK && createResponse.resultCode != Code.NODEEXISTS) {
        throw createResponse.resultException.get
      }
    }

    val createRequest = CreateRequest(path, data, acls(path), CreateMode.PERSISTENT)
    var createResponse = retryRequestUntilConnected(createRequest)

    if (throwIfPathExists && createResponse.resultCode == Code.NODEEXISTS) {
      createResponse.maybeThrow
    } else if (createResponse.resultCode == Code.NONODE) {
      createRecursive0(parentPath(path))
      createResponse = retryRequestUntilConnected(createRequest)
      if (throwIfPathExists || createResponse.resultCode != Code.NODEEXISTS)
        createResponse.maybeThrow
    } else if (createResponse.resultCode != Code.NODEEXISTS)
      createResponse.maybeThrow

  }

  private def createTopicPartition(partitions: Seq[TopicPartition]): Seq[CreateResponse] = {
    val createRequests = partitions.map { partition =>
      val path = TopicPartitionZNode.path(partition)
      CreateRequest(path, null, acls(path), CreateMode.PERSISTENT, Some(partition))
    }
    retryRequestsUntilConnected(createRequests)
  }

  private def createTopicPartitions(topics: Seq[String]): Seq[CreateResponse] = {
    val createRequests = topics.map { topic =>
      val path = TopicPartitionsZNode.path(topic)
      CreateRequest(path, null, acls(path), CreateMode.PERSISTENT, Some(topic))
    }
    retryRequestsUntilConnected(createRequests)
  }

  private def getTopicConfigs(topics: Seq[String]): Seq[GetDataResponse] = {
    val getDataRequests = topics.map { topic =>
      GetDataRequest(ConfigEntityZNode.path(ConfigType.Topic, topic), ctx = Some(topic))
    }
    retryRequestsUntilConnected(getDataRequests)
  }

  private def acls(path: String): Seq[ACL] = ZkData.defaultAcls(isSecure, path)

  private def retryRequestUntilConnected[Req <: AsyncRequest](request: Req): Req#Response = {
    retryRequestsUntilConnected(Seq(request)).head
  }

  private def retryRequestsUntilConnected[Req <: AsyncRequest](requests: Seq[Req]): Seq[Req#Response] = {
    val remainingRequests = ArrayBuffer(requests: _*)
    val responses = new ArrayBuffer[Req#Response]
    while (remainingRequests.nonEmpty) {
      val batchResponses = zooKeeperClient.handleRequests(remainingRequests)

      batchResponses.foreach(response => latencyMetric.update(response.metadata.responseTimeMs))

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
    if (code != Code.OK)
      throw KeeperException.create(code)
  }

  private class CheckedEphemeral(path: String, data: Array[Byte]) extends Logging {
    def create(): Code = {
      val createRequest = CreateRequest(path, data, acls(path), CreateMode.EPHEMERAL)
      val createResponse = retryRequestUntilConnected(createRequest)
      createResponse.resultCode match {
        case code@ Code.OK => code
        case Code.NODEEXISTS => getAfterNodeExists()
        case code =>
          error(s"Error while creating ephemeral at $path with return code: $code")
          code
      }
    }

    private def getAfterNodeExists(): Code = {
      val getDataRequest = GetDataRequest(path)
      val getDataResponse = retryRequestUntilConnected(getDataRequest)
      getDataResponse.resultCode match {
        case Code.OK if getDataResponse.stat.getEphemeralOwner != zooKeeperClient.sessionId =>
          error(s"Error while creating ephemeral at $path, node already exists and owner " +
            s"'${getDataResponse.stat.getEphemeralOwner}' does not match current session '${zooKeeperClient.sessionId}'")
          Code.NODEEXISTS
        case code@ Code.OK => code
        case Code.NONODE =>
          info(s"The ephemeral node at $path went away while reading it, attempting create() again")
          create()
        case code =>
          error(s"Error while creating ephemeral at $path as it already exists and error getting the node data due to $code")
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
  case class UpdateLeaderAndIsrResult(successfulPartitions: Map[TopicPartition, LeaderAndIsr],
                                      partitionsToRetry: Seq[TopicPartition],
                                      failedPartitions: Map[TopicPartition, Exception])

  /**
   * Create an instance of this class with the provided parameters.
   *
   * The metric group and type are preserved by default for compatibility with previous versions.
   */
  def apply(connectString: String,
            isSecure: Boolean,
            sessionTimeoutMs: Int,
            connectionTimeoutMs: Int,
            maxInFlightRequests: Int,
            time: Time,
            metricGroup: String = "kafka.server",
            metricType: String = "SessionExpireListener") = {
    val zooKeeperClient = new ZooKeeperClient(connectString, sessionTimeoutMs, connectionTimeoutMs, maxInFlightRequests,
      time, metricGroup, metricType)
    new KafkaZkClient(zooKeeperClient, isSecure, time)
  }
}
