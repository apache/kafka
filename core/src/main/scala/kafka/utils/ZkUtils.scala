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

package kafka.utils

import java.nio.charset.StandardCharsets
import java.util.concurrent.CountDownLatch

import kafka.admin._
import kafka.api.{ApiVersion, KAFKA_0_10_0_IV1, LeaderAndIsr}
import kafka.cluster._
import kafka.common.{KafkaException, NoEpochForPartitionException, TopicAndPartition}
import kafka.consumer.{ConsumerThreadId, TopicCount}
import kafka.controller.{LeaderIsrAndControllerEpoch, ReassignedPartitionsContext}
import kafka.zk.{BrokerIdZNode, ReassignPartitionsZNode, ZkData}
import org.I0Itec.zkclient.exception.{ZkBadVersionException, ZkException, ZkMarshallingError, ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener, IZkStateListener, ZkClient, ZkConnection}
import org.apache.kafka.common.config.ConfigException
import org.apache.zookeeper.AsyncCallback.{DataCallback, StringCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.data.{ACL, Stat}
import org.apache.zookeeper.{CreateMode, KeeperException, ZooDefs, ZooKeeper}

import scala.collection._
import scala.collection.JavaConverters._
import org.apache.kafka.common.TopicPartition

object ZkUtils {

  private val UseDefaultAcls = new java.util.ArrayList[ACL]

  // Important: it is necessary to add any new top level Zookeeper path here
  val AdminPath = "/admin"
  val BrokersPath = "/brokers"
  val ClusterPath = "/cluster"
  val ConfigPath = "/config"
  val ControllerPath = "/controller"
  val ControllerEpochPath = "/controller_epoch"
  val IsrChangeNotificationPath = "/isr_change_notification"
  val LogDirEventNotificationPath = "/log_dir_event_notification"
  val KafkaAclPath = "/kafka-acl"
  val KafkaAclChangesPath = "/kafka-acl-changes"

  val ConsumersPath = "/consumers"
  val ClusterIdPath = s"$ClusterPath/id"
  val BrokerIdsPath = s"$BrokersPath/ids"
  val BrokerTopicsPath = s"$BrokersPath/topics"
  val ReassignPartitionsPath = s"$AdminPath/reassign_partitions"
  val DeleteTopicsPath = s"$AdminPath/delete_topics"
  val PreferredReplicaLeaderElectionPath = s"$AdminPath/preferred_replica_election"
  val BrokerSequenceIdPath = s"$BrokersPath/seqid"
  val ConfigChangesPath = s"$ConfigPath/changes"
  val ConfigUsersPath = s"$ConfigPath/users"
  val ConfigBrokersPath = s"$ConfigPath/brokers"
  val ProducerIdBlockPath = "/latest_producer_id_block"

  val SecureZkRootPaths = ZkData.SecureRootPaths

  val SensitiveZkRootPaths = ZkData.SensitiveRootPaths

  def apply(zkUrl: String, sessionTimeout: Int, connectionTimeout: Int, isZkSecurityEnabled: Boolean): ZkUtils = {
    val (zkClient, zkConnection) = createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout)
    new ZkUtils(zkClient, zkConnection, isZkSecurityEnabled)
  }

  /*
   * Used in tests
   */
  def apply(zkClient: ZkClient, isZkSecurityEnabled: Boolean): ZkUtils = {
    new ZkUtils(zkClient, null, isZkSecurityEnabled)
  }

  def createZkClient(zkUrl: String, sessionTimeout: Int, connectionTimeout: Int): ZkClient = {
    val zkClient = new ZkClient(zkUrl, sessionTimeout, connectionTimeout, ZKStringSerializer)
    zkClient
  }

  def createZkClientAndConnection(zkUrl: String, sessionTimeout: Int, connectionTimeout: Int): (ZkClient, ZkConnection) = {
    val zkConnection = new ZkConnection(zkUrl, sessionTimeout)
    val zkClient = new ZkClient(zkConnection, connectionTimeout, ZKStringSerializer)
    (zkClient, zkConnection)
  }

  @deprecated("This is deprecated, use defaultAcls(isSecure, path) which doesn't make sensitive data world readable", since = "0.10.2.1")
  def DefaultAcls(isSecure: Boolean): java.util.List[ACL] = defaultAcls(isSecure, "")

  def sensitivePath(path: String): Boolean = ZkData.sensitivePath(path)

  def defaultAcls(isSecure: Boolean, path: String): java.util.List[ACL] = ZkData.defaultAcls(isSecure, path).asJava

  def maybeDeletePath(zkUrl: String, dir: String) {
    try {
      val zk = createZkClient(zkUrl, 30*1000, 30*1000)
      zk.deleteRecursive(dir)
      zk.close()
    } catch {
      case _: Throwable => // swallow
    }
  }

  /*
   * Get calls that only depend on static paths
   */
  def getTopicPath(topic: String): String = {
    ZkUtils.BrokerTopicsPath + "/" + topic
  }

  def getTopicPartitionsPath(topic: String): String = {
    getTopicPath(topic) + "/partitions"
  }

  def getTopicPartitionPath(topic: String, partitionId: Int): String =
    getTopicPartitionsPath(topic) + "/" + partitionId

  def getTopicPartitionLeaderAndIsrPath(topic: String, partitionId: Int): String =
    getTopicPartitionPath(topic, partitionId) + "/" + "state"

  def getEntityConfigRootPath(entityType: String): String =
    ZkUtils.ConfigPath + "/" + entityType

  def getEntityConfigPath(entityType: String, entity: String): String =
    getEntityConfigRootPath(entityType) + "/" + entity

  def getEntityConfigPath(entityPath: String): String =
    ZkUtils.ConfigPath + "/" + entityPath

  def getDeleteTopicPath(topic: String): String =
    DeleteTopicsPath + "/" + topic

  def parsePartitionReassignmentData(jsonData: String): Map[TopicAndPartition, Seq[Int]] = {
    val utf8Bytes = jsonData.getBytes(StandardCharsets.UTF_8)
    val assignments = ReassignPartitionsZNode.decode(utf8Bytes) match {
      case Left(e) => throw e
      case Right(result) => result
    }

    assignments.map { case (tp, p) => (new TopicAndPartition(tp), p) }
  }

  def controllerZkData(brokerId: Int, timestamp: Long): String = {
    Json.legacyEncodeAsString(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp.toString))
  }

  def preferredReplicaLeaderElectionZkData(partitions: scala.collection.Set[TopicAndPartition]): String = {
    Json.legacyEncodeAsString(Map("version" -> 1, "partitions" -> partitions.map(tp => Map("topic" -> tp.topic, "partition" -> tp.partition))))
  }

  def formatAsReassignmentJson(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]): String = {
    Json.legacyEncodeAsString(Map(
      "version" -> 1,
      "partitions" -> partitionsToBeReassigned.map { case (TopicAndPartition(topic, partition), replicas) =>
        Map(
          "topic" -> topic,
          "partition" -> partition,
          "replicas" -> replicas
        )
      }
    ))
  }

  def getReassignmentJson(partitionsToBeReassigned: Map[TopicPartition, Seq[Int]]): String = {
    Json.encodeAsString(Map(
      "version" -> 1,
      "partitions" -> partitionsToBeReassigned.map { case (tp, replicas) =>
        Map(
          "topic" -> tp.topic,
          "partition" -> tp.partition,
          "replicas" -> replicas
        )
      }
    ))
  }
}

/**
 * Legacy class for interacting with ZooKeeper. Whenever possible, ``KafkaZkClient`` should be used instead.
 */
class ZkUtils(val zkClient: ZkClient,
              val zkConnection: ZkConnection,
              val isSecure: Boolean) extends Logging {
  import ZkUtils._

  // These are persistent ZK paths that should exist on kafka broker startup.
  val persistentZkPaths = ZkData.PersistentZkPaths

  // Visible for testing
  val zkPath = new ZkPath(zkClient)

  @deprecated("This is deprecated, use defaultAcls(path) which doesn't make sensitive data world readable", since = "0.10.2.1")
  val DefaultAcls: java.util.List[ACL] = ZkUtils.defaultAcls(isSecure, "")

  def defaultAcls(path: String): java.util.List[ACL] = ZkUtils.defaultAcls(isSecure, path)

  def getController(): Int = {
    readDataMaybeNull(ControllerPath)._1 match {
      case Some(controller) => parseControllerId(controller)
      case None => throw new KafkaException("Controller doesn't exist")
    }
  }

  def parseControllerId(controllerInfoString: String): Int = {
    try {
      Json.parseFull(controllerInfoString) match {
        case Some(js) => js.asJsonObject("brokerid").to[Int]
        case None => throw new KafkaException("Failed to parse the controller info json [%s].".format(controllerInfoString))
      }
    } catch {
      case _: Throwable =>
        // It may be due to an incompatible controller register version
        warn("Failed to parse the controller info as json. "
          + "Probably this controller is still using the old format [%s] to store the broker id in zookeeper".format(controllerInfoString))
        try controllerInfoString.toInt
        catch {
          case t: Throwable => throw new KafkaException("Failed to parse the controller info: " + controllerInfoString + ". This is neither the new or the old format.", t)
        }
    }
  }

  /* Represents a cluster identifier. Stored in Zookeeper in JSON format: {"version" -> "1", "id" -> id } */
  object ClusterId {

    def toJson(id: String) = {
      Json.legacyEncodeAsString(Map("version" -> "1", "id" -> id))
    }

    def fromJson(clusterIdJson: String): String = {
      Json.parseFull(clusterIdJson).map(_.asJsonObject("id").to[String]).getOrElse {
        throw new KafkaException(s"Failed to parse the cluster id json $clusterIdJson")
      }
    }
  }

  def getClusterId: Option[String] =
    readDataMaybeNull(ClusterIdPath)._1.map(ClusterId.fromJson)

  def createOrGetClusterId(proposedClusterId: String): String = {
    try {
      createPersistentPath(ClusterIdPath, ClusterId.toJson(proposedClusterId))
      proposedClusterId
    } catch {
      case _: ZkNodeExistsException =>
        getClusterId.getOrElse(throw new KafkaException("Failed to get cluster id from Zookeeper. This can only happen if /cluster/id is deleted from Zookeeper."))
    }
  }

  def getSortedBrokerList(): Seq[Int] =
    getChildren(BrokerIdsPath).map(_.toInt).sorted

  def getAllBrokersInCluster(): Seq[Broker] = {
    val brokerIds = getChildrenParentMayNotExist(BrokerIdsPath).sorted
    brokerIds.map(_.toInt).map(getBrokerInfo(_)).filter(_.isDefined).map(_.get)
  }

  def getLeaderAndIsrForPartition(topic: String, partition: Int): Option[LeaderAndIsr] = {
    val leaderAndIsrPath = getTopicPartitionLeaderAndIsrPath(topic, partition)
    val (leaderAndIsrOpt, stat) = readDataMaybeNull(leaderAndIsrPath)
    debug(s"Read leaderISR $leaderAndIsrOpt for $topic-$partition")
    leaderAndIsrOpt.flatMap(leaderAndIsrStr => parseLeaderAndIsr(leaderAndIsrStr, leaderAndIsrPath, stat).map(_.leaderAndIsr))
  }

  private def parseLeaderAndIsr(leaderAndIsrStr: String, path: String, stat: Stat): Option[LeaderIsrAndControllerEpoch] = {
    Json.parseFull(leaderAndIsrStr).flatMap { js =>
      val leaderIsrAndEpochInfo = js.asJsonObject
      val leader = leaderIsrAndEpochInfo("leader").to[Int]
      val epoch = leaderIsrAndEpochInfo("leader_epoch").to[Int]
      val isr = leaderIsrAndEpochInfo("isr").to[List[Int]]
      val controllerEpoch = leaderIsrAndEpochInfo("controller_epoch").to[Int]
      val zkPathVersion = stat.getVersion
      trace(s"Leader $leader, Epoch $epoch, Isr $isr, Zk path version $zkPathVersion for leaderAndIsrPath $path")
      Some(LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch))}
  }

  def setupCommonPaths() {
    for(path <- persistentZkPaths)
      makeSurePersistentPathExists(path)
  }

  def getLeaderForPartition(topic: String, partition: Int): Option[Int] = {
    readDataMaybeNull(getTopicPartitionLeaderAndIsrPath(topic, partition))._1.flatMap { leaderAndIsr =>
      Json.parseFull(leaderAndIsr).map(_.asJsonObject("leader").to[Int])
    }
  }

  /**
   * This API should read the epoch in the ISR path. It is sufficient to read the epoch in the ISR path, since if the
   * leader fails after updating epoch in the leader path and before updating epoch in the ISR path, effectively some
   * other broker will retry becoming leader with the same new epoch value.
   */
  def getEpochForPartition(topic: String, partition: Int): Int = {
    readDataMaybeNull(getTopicPartitionLeaderAndIsrPath(topic, partition))._1 match {
      case Some(leaderAndIsr) =>
        Json.parseFull(leaderAndIsr) match {
          case None => throw new NoEpochForPartitionException("No epoch, leaderAndISR data for partition [%s,%d] is invalid".format(topic, partition))
          case Some(js) => js.asJsonObject("leader_epoch").to[Int]
        }
      case None => throw new NoEpochForPartitionException("No epoch, ISR path for partition [%s,%d] is empty"
        .format(topic, partition))
    }
  }

  /** returns a sequence id generated by updating BrokerSequenceIdPath in Zk.
    * users can provide brokerId in the config , inorder to avoid conflicts between zk generated
    * seqId and config.brokerId we increment zk seqId by KafkaConfig.MaxReservedBrokerId.
    */
  def getBrokerSequenceId(MaxReservedBrokerId: Int): Int = {
    getSequenceId(BrokerSequenceIdPath) + MaxReservedBrokerId
  }

  /**
   * Gets the in-sync replicas (ISR) for a specific topic and partition
   */
  def getInSyncReplicasForPartition(topic: String, partition: Int): Seq[Int] = {
    val leaderAndIsrOpt = readDataMaybeNull(getTopicPartitionLeaderAndIsrPath(topic, partition))._1
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        Json.parseFull(leaderAndIsr) match {
          case Some(js) => js.asJsonObject("isr").to[Seq[Int]]
          case None => Seq.empty[Int]
        }
      case None => Seq.empty[Int]
    }
  }

  /**
   * Gets the assigned replicas (AR) for a specific topic and partition
   */
  def getReplicasForPartition(topic: String, partition: Int): Seq[Int] = {
    val seqOpt = for {
      jsonPartitionMap <- readDataMaybeNull(getTopicPath(topic))._1
      js <- Json.parseFull(jsonPartitionMap)
      replicaMap <- js.asJsonObject.get("partitions")
      seq <- replicaMap.asJsonObject.get(partition.toString)
    } yield seq.to[Seq[Int]]
    seqOpt.getOrElse(Seq.empty)
  }

  /**
   * Register brokers with v4 json format (which includes multiple endpoints and rack) if
   * the apiVersion is 0.10.0.X or above. Register the broker with v2 json format otherwise.
   * Due to KAFKA-3100, 0.9.0.0 broker and old clients will break if JSON version is above 2.
   * We include v2 to make it possible for the broker to migrate from 0.9.0.0 to 0.10.0.X or above without having to
   * upgrade to 0.9.0.1 first (clients have to be upgraded to 0.9.0.1 in any case).
   *
   * This format also includes default endpoints for compatibility with older clients.
   *
   * @param id broker ID
   * @param host broker host name
   * @param port broker port
   * @param advertisedEndpoints broker end points
   * @param jmxPort jmx port
   * @param rack broker rack
   * @param apiVersion Kafka version the broker is running as
   */
  def registerBrokerInZk(id: Int,
                         host: String,
                         port: Int,
                         advertisedEndpoints: Seq[EndPoint],
                         jmxPort: Int,
                         rack: Option[String],
                         apiVersion: ApiVersion) {
    val brokerIdPath = BrokerIdsPath + "/" + id
    // see method documentation for reason why we do this
    val version = if (apiVersion >= KAFKA_0_10_0_IV1) 4 else 2
    val json = new String(BrokerIdZNode.encode(version, host, port, advertisedEndpoints, jmxPort, rack),
      StandardCharsets.UTF_8)
    registerBrokerInZk(brokerIdPath, json)

    info("Registered broker %d at path %s with addresses: %s".format(id, brokerIdPath, advertisedEndpoints.mkString(",")))
  }

  private def registerBrokerInZk(brokerIdPath: String, brokerInfo: String) {
    try {
      val zkCheckedEphemeral = new ZKCheckedEphemeral(brokerIdPath,
                                                      brokerInfo,
                                                      zkConnection.getZookeeper,
                                                      isSecure)
      zkCheckedEphemeral.create()
    } catch {
      case _: ZkNodeExistsException =>
        throw new RuntimeException("A broker is already registered on the path " + brokerIdPath
                + ". This probably " + "indicates that you either have configured a brokerid that is already in use, or "
                + "else you have shutdown this broker and restarted it faster than the zookeeper "
                + "timeout so it appears to be re-registering.")
    }
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  def getConsumerPartitionOwnerPath(group: String, topic: String, partition: Int): String = {
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    topicDirs.consumerOwnerDir + "/" + partition
  }

  def leaderAndIsrZkData(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int): String = {
    Json.legacyEncodeAsString(Map("version" -> 1, "leader" -> leaderAndIsr.leader, "leader_epoch" -> leaderAndIsr.leaderEpoch,
                    "controller_epoch" -> controllerEpoch, "isr" -> leaderAndIsr.isr))
  }

  /**
   * Get JSON partition to replica map from zookeeper.
   */
  def replicaAssignmentZkData(map: Map[String, Seq[Int]]): String = {
    Json.legacyEncodeAsString(Map("version" -> 1, "partitions" -> map))
  }

  /**
   *  make sure a persistent path exists in ZK. Create the path if not exist.
   */
  def makeSurePersistentPathExists(path: String, acls: java.util.List[ACL] = UseDefaultAcls) {
    //Consumer path is kept open as different consumers will write under this node.
    val acl = if (path == null || path.isEmpty || path.equals(ConsumersPath)) {
      ZooDefs.Ids.OPEN_ACL_UNSAFE
    } else if (acls eq UseDefaultAcls) {
      ZkUtils.defaultAcls(isSecure, path)
    } else {
      acls
    }

    if (!zkClient.exists(path))
      zkPath.createPersistent(path, createParents = true, acl) //won't throw NoNodeException or NodeExistsException
  }

  /**
   *  create the parent path
   */
  private def createParentPath(path: String, acls: java.util.List[ACL] = UseDefaultAcls): Unit = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0) {
      zkPath.createPersistent(parentDir, createParents = true, acl)
    }
  }

  /**
   * Create an ephemeral node with the given path and data. Create parents if necessary.
   */
  private def createEphemeralPath(path: String, data: String, acls: java.util.List[ACL]): Unit = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls
    try {
      zkPath.createEphemeral(path, data, acl)
    } catch {
      case _: ZkNoNodeException =>
        createParentPath(path)
        zkPath.createEphemeral(path, data, acl)
    }
  }

  /**
   * Create an ephemeral node with the given path and data.
   * Throw NodeExistException if node already exists.
   */
  def createEphemeralPathExpectConflict(path: String, data: String, acls: java.util.List[ACL] = UseDefaultAcls): Unit = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls
    try {
      createEphemeralPath(path, data, acl)
    } catch {
      case e: ZkNodeExistsException =>
        // this can happen when there is connection loss; make sure the data is what we intend to write
        var storedData: String = null
        try {
          storedData = readData(path)._1
        } catch {
          case _: ZkNoNodeException => // the node disappeared; treat as if node existed and let caller handles this
        }
        if (storedData == null || storedData != data) {
          info("conflict in " + path + " data: " + data + " stored data: " + storedData)
          throw e
        } else {
          // otherwise, the creation succeeded, return normally
          info(path + " exists with value " + data + " during connection loss; this is ok")
        }
    }
  }

  /**
   * Create a persistent node with the given path and data. Create parents if necessary.
   */
  def createPersistentPath(path: String, data: String = "", acls: java.util.List[ACL] = UseDefaultAcls): Unit = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls
    try {
      zkPath.createPersistent(path, data, acl)
    } catch {
      case _: ZkNoNodeException =>
        createParentPath(path)
        zkPath.createPersistent(path, data, acl)
    }
  }

  def createSequentialPersistentPath(path: String, data: String = "", acls: java.util.List[ACL] = UseDefaultAcls): String = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls
    zkPath.createPersistentSequential(path, data, acl)
  }

  /**
   * Update the value of a persistent node with the given path and data.
   * create parent directory if necessary. Never throw NodeExistException.
   * Return the updated path zkVersion
   */
  def updatePersistentPath(path: String, data: String, acls: java.util.List[ACL] = UseDefaultAcls) = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls
    try {
      zkClient.writeData(path, data)
    } catch {
      case _: ZkNoNodeException =>
        createParentPath(path)
        try {
          zkPath.createPersistent(path, data, acl)
        } catch {
          case _: ZkNodeExistsException =>
            zkClient.writeData(path, data)
        }
    }
  }

  /**
   * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the path doesn't
   * exist, the current version is not the expected version, etc.) return (false, -1)
   *
   * When there is a ConnectionLossException during the conditional update, zkClient will retry the update and may fail
   * since the previous update may have succeeded (but the stored zkVersion no longer matches the expected one).
   * In this case, we will run the optionalChecker to further check if the previous write did indeed succeeded.
   */
  def conditionalUpdatePersistentPath(path: String, data: String, expectVersion: Int,
    optionalChecker:Option[(ZkUtils, String, String) => (Boolean,Int)] = None): (Boolean, Int) = {
    try {
      val stat = zkClient.writeDataReturnStat(path, data, expectVersion)
      debug("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d"
        .format(path, data, expectVersion, stat.getVersion))
      (true, stat.getVersion)
    } catch {
      case e1: ZkBadVersionException =>
        optionalChecker match {
          case Some(checker) => checker(this, path, data)
          case _ =>
            debug("Checker method is not passed skipping zkData match")
            debug("Conditional update of path %s with data %s and expected version %d failed due to %s"
              .format(path, data,expectVersion, e1.getMessage))
            (false, -1)
        }
      case e2: Exception =>
        debug("Conditional update of path %s with data %s and expected version %d failed due to %s".format(path, data,
          expectVersion, e2.getMessage))
        (false, -1)
    }
  }

  /**
   * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the current
   * version is not the expected version, etc.) return (false, -1). If path doesn't exist, throws ZkNoNodeException
   */
  def conditionalUpdatePersistentPathIfExists(path: String, data: String, expectVersion: Int): (Boolean, Int) = {
    try {
      val stat = zkClient.writeDataReturnStat(path, data, expectVersion)
      debug("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d"
        .format(path, data, expectVersion, stat.getVersion))
      (true, stat.getVersion)
    } catch {
      case nne: ZkNoNodeException => throw nne
      case e: Exception =>
        error("Conditional update of path %s with data %s and expected version %d failed due to %s".format(path, data,
          expectVersion, e.getMessage))
        (false, -1)
    }
  }

  /**
   * Update the value of a ephemeral node with the given path and data.
   * create parent directory if necessary. Never throw NodeExistException.
   */
  def updateEphemeralPath(path: String, data: String, acls: java.util.List[ACL] = UseDefaultAcls): Unit = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls
    try {
      zkClient.writeData(path, data)
    } catch {
      case _: ZkNoNodeException =>
        createParentPath(path)
        zkPath.createEphemeral(path, data, acl)
    }
  }

  def deletePath(path: String): Boolean = {
    zkClient.delete(path)
  }

  /**
    * Conditional delete the persistent path data, return true if it succeeds,
    * false otherwise (the current version is not the expected version)
    */
   def conditionalDeletePath(path: String, expectedVersion: Int): Boolean = {
    try {
      zkClient.delete(path, expectedVersion)
      true
    } catch {
      case _: ZkBadVersionException => false
    }
  }

  def deletePathRecursive(path: String) {
    zkClient.deleteRecursive(path)
  }

  def subscribeDataChanges(path: String, listener: IZkDataListener): Unit =
    zkClient.subscribeDataChanges(path, listener)

  def unsubscribeDataChanges(path: String, dataListener: IZkDataListener): Unit =
    zkClient.unsubscribeDataChanges(path, dataListener)

  def subscribeStateChanges(listener: IZkStateListener): Unit =
    zkClient.subscribeStateChanges(listener)

  def subscribeChildChanges(path: String, listener: IZkChildListener): Option[Seq[String]] =
    Option(zkClient.subscribeChildChanges(path, listener)).map(_.asScala)

  def unsubscribeChildChanges(path: String, childListener: IZkChildListener): Unit =
    zkClient.unsubscribeChildChanges(path, childListener)

  def unsubscribeAll(): Unit =
    zkClient.unsubscribeAll()

  def readData(path: String): (String, Stat) = {
    val stat: Stat = new Stat()
    val dataStr: String = zkClient.readData[String](path, stat)
    (dataStr, stat)
  }

  def readDataMaybeNull(path: String): (Option[String], Stat) = {
    val stat = new Stat()
    val dataAndStat = try {
                        val dataStr = zkClient.readData[String](path, stat)
                        (Some(dataStr), stat)
                      } catch {
                        case _: ZkNoNodeException =>
                          (None, stat)
                      }
    dataAndStat
  }

  def readDataAndVersionMaybeNull(path: String): (Option[String], Int) = {
    val stat = new Stat()
    try {
      val data = zkClient.readData[String](path, stat)
      (Option(data), stat.getVersion)
    } catch {
      case _: ZkNoNodeException => (None, stat.getVersion)
    }
  }

  def getChildren(path: String): Seq[String] = zkClient.getChildren(path).asScala

  def getChildrenParentMayNotExist(path: String): Seq[String] = {
    try {
      zkClient.getChildren(path).asScala
    } catch {
      case _: ZkNoNodeException => Nil
    }
  }

  /**
   * Check if the given path exists
   */
  def pathExists(path: String): Boolean = {
    zkClient.exists(path)
  }

  def isTopicMarkedForDeletion(topic: String): Boolean = {
    pathExists(getDeleteTopicPath(topic))
  }

  def getCluster(): Cluster = {
    val cluster = new Cluster
    val nodes = getChildrenParentMayNotExist(BrokerIdsPath)
    for (node <- nodes) {
      val brokerZKString = readData(BrokerIdsPath + "/" + node)._1
      cluster.add(parseBrokerJson(node.toInt, brokerZKString))
    }
    cluster
  }

  private def parseBrokerJson(id: Int, jsonString: String): Broker = {
    BrokerIdZNode.decode(id, jsonString.getBytes(StandardCharsets.UTF_8)).broker
  }

  def getPartitionLeaderAndIsrForTopics(topicAndPartitions: Set[TopicAndPartition]): mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = {
    val ret = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
    for(topicAndPartition <- topicAndPartitions) {
      getLeaderIsrAndEpochForPartition(topicAndPartition.topic, topicAndPartition.partition).foreach { leaderIsrAndControllerEpoch =>
        ret.put(topicAndPartition, leaderIsrAndControllerEpoch)
      }
    }
    ret
  }

  private[utils] def getLeaderIsrAndEpochForPartition(topic: String, partition: Int): Option[LeaderIsrAndControllerEpoch] = {
    val leaderAndIsrPath = getTopicPartitionLeaderAndIsrPath(topic, partition)
    val (leaderAndIsrOpt, stat) = readDataMaybeNull(leaderAndIsrPath)
    debug(s"Read leaderISR $leaderAndIsrOpt for $topic-$partition")
    leaderAndIsrOpt.flatMap(leaderAndIsrStr => parseLeaderAndIsr(leaderAndIsrStr, leaderAndIsrPath, stat))
  }

  def getReplicaAssignmentForTopics(topics: Seq[String]): mutable.Map[TopicAndPartition, Seq[Int]] = {
    val ret = new mutable.HashMap[TopicAndPartition, Seq[Int]]
    topics.foreach { topic =>
      readDataMaybeNull(getTopicPath(topic))._1.foreach { jsonPartitionMap =>
        Json.parseFull(jsonPartitionMap).foreach { js =>
          js.asJsonObject.get("partitions").foreach { partitionsJs =>
            partitionsJs.asJsonObject.iterator.foreach { case (partition, replicas) =>
              ret.put(TopicAndPartition(topic, partition.toInt), replicas.to[Seq[Int]])
              debug("Replicas assigned to topic [%s], partition [%s] are [%s]".format(topic, partition, replicas))
            }
          }
        }
      }
    }
    ret
  }

  def getPartitionAssignmentForTopics(topics: Seq[String]): mutable.Map[String, collection.Map[Int, Seq[Int]]] = {
    val ret = new mutable.HashMap[String, Map[Int, Seq[Int]]]()
    topics.foreach { topic =>
      val partitionMapOpt = for {
        jsonPartitionMap <- readDataMaybeNull(getTopicPath(topic))._1
        js <- Json.parseFull(jsonPartitionMap)
        replicaMap <- js.asJsonObject.get("partitions")
      } yield replicaMap.asJsonObject.iterator.map { case (k, v) => (k.toInt, v.to[Seq[Int]]) }.toMap
      val partitionMap = partitionMapOpt.getOrElse(Map.empty)
      debug("Partition map for /brokers/topics/%s is %s".format(topic, partitionMap))
      ret += (topic -> partitionMap)
    }
    ret
  }

  def getPartitionsForTopics(topics: Seq[String]): mutable.Map[String, Seq[Int]] = {
    getPartitionAssignmentForTopics(topics).map { topicAndPartitionMap =>
      val topic = topicAndPartitionMap._1
      val partitionMap = topicAndPartitionMap._2
      debug("partition assignment of /brokers/topics/%s is %s".format(topic, partitionMap))
      topic -> partitionMap.keys.toSeq.sortWith((s, t) => s < t)
    }
  }

  def getTopicPartitionCount(topic: String): Option[Int] = {
    val topicData = getPartitionAssignmentForTopics(Seq(topic))
    if (topicData(topic).nonEmpty)
      Some(topicData(topic).size)
    else
      None
  }

  def getPartitionsBeingReassigned(): Map[TopicAndPartition, ReassignedPartitionsContext] = {
    // read the partitions and their new replica list
    val jsonPartitionMapOpt = readDataMaybeNull(ReassignPartitionsPath)._1
    jsonPartitionMapOpt match {
      case Some(jsonPartitionMap) =>
        val reassignedPartitions = parsePartitionReassignmentData(jsonPartitionMap)
        reassignedPartitions.map { case (tp, newReplicas) =>
          tp -> new ReassignedPartitionsContext(newReplicas, null)
        }
      case None => Map.empty[TopicAndPartition, ReassignedPartitionsContext]
    }
  }

  def updatePartitionReassignmentData(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]) {
    val zkPath = ZkUtils.ReassignPartitionsPath
    partitionsToBeReassigned.size match {
      case 0 => // need to delete the /admin/reassign_partitions path
        deletePath(zkPath)
        info("No more partitions need to be reassigned. Deleting zk path %s".format(zkPath))
      case _ =>
        val jsonData = formatAsReassignmentJson(partitionsToBeReassigned)
        try {
          updatePersistentPath(zkPath, jsonData)
          debug("Updated partition reassignment path with %s".format(jsonData))
        } catch {
          case _: ZkNoNodeException =>
            createPersistentPath(zkPath, jsonData)
            debug("Created path %s with %s for partition reassignment".format(zkPath, jsonData))
          case e2: Throwable => throw new AdminOperationException(e2.toString)
        }
    }
  }

  def getPartitionsUndergoingPreferredReplicaElection(): Set[TopicAndPartition] = {
    // read the partitions and their new replica list
    val jsonPartitionListOpt = readDataMaybeNull(PreferredReplicaLeaderElectionPath)._1
    jsonPartitionListOpt match {
      case Some(jsonPartitionList) => PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(jsonPartitionList).map(tp => new TopicAndPartition(tp))
      case None => Set.empty[TopicAndPartition]
    }
  }

  def deletePartition(brokerId: Int, topic: String) {
    val brokerIdPath = BrokerIdsPath + "/" + brokerId
    zkClient.delete(brokerIdPath)
    val brokerPartTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic + "/" + brokerId
    zkClient.delete(brokerPartTopicPath)
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  def getConsumersInGroup(group: String): Seq[String] = {
    val dirs = new ZKGroupDirs(group)
    getChildren(dirs.consumerRegistryDir)
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  def getConsumersPerTopic(group: String, excludeInternalTopics: Boolean): mutable.Map[String, List[ConsumerThreadId]] = {
    val dirs = new ZKGroupDirs(group)
    val consumers = getChildrenParentMayNotExist(dirs.consumerRegistryDir)
    val consumersPerTopicMap = new mutable.HashMap[String, List[ConsumerThreadId]]
    for (consumer <- consumers) {
      val topicCount = TopicCount.constructTopicCount(group, consumer, this, excludeInternalTopics)
      for ((topic, consumerThreadIdSet) <- topicCount.getConsumerThreadIdsPerTopic) {
        for (consumerThreadId <- consumerThreadIdSet)
          consumersPerTopicMap.get(topic) match {
            case Some(curConsumers) => consumersPerTopicMap.put(topic, consumerThreadId :: curConsumers)
            case _ => consumersPerTopicMap.put(topic, List(consumerThreadId))
          }
      }
    }
    for ((topic, consumerList) <- consumersPerTopicMap)
      consumersPerTopicMap.put(topic, consumerList.sortWith((s, t) => s < t))
    consumersPerTopicMap
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  def getTopicsPerMemberId(group: String, excludeInternalTopics: Boolean = true): Map[String, List[String]] = {
    val dirs = new ZKGroupDirs(group)
    val memberIds = getChildrenParentMayNotExist(dirs.consumerRegistryDir)
    memberIds.map { memberId =>
      val topicCount = TopicCount.constructTopicCount(group, memberId, this, excludeInternalTopics)
      memberId -> topicCount.getTopicCountMap.keys.toList
    }.toMap
  }

  /**
   * This API takes in a broker id, queries zookeeper for the broker metadata and returns the metadata for that broker
   * or throws an exception if the broker dies before the query to zookeeper finishes
   *
   * @param brokerId The broker id
   * @return An optional Broker object encapsulating the broker metadata
   */
  def getBrokerInfo(brokerId: Int): Option[Broker] = {
    readDataMaybeNull(BrokerIdsPath + "/" + brokerId)._1 match {
      case Some(brokerInfo) => Some(parseBrokerJson(brokerId, brokerInfo))
      case None => None
    }
  }

  /**
    * This API produces a sequence number by creating / updating given path in zookeeper
    * It uses the stat returned by the zookeeper and return the version. Every time
    * client updates the path stat.version gets incremented. Starting value of sequence number is 1.
    */
  def getSequenceId(path: String, acls: java.util.List[ACL] = UseDefaultAcls): Int = {
    val acl = if (acls == UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls
    def writeToZk: Int = zkClient.writeDataReturnStat(path, "", -1).getVersion
    try {
      writeToZk
    } catch {
      case _: ZkNoNodeException =>
        makeSurePersistentPathExists(path, acl)
        writeToZk
    }
  }

  def getAllTopics(): Seq[String] = {
    val topics = getChildrenParentMayNotExist(BrokerTopicsPath)
    if(topics == null)
      Seq.empty[String]
    else
      topics
  }

  /**
   * Returns all the entities whose configs have been overridden.
   */
  def getAllEntitiesWithConfig(entityType: String): Seq[String] = {
    val entities = getChildrenParentMayNotExist(getEntityConfigRootPath(entityType))
    if(entities == null)
      Seq.empty[String]
    else
      entities
  }

  def getAllPartitions(): Set[TopicAndPartition] = {
    val topics = getChildrenParentMayNotExist(BrokerTopicsPath)
    if (topics == null) Set.empty[TopicAndPartition]
    else {
      topics.flatMap { topic =>
        // The partitions path may not exist if the topic is in the process of being deleted
        getChildrenParentMayNotExist(getTopicPartitionsPath(topic)).map(_.toInt).map(TopicAndPartition(topic, _))
      }.toSet
    }
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  def getConsumerGroups() = {
    getChildren(ConsumersPath)
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  def getTopicsByConsumerGroup(consumerGroup:String) = {
    getChildrenParentMayNotExist(new ZKGroupDirs(consumerGroup).consumerGroupOwnersDir)
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  def getAllConsumerGroupsForTopic(topic: String): Set[String] = {
    val groups = getChildrenParentMayNotExist(ConsumersPath)
    if (groups == null) Set.empty
    else {
      groups.foldLeft(Set.empty[String]) {(consumerGroupsForTopic, group) =>
        val topics = getChildren(new ZKGroupDirs(group).consumerGroupOffsetsDir)
        if (topics.contains(topic)) consumerGroupsForTopic + group
        else consumerGroupsForTopic
      }
    }
  }

  def close() {
    zkClient.close()
  }
}

private object ZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data : Object): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes : Array[Byte]): Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
class ZKGroupDirs(val group: String) {
  def consumerDir = ZkUtils.ConsumersPath
  def consumerGroupDir = consumerDir + "/" + group
  def consumerRegistryDir = consumerGroupDir + "/ids"
  def consumerGroupOffsetsDir = consumerGroupDir + "/offsets"
  def consumerGroupOwnersDir = consumerGroupDir + "/owners"
}

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
class ZKGroupTopicDirs(group: String, topic: String) extends ZKGroupDirs(group) {
  def consumerOffsetDir = consumerGroupOffsetsDir + "/" + topic
  def consumerOwnerDir = consumerGroupOwnersDir + "/" + topic
}

object ZKConfig {
  val ZkConnectProp = "zookeeper.connect"
  val ZkSessionTimeoutMsProp = "zookeeper.session.timeout.ms"
  val ZkConnectionTimeoutMsProp = "zookeeper.connection.timeout.ms"
  val ZkSyncTimeMsProp = "zookeeper.sync.time.ms"
}

class ZKConfig(props: VerifiableProperties) {
  import ZKConfig._

  /** ZK host string */
  val zkConnect = props.getString(ZkConnectProp)

  /** zookeeper session timeout */
  val zkSessionTimeoutMs = props.getInt(ZkSessionTimeoutMsProp, 6000)

  /** the max time that the client waits to establish a connection to zookeeper */
  val zkConnectionTimeoutMs = props.getInt(ZkConnectionTimeoutMsProp, zkSessionTimeoutMs)

  /** how far a ZK follower can be behind a ZK leader */
  val zkSyncTimeMs = props.getInt(ZkSyncTimeMsProp, 2000)
}

class ZkPath(zkClient: ZkClient) {

  @volatile private var isNamespacePresent: Boolean = false

  def checkNamespace() {
    if (isNamespacePresent)
      return

    if (!zkClient.exists("/")) {
      throw new ConfigException("Zookeeper namespace does not exist")
    }
    isNamespacePresent = true
  }

  def resetNamespaceCheckedState() {
    isNamespacePresent = false
  }

  def createPersistent(path: String, data: Object, acls: java.util.List[ACL]) {
    checkNamespace()
    zkClient.createPersistent(path, data, acls)
  }

  def createPersistent(path: String, createParents: Boolean, acls: java.util.List[ACL]) {
    checkNamespace()
    zkClient.createPersistent(path, createParents, acls)
  }

  def createEphemeral(path: String, data: Object, acls: java.util.List[ACL]) {
    checkNamespace()
    zkClient.createEphemeral(path, data, acls)
  }

  def createPersistentSequential(path: String, data: Object, acls: java.util.List[ACL]): String = {
    checkNamespace()
    zkClient.createPersistentSequential(path, data, acls)
  }
}

/**
 * Creates an ephemeral znode checking the session owner
 * in the case of conflict. In the regular case, the
 * znode is created and the create call returns OK. If
 * the call receives a node exists event, then it checks
 * if the session matches. If it does, then it returns OK,
 * and otherwise it fails the operation.
 */

class ZKCheckedEphemeral(path: String,
                         data: String,
                         zkHandle: ZooKeeper,
                         isSecure: Boolean) extends Logging {
  private val createCallback = new CreateCallback
  private val getDataCallback = new GetDataCallback
  val latch: CountDownLatch = new CountDownLatch(1)
  var result: Code = Code.OK
  val defaultAcls = ZkUtils.defaultAcls(isSecure, path)

  private class CreateCallback extends StringCallback {
    def processResult(rc: Int,
                      path: String,
                      ctx: Object,
                      name: String) {
      Code.get(rc) match {
        case Code.OK =>
           setResult(Code.OK)
        case Code.CONNECTIONLOSS =>
          // try again
          createEphemeral
        case Code.NONODE =>
          error("No node for path %s (could be the parent missing)".format(path))
          setResult(Code.NONODE)
        case Code.NODEEXISTS =>
          zkHandle.getData(path, false, getDataCallback, null)
        case Code.SESSIONEXPIRED =>
          error("Session has expired while creating %s".format(path))
          setResult(Code.SESSIONEXPIRED)
        case Code.INVALIDACL =>
          error("Invalid ACL")
          setResult(Code.INVALIDACL)
        case _ =>
          warn("ZooKeeper event while creating registration node: %s %s".format(path, Code.get(rc)))
          setResult(Code.get(rc))
      }
    }
  }

  private class GetDataCallback extends DataCallback {
      def processResult(rc: Int,
                        path: String,
                        ctx: Object,
                        readData: Array[Byte],
                        stat: Stat) {
        Code.get(rc) match {
          case Code.OK =>
                if (stat.getEphemeralOwner != zkHandle.getSessionId)
                  setResult(Code.NODEEXISTS)
                else
                  setResult(Code.OK)
          case Code.NONODE =>
            info("The ephemeral node [%s] at %s has gone away while reading it, ".format(data, path))
            createEphemeral
          case Code.SESSIONEXPIRED =>
            error("Session has expired while reading znode %s".format(path))
            setResult(Code.SESSIONEXPIRED)
          case Code.INVALIDACL =>
            error("Invalid ACL")
            setResult(Code.INVALIDACL)
          case _ =>
            warn("ZooKeeper event while getting znode data: %s %s".format(path, Code.get(rc)))
            setResult(Code.get(rc))
        }
      }
  }

  private def createEphemeral() {
    zkHandle.create(path,
                    ZKStringSerializer.serialize(data),
                    defaultAcls,
                    CreateMode.EPHEMERAL,
                    createCallback,
                    null)
  }

  private def createRecursive(prefix: String, suffix: String) {
    debug("Path: %s, Prefix: %s, Suffix: %s".format(path, prefix, suffix))
    if(suffix.isEmpty()) {
      createEphemeral
    } else {
      zkHandle.create(prefix,
                      new Array[Byte](0),
                      defaultAcls,
                      CreateMode.PERSISTENT,
                      new StringCallback() {
                        def processResult(rc : Int,
                                          path : String,
                                          ctx : Object,
                                          name : String) {
                          Code.get(rc) match {
                            case Code.OK | Code.NODEEXISTS =>
                              // Nothing to do
                            case Code.CONNECTIONLOSS =>
                              // try again
                              val suffix = ctx.asInstanceOf[String]
                              createRecursive(path, suffix)
                            case Code.NONODE =>
                              error("No node for path %s (could be the parent missing)".format(path))
                              setResult(Code.get(rc))
                            case Code.SESSIONEXPIRED =>
                              error("Session has expired while creating %s".format(path))
                              setResult(Code.get(rc))
                            case Code.INVALIDACL =>
                              error("Invalid ACL")
                              setResult(Code.INVALIDACL)
                            case _ =>
                              warn("ZooKeeper event while creating registration node: %s %s".format(path, Code.get(rc)))
                              setResult(Code.get(rc))
                          }
                        }
                  },
                  suffix)
      // Update prefix and suffix
      val index = suffix.indexOf('/', 1) match {
        case -1 => suffix.length
        case x : Int => x
      }
      // Get new prefix
      val newPrefix = prefix + suffix.substring(0, index)
      // Get new suffix
      val newSuffix = suffix.substring(index, suffix.length)
      createRecursive(newPrefix, newSuffix)
    }
  }

  private def setResult(code: Code) {
    result = code
    latch.countDown()
  }

  private def waitUntilResolved(): Code = {
    latch.await()
    result
  }

  def create() {
    val index = path.indexOf('/', 1) match {
        case -1 => path.length
        case x : Int => x
    }
    val prefix = path.substring(0, index)
    val suffix = path.substring(index, path.length)
    debug(s"Path: $path, Prefix: $prefix, Suffix: $suffix")
    info(s"Creating $path (is it secure? $isSecure)")
    createRecursive(prefix, suffix)
    val result = waitUntilResolved()
    info("Result of znode creation is: %s".format(result))
    result match {
      case Code.OK =>
        // Nothing to do
      case _ =>
        throw ZkException.create(KeeperException.create(result))
    }
  }
}
