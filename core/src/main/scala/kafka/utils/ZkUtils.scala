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

import java.util.concurrent.CountDownLatch

import kafka.admin._
import kafka.api.{ApiVersion, KAFKA_0_10_0_IV0, LeaderAndIsr}
import kafka.cluster._
import kafka.common.{KafkaException, NoEpochForPartitionException, TopicAndPartition}
import kafka.consumer.{ConsumerThreadId, TopicCount}
import kafka.controller.{KafkaController, LeaderIsrAndControllerEpoch, ReassignedPartitionsContext}
import kafka.server.ConfigType
import kafka.utils.ZkUtils._
import org.I0Itec.zkclient.exception.{ZkBadVersionException, ZkException, ZkMarshallingError, ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.zookeeper.AsyncCallback.{DataCallback, StringCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.data.{ACL, Stat}
import org.apache.zookeeper.{CreateMode, KeeperException, ZooDefs, ZooKeeper}

import scala.collection._

object ZkUtils {
  val ConsumersPath = "/consumers"
  val BrokerIdsPath = "/brokers/ids"
  val BrokerTopicsPath = "/brokers/topics"
  val ControllerPath = "/controller"
  val ControllerEpochPath = "/controller_epoch"
  val ReassignPartitionsPath = "/admin/reassign_partitions"
  val DeleteTopicsPath = "/admin/delete_topics"
  val PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election"
  val BrokerSequenceIdPath = "/brokers/seqid"
  val IsrChangeNotificationPath = "/isr_change_notification"
  val EntityConfigPath = "/config"
  val EntityConfigChangesPath = "/config/changes"

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

  def DefaultAcls(isSecure: Boolean): java.util.List[ACL] = if (isSecure) {
    val list = new java.util.ArrayList[ACL]
    list.addAll(ZooDefs.Ids.CREATOR_ALL_ACL)
    list.addAll(ZooDefs.Ids.READ_ACL_UNSAFE)
    list
  } else {
    ZooDefs.Ids.OPEN_ACL_UNSAFE
  }

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
    ZkUtils.EntityConfigPath + "/" + entityType

  def getEntityConfigPath(entityType: String, entity: String): String =
    getEntityConfigRootPath(entityType) + "/" + entity

  def getDeleteTopicPath(topic: String): String =
    DeleteTopicsPath + "/" + topic
}

class ZkUtils(val zkClient: ZkClient,
              val zkConnection: ZkConnection,
              val isSecure: Boolean) extends Logging {
  // These are persistent ZK paths that should exist on kafka broker startup.
  val persistentZkPaths = Seq(ConsumersPath,
                              BrokerIdsPath,
                              BrokerTopicsPath,
                              EntityConfigChangesPath,
                              getEntityConfigRootPath(ConfigType.Topic),
                              getEntityConfigRootPath(ConfigType.Client),
                              DeleteTopicsPath,
                              BrokerSequenceIdPath,
                              IsrChangeNotificationPath)

  val securePersistentZkPaths = Seq(BrokerIdsPath,
                                    BrokerTopicsPath,
                                    EntityConfigChangesPath,
                                    getEntityConfigRootPath(ConfigType.Topic),
                                    getEntityConfigRootPath(ConfigType.Client),
                                    DeleteTopicsPath,
                                    BrokerSequenceIdPath,
                                    IsrChangeNotificationPath)

  val DefaultAcls: java.util.List[ACL] = ZkUtils.DefaultAcls(isSecure)

  def getController(): Int = {
    readDataMaybeNull(ControllerPath)._1 match {
      case Some(controller) => KafkaController.parseControllerId(controller)
      case None => throw new KafkaException("Controller doesn't exist")
    }
  }

  def getSortedBrokerList(): Seq[Int] =
    getChildren(BrokerIdsPath).map(_.toInt).sorted

  def getAllBrokersInCluster(): Seq[Broker] = {
    val brokerIds = getChildrenParentMayNotExist(BrokerIdsPath).sorted
    brokerIds.map(_.toInt).map(getBrokerInfo(_)).filter(_.isDefined).map(_.get)
  }

  def getAllBrokerEndPointsForChannel(protocolType: SecurityProtocol): Seq[BrokerEndPoint] = {
    getAllBrokersInCluster().map(_.getBrokerEndPoint(protocolType))
  }

  def getLeaderAndIsrForPartition(topic: String, partition: Int):Option[LeaderAndIsr] = {
    ReplicationUtils.getLeaderIsrAndEpochForPartition(this, topic, partition).map(_.leaderAndIsr)
  }

  def setupCommonPaths() {
    for(path <- persistentZkPaths)
      makeSurePersistentPathExists(path)
  }

  def getLeaderForPartition(topic: String, partition: Int): Option[Int] = {
    val leaderAndIsrOpt = readDataMaybeNull(getTopicPartitionLeaderAndIsrPath(topic, partition))._1
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        Json.parseFull(leaderAndIsr) match {
          case Some(m) =>
            Some(m.asInstanceOf[Map[String, Any]].get("leader").get.asInstanceOf[Int])
          case None => None
        }
      case None => None
    }
  }

  /**
   * This API should read the epoch in the ISR path. It is sufficient to read the epoch in the ISR path, since if the
   * leader fails after updating epoch in the leader path and before updating epoch in the ISR path, effectively some
   * other broker will retry becoming leader with the same new epoch value.
   */
  def getEpochForPartition(topic: String, partition: Int): Int = {
    val leaderAndIsrOpt = readDataMaybeNull(getTopicPartitionLeaderAndIsrPath(topic, partition))._1
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        Json.parseFull(leaderAndIsr) match {
          case None => throw new NoEpochForPartitionException("No epoch, leaderAndISR data for partition [%s,%d] is invalid".format(topic, partition))
          case Some(m) => m.asInstanceOf[Map[String, Any]].get("leader_epoch").get.asInstanceOf[Int]
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
          case Some(m) => m.asInstanceOf[Map[String, Any]].get("isr").get.asInstanceOf[Seq[Int]]
          case None => Seq.empty[Int]
        }
      case None => Seq.empty[Int]
    }
  }

  /**
   * Gets the assigned replicas (AR) for a specific topic and partition
   */
  def getReplicasForPartition(topic: String, partition: Int): Seq[Int] = {
    val jsonPartitionMapOpt = readDataMaybeNull(getTopicPath(topic))._1
    jsonPartitionMapOpt match {
      case Some(jsonPartitionMap) =>
        Json.parseFull(jsonPartitionMap) match {
          case Some(m) => m.asInstanceOf[Map[String, Any]].get("partitions") match {
            case Some(replicaMap) => replicaMap.asInstanceOf[Map[String, Seq[Int]]].get(partition.toString) match {
              case Some(seq) => seq
              case None => Seq.empty[Int]
            }
            case None => Seq.empty[Int]
          }
          case None => Seq.empty[Int]
        }
      case None => Seq.empty[Int]
    }
  }

  /**
   * Register brokers with v3 json format (which includes multiple endpoints and rack) if
   * the apiVersion is 0.10.0.X or above. Register the broker with v2 json format otherwise.
   * Due to KAFKA-3100, 0.9.0.0 broker and old clients will break if JSON version is above 2.
   * We include v2 to make it possible for the broker to migrate from 0.9.0.0 to 0.10.0.X without having to upgrade
   * to 0.9.0.1 first (clients have to be upgraded to 0.9.0.1 in any case).
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
                         advertisedEndpoints: collection.Map[SecurityProtocol, EndPoint],
                         jmxPort: Int,
                         rack: Option[String],
                         apiVersion: ApiVersion) {
    val brokerIdPath = BrokerIdsPath + "/" + id
    val timestamp = SystemTime.milliseconds.toString

    val version = if (apiVersion >= KAFKA_0_10_0_IV0) 3 else 2
    var jsonMap = Map("version" -> version,
                      "host" -> host,
                      "port" -> port,
                      "endpoints" -> advertisedEndpoints.values.map(_.connectionString).toArray,
                      "jmx_port" -> jmxPort,
                      "timestamp" -> timestamp
    )
    rack.foreach(rack => if (version >= 3) jsonMap += ("rack" -> rack))

    val brokerInfo = Json.encode(jsonMap)
    registerBrokerInZk(brokerIdPath, brokerInfo)

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
      case e: ZkNodeExistsException =>
        throw new RuntimeException("A broker is already registered on the path " + brokerIdPath
                + ". This probably " + "indicates that you either have configured a brokerid that is already in use, or "
                + "else you have shutdown this broker and restarted it faster than the zookeeper "
                + "timeout so it appears to be re-registering.")
    }
  }

  def getConsumerPartitionOwnerPath(group: String, topic: String, partition: Int): String = {
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    topicDirs.consumerOwnerDir + "/" + partition
  }


  def leaderAndIsrZkData(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int): String = {
    Json.encode(Map("version" -> 1, "leader" -> leaderAndIsr.leader, "leader_epoch" -> leaderAndIsr.leaderEpoch,
                    "controller_epoch" -> controllerEpoch, "isr" -> leaderAndIsr.isr))
  }

  /**
   * Get JSON partition to replica map from zookeeper.
   */
  def replicaAssignmentZkData(map: Map[String, Seq[Int]]): String = {
    Json.encode(Map("version" -> 1, "partitions" -> map))
  }

  /**
   *  make sure a persistent path exists in ZK. Create the path if not exist.
   */
  def makeSurePersistentPathExists(path: String, acls: java.util.List[ACL] = DefaultAcls) {
    //Consumer path is kept open as different consumers will write under this node.
    val acl = if (path == null || path.isEmpty || path.equals(ConsumersPath)) {
      ZooDefs.Ids.OPEN_ACL_UNSAFE
    } else acls

    if (!zkClient.exists(path))
      ZkPath.createPersistent(zkClient, path, true, acl) //won't throw NoNodeException or NodeExistsException
  }

  /**
   *  create the parent path
   */
  private def createParentPath(path: String, acls: java.util.List[ACL] = DefaultAcls): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0) {
      ZkPath.createPersistent(zkClient, parentDir, true, acls)
    }
  }

  /**
   * Create an ephemeral node with the given path and data. Create parents if necessary.
   */
  private def createEphemeralPath(path: String, data: String, acls: java.util.List[ACL] = DefaultAcls): Unit = {
    try {
      ZkPath.createEphemeral(zkClient, path, data, acls)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(path)
        ZkPath.createEphemeral(zkClient, path, data, acls)
      }
    }
  }

  /**
   * Create an ephemeral node with the given path and data.
   * Throw NodeExistException if node already exists.
   */
  def createEphemeralPathExpectConflict(path: String, data: String, acls: java.util.List[ACL] = DefaultAcls): Unit = {
    try {
      createEphemeralPath(path, data, acls)
    } catch {
      case e: ZkNodeExistsException => {
        // this can happen when there is connection loss; make sure the data is what we intend to write
        var storedData: String = null
        try {
          storedData = readData(path)._1
        } catch {
          case e1: ZkNoNodeException => // the node disappeared; treat as if node existed and let caller handles this
          case e2: Throwable => throw e2
        }
        if (storedData == null || storedData != data) {
          info("conflict in " + path + " data: " + data + " stored data: " + storedData)
          throw e
        } else {
          // otherwise, the creation succeeded, return normally
          info(path + " exists with value " + data + " during connection loss; this is ok")
        }
      }
      case e2: Throwable => throw e2
    }
  }

  /**
   * Create an persistent node with the given path and data. Create parents if necessary.
   */
  def createPersistentPath(path: String, data: String = "", acls: java.util.List[ACL] = DefaultAcls): Unit = {
    try {
      ZkPath.createPersistent(zkClient, path, data, acls)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(path)
        ZkPath.createPersistent(zkClient, path, data, acls)
      }
    }
  }

  def createSequentialPersistentPath(path: String, data: String = "", acls: java.util.List[ACL] = DefaultAcls): String = {
    ZkPath.createPersistentSequential(zkClient, path, data, acls)
  }

  /**
   * Update the value of a persistent node with the given path and data.
   * create parent directory if necessary. Never throw NodeExistException.
   * Return the updated path zkVersion
   */
  def updatePersistentPath(path: String, data: String, acls: java.util.List[ACL] = DefaultAcls) = {
    try {
      zkClient.writeData(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(path)
        try {
          ZkPath.createPersistent(zkClient, path, data, acls)
        } catch {
          case e: ZkNodeExistsException =>
            zkClient.writeData(path, data)
          case e2: Throwable => throw e2
        }
      }
      case e2: Throwable => throw e2
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
            warn("Conditional update of path %s with data %s and expected version %d failed due to %s"
              .format(path, data,expectVersion, e1.getMessage))
            (false, -1)
        }
      case e2: Exception =>
        warn("Conditional update of path %s with data %s and expected version %d failed due to %s".format(path, data,
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
   * Update the value of a persistent node with the given path and data.
   * create parent directory if necessary. Never throw NodeExistException.
   */
  def updateEphemeralPath(path: String, data: String, acls: java.util.List[ACL] = DefaultAcls): Unit = {
    try {
      zkClient.writeData(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(path)
        ZkPath.createEphemeral(zkClient, path, data, acls)
      }
      case e2: Throwable => throw e2
    }
  }

  def deletePath(path: String): Boolean = {
    try {
      zkClient.delete(path)
    } catch {
      case e: ZkNoNodeException =>
        // this can happen during a connection loss event, return normally
        info(path + " deleted during connection loss; this is ok")
        false
      case e2: Throwable => throw e2
    }
  }

  /**
    * Conditional delete the persistent path data, return true if it succeeds,
    * otherwise (the current version is not the expected version)
    */
   def conditionalDeletePath(path: String, expectedVersion: Int): Boolean = {
    try {
      zkClient.delete(path, expectedVersion)
      true
    } catch {
      case e: KeeperException.BadVersionException => false
    }
  }

  def deletePathRecursive(path: String) {
    try {
      zkClient.deleteRecursive(path)
    } catch {
      case e: ZkNoNodeException =>
        // this can happen during a connection loss event, return normally
        info(path + " deleted during connection loss; this is ok")
      case e2: Throwable => throw e2
    }
  }

  def readData(path: String): (String, Stat) = {
    val stat: Stat = new Stat()
    val dataStr: String = zkClient.readData(path, stat)
    (dataStr, stat)
  }

  def readDataMaybeNull(path: String): (Option[String], Stat) = {
    val stat: Stat = new Stat()
    val dataAndStat = try {
                        (Some(zkClient.readData(path, stat)), stat)
                      } catch {
                        case e: ZkNoNodeException =>
                          (None, stat)
                        case e2: Throwable => throw e2
                      }
    dataAndStat
  }

  def getChildren(path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    // triggers implicit conversion from java list to scala Seq
    zkClient.getChildren(path)
  }

  def getChildrenParentMayNotExist(path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    // triggers implicit conversion from java list to scala Seq
    try {
      zkClient.getChildren(path)
    } catch {
      case e: ZkNoNodeException => Nil
      case e2: Throwable => throw e2
    }
  }

  /**
   * Check if the given path exists
   */
  def pathExists(path: String): Boolean = {
    zkClient.exists(path)
  }

  def getCluster() : Cluster = {
    val cluster = new Cluster
    val nodes = getChildrenParentMayNotExist(BrokerIdsPath)
    for (node <- nodes) {
      val brokerZKString = readData(BrokerIdsPath + "/" + node)._1
      cluster.add(Broker.createBroker(node.toInt, brokerZKString))
    }
    cluster
  }

  def getPartitionLeaderAndIsrForTopics(zkClient: ZkClient, topicAndPartitions: Set[TopicAndPartition])
  : mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = {
    val ret = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
    for(topicAndPartition <- topicAndPartitions) {
      ReplicationUtils.getLeaderIsrAndEpochForPartition(this, topicAndPartition.topic, topicAndPartition.partition) match {
        case Some(leaderIsrAndControllerEpoch) => ret.put(topicAndPartition, leaderIsrAndControllerEpoch)
        case None =>
      }
    }
    ret
  }

  def getReplicaAssignmentForTopics(topics: Seq[String]): mutable.Map[TopicAndPartition, Seq[Int]] = {
    val ret = new mutable.HashMap[TopicAndPartition, Seq[Int]]
    topics.foreach { topic =>
      val jsonPartitionMapOpt = readDataMaybeNull(getTopicPath(topic))._1
      jsonPartitionMapOpt match {
        case Some(jsonPartitionMap) =>
          Json.parseFull(jsonPartitionMap) match {
            case Some(m) => m.asInstanceOf[Map[String, Any]].get("partitions") match {
              case Some(repl)  =>
                val replicaMap = repl.asInstanceOf[Map[String, Seq[Int]]]
                for((partition, replicas) <- replicaMap){
                  ret.put(TopicAndPartition(topic, partition.toInt), replicas)
                  debug("Replicas assigned to topic [%s], partition [%s] are [%s]".format(topic, partition, replicas))
                }
              case None =>
            }
            case None =>
          }
        case None =>
      }
    }
    ret
  }

  def getPartitionAssignmentForTopics(topics: Seq[String]): mutable.Map[String, collection.Map[Int, Seq[Int]]] = {
    val ret = new mutable.HashMap[String, Map[Int, Seq[Int]]]()
    topics.foreach{ topic =>
      val jsonPartitionMapOpt = readDataMaybeNull(getTopicPath(topic))._1
      val partitionMap = jsonPartitionMapOpt match {
        case Some(jsonPartitionMap) =>
          Json.parseFull(jsonPartitionMap) match {
            case Some(m) => m.asInstanceOf[Map[String, Any]].get("partitions") match {
              case Some(replicaMap) =>
                val m1 = replicaMap.asInstanceOf[Map[String, Seq[Int]]]
                m1.map(p => (p._1.toInt, p._2))
              case None => Map[Int, Seq[Int]]()
            }
            case None => Map[Int, Seq[Int]]()
          }
        case None => Map[Int, Seq[Int]]()
      }
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
      (topic -> partitionMap.keys.toSeq.sortWith((s,t) => s < t))
    }
  }

  def getPartitionsBeingReassigned(): Map[TopicAndPartition, ReassignedPartitionsContext] = {
    // read the partitions and their new replica list
    val jsonPartitionMapOpt = readDataMaybeNull(ReassignPartitionsPath)._1
    jsonPartitionMapOpt match {
      case Some(jsonPartitionMap) =>
        val reassignedPartitions = parsePartitionReassignmentData(jsonPartitionMap)
        reassignedPartitions.map(p => (p._1 -> new ReassignedPartitionsContext(p._2)))
      case None => Map.empty[TopicAndPartition, ReassignedPartitionsContext]
    }
  }

  // Parses without deduplicating keys so the data can be checked before allowing reassignment to proceed
  def parsePartitionReassignmentDataWithoutDedup(jsonData: String): Seq[(TopicAndPartition, Seq[Int])] = {
    Json.parseFull(jsonData) match {
      case Some(m) =>
        m.asInstanceOf[Map[String, Any]].get("partitions") match {
          case Some(partitionsSeq) =>
            partitionsSeq.asInstanceOf[Seq[Map[String, Any]]].map(p => {
              val topic = p.get("topic").get.asInstanceOf[String]
              val partition = p.get("partition").get.asInstanceOf[Int]
              val newReplicas = p.get("replicas").get.asInstanceOf[Seq[Int]]
              TopicAndPartition(topic, partition) -> newReplicas
            })
          case None =>
            Seq.empty
        }
      case None =>
        Seq.empty
    }
  }

  def parsePartitionReassignmentData(jsonData: String): Map[TopicAndPartition, Seq[Int]] = {
    parsePartitionReassignmentDataWithoutDedup(jsonData).toMap
  }

  def parseTopicsData(jsonData: String): Seq[String] = {
    var topics = List.empty[String]
    Json.parseFull(jsonData) match {
      case Some(m) =>
        m.asInstanceOf[Map[String, Any]].get("topics") match {
          case Some(partitionsSeq) =>
            val mapPartitionSeq = partitionsSeq.asInstanceOf[Seq[Map[String, Any]]]
            mapPartitionSeq.foreach(p => {
              val topic = p.get("topic").get.asInstanceOf[String]
              topics ++= List(topic)
            })
          case None =>
        }
      case None =>
    }
    topics
  }

  def formatAsReassignmentJson(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]): String = {
    Json.encode(Map("version" -> 1, "partitions" -> partitionsToBeReassigned.map(e => Map("topic" -> e._1.topic, "partition" -> e._1.partition,
                                                                                          "replicas" -> e._2))))
  }

  def updatePartitionReassignmentData(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]) {
    val zkPath = ReassignPartitionsPath
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
          case nne: ZkNoNodeException =>
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
      case Some(jsonPartitionList) => PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(jsonPartitionList)
      case None => Set.empty[TopicAndPartition]
    }
  }

  def deletePartition(brokerId: Int, topic: String) {
    val brokerIdPath = BrokerIdsPath + "/" + brokerId
    zkClient.delete(brokerIdPath)
    val brokerPartTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic + "/" + brokerId
    zkClient.delete(brokerPartTopicPath)
  }

  def getConsumersInGroup(group: String): Seq[String] = {
    val dirs = new ZKGroupDirs(group)
    getChildren(dirs.consumerRegistryDir)
  }

  def getConsumersPerTopic(group: String, excludeInternalTopics: Boolean) : mutable.Map[String, List[ConsumerThreadId]] = {
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
    for ( (topic, consumerList) <- consumersPerTopicMap )
      consumersPerTopicMap.put(topic, consumerList.sortWith((s,t) => s < t))
    consumersPerTopicMap
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
      case Some(brokerInfo) => Some(Broker.createBroker(brokerId, brokerInfo))
      case None => None
    }
  }

  /**
    * This API produces a sequence number by creating / updating given path in zookeeper
    * It uses the stat returned by the zookeeper and return the version. Every time
    * client updates the path stat.version gets incremented
    */
  def getSequenceId(path: String, acls: java.util.List[ACL] = DefaultAcls): Int = {
    try {
      val stat = zkClient.writeDataReturnStat(path, "", -1)
      stat.getVersion
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(BrokerSequenceIdPath, acls)
        try {
          zkClient.createPersistent(BrokerSequenceIdPath, "", acls)
          0
        } catch {
          case e: ZkNodeExistsException =>
            val stat = zkClient.writeDataReturnStat(BrokerSequenceIdPath, "", -1)
            stat.getVersion
        }
      }
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
    if(topics == null) Set.empty[TopicAndPartition]
    else {
      topics.map { topic =>
        getChildren(getTopicPartitionsPath(topic)).map(_.toInt).map(TopicAndPartition(topic, _))
      }.flatten.toSet
    }
  }

  def getConsumerGroups() = {
    getChildren(ConsumersPath)
  }

  def getTopicsByConsumerGroup(consumerGroup:String) = {
    getChildrenParentMayNotExist(new ZKGroupDirs(consumerGroup).consumerGroupOwnersDir)
  }

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
    if(zkClient != null) {
      zkClient.close()
    }
  }
}

private object ZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data : Object) : Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes : Array[Byte]) : Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}

class ZKGroupDirs(val group: String) {
  def consumerDir = ConsumersPath
  def consumerGroupDir = consumerDir + "/" + group
  def consumerRegistryDir = consumerGroupDir + "/ids"
  def consumerGroupOffsetsDir = consumerGroupDir + "/offsets"
  def consumerGroupOwnersDir = consumerGroupDir + "/owners"
}

class ZKGroupTopicDirs(group: String, topic: String) extends ZKGroupDirs(group) {
  def consumerOffsetDir = consumerGroupOffsetsDir + "/" + topic
  def consumerOwnerDir = consumerGroupOwnersDir + "/" + topic
}


class ZKConfig(props: VerifiableProperties) {
  /** ZK host string */
  val zkConnect = props.getString("zookeeper.connect")

  /** zookeeper session timeout */
  val zkSessionTimeoutMs = props.getInt("zookeeper.session.timeout.ms", 6000)

  /** the max time that the client waits to establish a connection to zookeeper */
  val zkConnectionTimeoutMs = props.getInt("zookeeper.connection.timeout.ms",zkSessionTimeoutMs)

  /** how far a ZK follower can be behind a ZK leader */
  val zkSyncTimeMs = props.getInt("zookeeper.sync.time.ms", 2000)
}

object ZkPath {
  @volatile private var isNamespacePresent: Boolean = false

  def checkNamespace(client: ZkClient) {
    if(isNamespacePresent)
      return

    if (!client.exists("/")) {
      throw new ConfigException("Zookeeper namespace does not exist")
    }
    isNamespacePresent = true
  }

  def resetNamespaceCheckedState {
    isNamespacePresent = false
  }

  def createPersistent(client: ZkClient, path: String, data: Object, acls: java.util.List[ACL]) {
    checkNamespace(client)
    client.createPersistent(path, data, acls)
  }

  def createPersistent(client: ZkClient, path: String, createParents: Boolean, acls: java.util.List[ACL]) {
    checkNamespace(client)
    client.createPersistent(path, createParents, acls)
  }

  def createEphemeral(client: ZkClient, path: String, data: Object, acls: java.util.List[ACL]) {
    checkNamespace(client)
    client.createEphemeral(path, data, acls)
  }

  def createPersistentSequential(client: ZkClient, path: String, data: Object, acls: java.util.List[ACL]): String = {
    checkNamespace(client)
    client.createPersistentSequential(path, data, acls)
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
                    DefaultAcls(isSecure),
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
                      DefaultAcls(isSecure),
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
