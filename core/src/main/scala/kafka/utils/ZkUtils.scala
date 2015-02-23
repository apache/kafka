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

import kafka.cluster.{Broker, Cluster}
import kafka.consumer.{ConsumerThreadId, TopicCount}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNodeExistsException, ZkNoNodeException,
  ZkMarshallingError, ZkBadVersionException}
import org.I0Itec.zkclient.serialize.ZkSerializer
import collection._
import kafka.api.LeaderAndIsr
import org.apache.zookeeper.data.Stat
import kafka.admin._
import kafka.common.{KafkaException, NoEpochForPartitionException}
import kafka.controller.ReassignedPartitionsContext
import kafka.controller.KafkaController
import scala.Some
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.common.TopicAndPartition
import scala.collection

object ZkUtils extends Logging {
  val ConsumersPath = "/consumers"
  val BrokerIdsPath = "/brokers/ids"
  val BrokerTopicsPath = "/brokers/topics"
  val TopicConfigPath = "/config/topics"
  val TopicConfigChangesPath = "/config/changes"
  val ControllerPath = "/controller"
  val ControllerEpochPath = "/controller_epoch"
  val ReassignPartitionsPath = "/admin/reassign_partitions"
  val DeleteTopicsPath = "/admin/delete_topics"
  val PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election"

  def getTopicPath(topic: String): String = {
    BrokerTopicsPath + "/" + topic
  }

  def getTopicPartitionsPath(topic: String): String = {
    getTopicPath(topic) + "/partitions"
  }

  def getTopicConfigPath(topic: String): String =
    TopicConfigPath + "/" + topic

  def getDeleteTopicPath(topic: String): String =
    DeleteTopicsPath + "/" + topic

  def getController(zkClient: ZkClient): Int = {
    readDataMaybeNull(zkClient, ControllerPath)._1 match {
      case Some(controller) => KafkaController.parseControllerId(controller)
      case None => throw new KafkaException("Controller doesn't exist")
    }
  }

  def getTopicPartitionPath(topic: String, partitionId: Int): String =
    getTopicPartitionsPath(topic) + "/" + partitionId

  def getTopicPartitionLeaderAndIsrPath(topic: String, partitionId: Int): String =
    getTopicPartitionPath(topic, partitionId) + "/" + "state"

  def getSortedBrokerList(zkClient: ZkClient): Seq[Int] =
    ZkUtils.getChildren(zkClient, BrokerIdsPath).map(_.toInt).sorted

  def getAllBrokersInCluster(zkClient: ZkClient): Seq[Broker] = {
    val brokerIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath).sorted
    brokerIds.map(_.toInt).map(getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
  }

  def getLeaderAndIsrForPartition(zkClient: ZkClient, topic: String, partition: Int):Option[LeaderAndIsr] = {
    ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition).map(_.leaderAndIsr)
  }

  def setupCommonPaths(zkClient: ZkClient) {
    for(path <- Seq(ConsumersPath, BrokerIdsPath, BrokerTopicsPath, TopicConfigChangesPath, TopicConfigPath, DeleteTopicsPath))
      makeSurePersistentPathExists(zkClient, path)
  }

  def getLeaderForPartition(zkClient: ZkClient, topic: String, partition: Int): Option[Int] = {
    val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1
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
  def getEpochForPartition(zkClient: ZkClient, topic: String, partition: Int): Int = {
    val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1
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

  /**
   * Gets the in-sync replicas (ISR) for a specific topic and partition
   */
  def getInSyncReplicasForPartition(zkClient: ZkClient, topic: String, partition: Int): Seq[Int] = {
    val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1
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
  def getReplicasForPartition(zkClient: ZkClient, topic: String, partition: Int): Seq[Int] = {
    val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
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

  def registerBrokerInZk(zkClient: ZkClient, id: Int, host: String, port: Int, timeout: Int, jmxPort: Int) {
    val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + id
    val timestamp = SystemTime.milliseconds.toString
    val brokerInfo = Json.encode(Map("version" -> 1, "host" -> host, "port" -> port, "jmx_port" -> jmxPort, "timestamp" -> timestamp))
    val expectedBroker = new Broker(id, host, port)

    try {
      createEphemeralPathExpectConflictHandleZKBug(zkClient, brokerIdPath, brokerInfo, expectedBroker,
        (brokerString: String, broker: Any) => Broker.createBroker(broker.asInstanceOf[Broker].id, brokerString).equals(broker.asInstanceOf[Broker]),
        timeout)

    } catch {
      case e: ZkNodeExistsException =>
        throw new RuntimeException("A broker is already registered on the path " + brokerIdPath
          + ". This probably " + "indicates that you either have configured a brokerid that is already in use, or "
          + "else you have shutdown this broker and restarted it faster than the zookeeper "
          + "timeout so it appears to be re-registering.")
    }
    info("Registered broker %d at path %s with address %s:%d.".format(id, brokerIdPath, host, port))
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
  def makeSurePersistentPathExists(client: ZkClient, path: String) {
    if (!client.exists(path))
      client.createPersistent(path, true) // won't throw NoNodeException or NodeExistsException
  }

  /**
   *  create the parent path
   */
  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  /**
   * Create an ephemeral node with the given path and data. Create parents if necessary.
   */
  private def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createEphemeral(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
    }
  }

  /**
   * Create an ephemeral node with the given path and data.
   * Throw NodeExistException if node already exists.
   */
  def createEphemeralPathExpectConflict(client: ZkClient, path: String, data: String): Unit = {
    try {
      createEphemeralPath(client, path, data)
    } catch {
      case e: ZkNodeExistsException => {
        // this can happen when there is connection loss; make sure the data is what we intend to write
        var storedData: String = null
        try {
          storedData = readData(client, path)._1
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
   * Create an ephemeral node with the given path and data.
   * Throw NodeExistsException if node already exists.
   * Handles the following ZK session timeout bug:
   *
   * https://issues.apache.org/jira/browse/ZOOKEEPER-1740
   *
   * Upon receiving a NodeExistsException, read the data from the conflicted path and
   * trigger the checker function comparing the read data and the expected data,
   * If the checker function returns true then the above bug might be encountered, back off and retry;
   * otherwise re-throw the exception
   */
  def createEphemeralPathExpectConflictHandleZKBug(zkClient: ZkClient, path: String, data: String, expectedCallerData: Any, checker: (String, Any) => Boolean, backoffTime: Int): Unit = {
    while (true) {
      try {
        createEphemeralPathExpectConflict(zkClient, path, data)
        return
      } catch {
        case e: ZkNodeExistsException => {
          // An ephemeral node may still exist even after its corresponding session has expired
          // due to a Zookeeper bug, in this case we need to retry writing until the previous node is deleted
          // and hence the write succeeds without ZkNodeExistsException
          ZkUtils.readDataMaybeNull(zkClient, path)._1 match {
            case Some(writtenData) => {
              if (checker(writtenData, expectedCallerData)) {
                info("I wrote this conflicted ephemeral node [%s] at %s a while back in a different session, ".format(data, path)
                  + "hence I will backoff for this node to be deleted by Zookeeper and retry")

                Thread.sleep(backoffTime)
              } else {
                throw e
              }
            }
            case None => // the node disappeared; retry creating the ephemeral node immediately
          }
        }
        case e2: Throwable => throw e2
      }
    }
  }

  /**
   * Create an persistent node with the given path and data. Create parents if necessary.
   */
  def createPersistentPath(client: ZkClient, path: String, data: String = ""): Unit = {
    try {
      client.createPersistent(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createPersistent(path, data)
      }
    }
  }

  def createSequentialPersistentPath(client: ZkClient, path: String, data: String = ""): String = {
    client.createPersistentSequential(path, data)
  }

  /**
   * Update the value of a persistent node with the given path and data.
   * create parrent directory if necessary. Never throw NodeExistException.
   * Return the updated path zkVersion
   */
  def updatePersistentPath(client: ZkClient, path: String, data: String) = {
    try {
      client.writeData(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        try {
          client.createPersistent(path, data)
        } catch {
          case e: ZkNodeExistsException =>
            client.writeData(path, data)
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
  def conditionalUpdatePersistentPath(client: ZkClient, path: String, data: String, expectVersion: Int,
    optionalChecker:Option[(ZkClient, String, String) => (Boolean,Int)] = None): (Boolean, Int) = {
    try {
      val stat = client.writeDataReturnStat(path, data, expectVersion)
      debug("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d"
        .format(path, data, expectVersion, stat.getVersion))
      (true, stat.getVersion)
    } catch {
      case e1: ZkBadVersionException =>
        optionalChecker match {
          case Some(checker) => return checker(client, path, data)
          case _ => debug("Checker method is not passed skipping zkData match")
        }
        warn("Conditional update of path %s with data %s and expected version %d failed due to %s".format(path, data,
          expectVersion, e1.getMessage))
        (false, -1)
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
  def conditionalUpdatePersistentPathIfExists(client: ZkClient, path: String, data: String, expectVersion: Int): (Boolean, Int) = {
    try {
      val stat = client.writeDataReturnStat(path, data, expectVersion)
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
   * create parrent directory if necessary. Never throw NodeExistException.
   */
  def updateEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.writeData(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
      case e2: Throwable => throw e2
    }
  }

  def deletePath(client: ZkClient, path: String): Boolean = {
    try {
      client.delete(path)
    } catch {
      case e: ZkNoNodeException =>
        // this can happen during a connection loss event, return normally
        info(path + " deleted during connection loss; this is ok")
        false
      case e2: Throwable => throw e2
    }
  }

  def deletePathRecursive(client: ZkClient, path: String) {
    try {
      client.deleteRecursive(path)
    } catch {
      case e: ZkNoNodeException =>
        // this can happen during a connection loss event, return normally
        info(path + " deleted during connection loss; this is ok")
      case e2: Throwable => throw e2
    }
  }

  def maybeDeletePath(zkUrl: String, dir: String) {
    try {
      val zk = new ZkClient(zkUrl, 30*1000, 30*1000, ZKStringSerializer)
      zk.deleteRecursive(dir)
      zk.close()
    } catch {
      case _: Throwable => // swallow
    }
  }

  def readData(client: ZkClient, path: String): (String, Stat) = {
    val stat: Stat = new Stat()
    val dataStr: String = client.readData(path, stat)
    (dataStr, stat)
  }

  def readDataMaybeNull(client: ZkClient, path: String): (Option[String], Stat) = {
    val stat: Stat = new Stat()
    val dataAndStat = try {
                        (Some(client.readData(path, stat)), stat)
                      } catch {
                        case e: ZkNoNodeException =>
                          (None, stat)
                        case e2: Throwable => throw e2
                      }
    dataAndStat
  }

  def getChildren(client: ZkClient, path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    // triggers implicit conversion from java list to scala Seq
    client.getChildren(path)
  }

  def getChildrenParentMayNotExist(client: ZkClient, path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    // triggers implicit conversion from java list to scala Seq
    try {
      client.getChildren(path)
    } catch {
      case e: ZkNoNodeException => return Nil
      case e2: Throwable => throw e2
    }
  }

  /**
   * Check if the given path exists
   */
  def pathExists(client: ZkClient, path: String): Boolean = {
    client.exists(path)
  }

  def getCluster(zkClient: ZkClient) : Cluster = {
    val cluster = new Cluster
    val nodes = getChildrenParentMayNotExist(zkClient, BrokerIdsPath)
    for (node <- nodes) {
      val brokerZKString = readData(zkClient, BrokerIdsPath + "/" + node)._1
      cluster.add(Broker.createBroker(node.toInt, brokerZKString))
    }
    cluster
  }

  def getPartitionLeaderAndIsrForTopics(zkClient: ZkClient, topicAndPartitions: Set[TopicAndPartition])
  : mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = {
    val ret = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
    for(topicAndPartition <- topicAndPartitions) {
      ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topicAndPartition.topic, topicAndPartition.partition) match {
        case Some(leaderIsrAndControllerEpoch) => ret.put(topicAndPartition, leaderIsrAndControllerEpoch)
        case None =>
      }
    }
    ret
  }

  def getReplicaAssignmentForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[TopicAndPartition, Seq[Int]] = {
    val ret = new mutable.HashMap[TopicAndPartition, Seq[Int]]
    topics.foreach { topic =>
      val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
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

  def getPartitionAssignmentForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, collection.Map[Int, Seq[Int]]] = {
    val ret = new mutable.HashMap[String, Map[Int, Seq[Int]]]()
    topics.foreach{ topic =>
      val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
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

  def getPartitionsForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, Seq[Int]] = {
    getPartitionAssignmentForTopics(zkClient, topics).map { topicAndPartitionMap =>
      val topic = topicAndPartitionMap._1
      val partitionMap = topicAndPartitionMap._2
      debug("partition assignment of /brokers/topics/%s is %s".format(topic, partitionMap))
      (topic -> partitionMap.keys.toSeq.sortWith((s,t) => s < t))
    }
  }

  def getPartitionsBeingReassigned(zkClient: ZkClient): Map[TopicAndPartition, ReassignedPartitionsContext] = {
    // read the partitions and their new replica list
    val jsonPartitionMapOpt = readDataMaybeNull(zkClient, ReassignPartitionsPath)._1
    jsonPartitionMapOpt match {
      case Some(jsonPartitionMap) =>
        val reassignedPartitions = parsePartitionReassignmentData(jsonPartitionMap)
        reassignedPartitions.map(p => (p._1 -> new ReassignedPartitionsContext(p._2)))
      case None => Map.empty[TopicAndPartition, ReassignedPartitionsContext]
    }
  }

  // Parses without deduplicating keys so the the data can be checked before allowing reassignment to proceed
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

  def getPartitionReassignmentZkData(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]): String = {
    Json.encode(Map("version" -> 1, "partitions" -> partitionsToBeReassigned.map(e => Map("topic" -> e._1.topic, "partition" -> e._1.partition,
                                                                                          "replicas" -> e._2))))
  }

  def updatePartitionReassignmentData(zkClient: ZkClient, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]) {
    val zkPath = ZkUtils.ReassignPartitionsPath
    partitionsToBeReassigned.size match {
      case 0 => // need to delete the /admin/reassign_partitions path
        deletePath(zkClient, zkPath)
        info("No more partitions need to be reassigned. Deleting zk path %s".format(zkPath))
      case _ =>
        val jsonData = getPartitionReassignmentZkData(partitionsToBeReassigned)
        try {
          updatePersistentPath(zkClient, zkPath, jsonData)
          info("Updated partition reassignment path with %s".format(jsonData))
        } catch {
          case nne: ZkNoNodeException =>
            ZkUtils.createPersistentPath(zkClient, zkPath, jsonData)
            debug("Created path %s with %s for partition reassignment".format(zkPath, jsonData))
          case e2: Throwable => throw new AdminOperationException(e2.toString)
        }
    }
  }

  def getPartitionsUndergoingPreferredReplicaElection(zkClient: ZkClient): Set[TopicAndPartition] = {
    // read the partitions and their new replica list
    val jsonPartitionListOpt = readDataMaybeNull(zkClient, PreferredReplicaLeaderElectionPath)._1
    jsonPartitionListOpt match {
      case Some(jsonPartitionList) => PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(jsonPartitionList)
      case None => Set.empty[TopicAndPartition]
    }
  }

  def deletePartition(zkClient : ZkClient, brokerId: Int, topic: String) {
    val brokerIdPath = BrokerIdsPath + "/" + brokerId
    zkClient.delete(brokerIdPath)
    val brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/" + brokerId
    zkClient.delete(brokerPartTopicPath)
  }

  def getConsumersInGroup(zkClient: ZkClient, group: String): Seq[String] = {
    val dirs = new ZKGroupDirs(group)
    getChildren(zkClient, dirs.consumerRegistryDir)
  }

  def getConsumersPerTopic(zkClient: ZkClient, group: String, excludeInternalTopics: Boolean) : mutable.Map[String, List[ConsumerThreadId]] = {
    val dirs = new ZKGroupDirs(group)
    val consumers = getChildrenParentMayNotExist(zkClient, dirs.consumerRegistryDir)
    val consumersPerTopicMap = new mutable.HashMap[String, List[ConsumerThreadId]]
    for (consumer <- consumers) {
      val topicCount = TopicCount.constructTopicCount(group, consumer, zkClient, excludeInternalTopics)
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
   * @param brokerId The broker id
   * @param zkClient The zookeeper client connection
   * @return An optional Broker object encapsulating the broker metadata
   */
  def getBrokerInfo(zkClient: ZkClient, brokerId: Int): Option[Broker] = {
    ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
      case Some(brokerInfo) => Some(Broker.createBroker(brokerId, brokerInfo))
      case None => None
    }
  }

  def getAllTopics(zkClient: ZkClient): Seq[String] = {
    val topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath)
    if(topics == null)
      Seq.empty[String]
    else
      topics
  }

  def getAllPartitions(zkClient: ZkClient): Set[TopicAndPartition] = {
    val topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath)
    if(topics == null) Set.empty[TopicAndPartition]
    else {
      topics.map { topic =>
        getChildren(zkClient, getTopicPartitionsPath(topic)).map(_.toInt).map(TopicAndPartition(topic, _))
      }.flatten.toSet
    }
  }
}

object ZKStringSerializer extends ZkSerializer {

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
  def consumerDir = ZkUtils.ConsumersPath
  def consumerGroupDir = consumerDir + "/" + group
  def consumerRegistryDir = consumerGroupDir + "/ids"
}

class ZKGroupTopicDirs(group: String, topic: String) extends ZKGroupDirs(group) {
  def consumerOffsetDir = consumerGroupDir + "/offsets/" + topic
  def consumerOwnerDir = consumerGroupDir + "/owners/" + topic
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
