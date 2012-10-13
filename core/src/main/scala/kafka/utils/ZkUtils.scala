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
import kafka.consumer.TopicCount
import org.I0Itec.zkclient.{IZkDataListener, ZkClient}
import org.I0Itec.zkclient.exception.{ZkNodeExistsException, ZkNoNodeException, ZkMarshallingError}
import org.I0Itec.zkclient.serialize.ZkSerializer
import scala.collection._
import kafka.api.LeaderAndIsr
import org.apache.zookeeper.data.Stat
import java.util.concurrent.locks.{ReentrantLock, Condition}
import kafka.controller.{PartitionAndReplica, ReassignedPartitionsContext}
import kafka.admin._
import kafka.common.{TopicAndPartition, KafkaException, NoEpochForPartitionException}

object ZkUtils extends Logging {
  val ConsumersPath = "/consumers"
  val BrokerIdsPath = "/brokers/ids"
  val BrokerTopicsPath = "/brokers/topics"
  val ControllerPath = "/controller"
  val ReassignPartitionsPath = "/admin/reassign_partitions"
  val PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election"

  def getTopicPath(topic: String): String ={
    BrokerTopicsPath + "/" + topic
  }

  def getTopicPartitionsPath(topic: String): String ={
    getTopicPath(topic) + "/partitions"
  }

  def getController(zkClient: ZkClient): Int= {
    readDataMaybeNull(zkClient, ControllerPath)._1 match {
      case Some(controller) => controller.toInt
      case None => throw new KafkaException("Controller doesn't exist")
    }
  }

  def getTopicPartitionPath(topic: String, partitionId: Int): String ={
    getTopicPartitionsPath(topic) + "/" + partitionId
  }

  def getTopicPartitionLeaderAndIsrPath(topic: String, partitionId: Int): String ={
    getTopicPartitionPath(topic, partitionId) + "/" + "leaderAndISR"
  }

  def getSortedBrokerList(zkClient: ZkClient): Seq[String] ={
    ZkUtils.getChildren(zkClient, ZkUtils.BrokerIdsPath).sorted
  }

  def getAllLiveBrokerIds(zkClient: ZkClient): Set[Int] = {
    ZkUtils.getChildren(zkClient, BrokerIdsPath).map(_.toInt).toSet
  }

  def getAllBrokersInCluster(zkClient: ZkClient): Seq[Broker] = {
    val brokerIds = ZkUtils.getChildren(zkClient, ZkUtils.BrokerIdsPath).sorted
    brokerIds.map(_.toInt).map(getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
  }

  def getLeaderAndIsrForPartition(zkClient: ZkClient, topic: String, partition: Int):Option[LeaderAndIsr] = {
    val leaderAndISRPath = getTopicPartitionLeaderAndIsrPath(topic, partition)
    val leaderAndIsrInfo = readDataMaybeNull(zkClient, leaderAndISRPath)
    val leaderAndIsrOpt = leaderAndIsrInfo._1
    val stat = leaderAndIsrInfo._2
    leaderAndIsrOpt match {
      case Some(leaderAndIsrStr) => parseLeaderAndIsr(leaderAndIsrStr, topic, partition, stat)
      case None => None
    }
  }

  def parseLeaderAndIsr(leaderAndIsrStr: String, topic: String, partition: Int, stat: Stat): Option[LeaderAndIsr] = {
    SyncJSON.parseFull(leaderAndIsrStr) match {
      case Some(m) =>
        val leader = m.asInstanceOf[Map[String, String]].get("leader").get.toInt
        val epoch = m.asInstanceOf[Map[String, String]].get("leaderEpoch").get.toInt
        val isrString = m.asInstanceOf[Map[String, String]].get("ISR").get
        val isr = Utils.getCSVList(isrString).map(r => r.toInt)
        val zkPathVersion = stat.getVersion
        debug("Leader %d, Epoch %d, Isr %s, Zk path version %d for topic %s and partition %d".format(leader, epoch,
          isr.toString(), zkPathVersion, topic, partition))
        Some(LeaderAndIsr(leader, epoch, isr.toList, zkPathVersion))
      case None => None
    }
  }

  def getLeaderForPartition(zkClient: ZkClient, topic: String, partition: Int): Option[Int] = {
    val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        SyncJSON.parseFull(leaderAndIsr) match {
          case Some(m) =>
            Some(m.asInstanceOf[Map[String, String]].get("leader").get.toInt)
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
        SyncJSON.parseFull(leaderAndIsr) match {
          case None => throw new NoEpochForPartitionException("No epoch, leaderAndISR data for topic %s partition %d is invalid".format(topic, partition))
          case Some(m) => m.asInstanceOf[Map[String, String]].get("leaderEpoch").get.toInt
        }
      case None => throw new NoEpochForPartitionException("No epoch, ISR path for topic %s partition %d is empty"
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
        SyncJSON.parseFull(leaderAndIsr) match {
          case Some(m) =>
            val ISRString = m.asInstanceOf[Map[String, String]].get("ISR").get
            Utils.getCSVList(ISRString).map(r => r.toInt)
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
        SyncJSON.parseFull(jsonPartitionMap) match {
          case Some(m) => m.asInstanceOf[Map[String, List[String]]].get(partition.toString) match {
            case None => Seq.empty[Int]
            case Some(seq) => seq.map(_.toInt)
          }
          case None => Seq.empty[Int]
        }
      case None => Seq.empty[Int]
    }
  }

  def isPartitionOnBroker(zkClient: ZkClient, topic: String, partition: Int, brokerId: Int): Boolean = {
    val replicas = getReplicasForPartition(zkClient, topic, partition)
    debug("The list of replicas for topic %s, partition %d is %s".format(topic, partition, replicas))
    replicas.contains(brokerId.toString)
  }

  def registerBrokerInZk(zkClient: ZkClient, id: Int, host: String, creator: String, port: Int) {
    val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + id
    val broker = new Broker(id, creator, host, port)
    try {
      createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString)
    } catch {
      case e: ZkNodeExistsException =>
        throw new RuntimeException("A broker is already registered on the path " + brokerIdPath + ". This probably " + "indicates that you either have configured a brokerid that is already in use, or " + "else you have shutdown this broker and restarted it faster than the zookeeper " + "timeout so it appears to be re-registering.")
    }
    info("Registering broker " + brokerIdPath + " succeeded with " + broker)
  }

  def getConsumerPartitionOwnerPath(group: String, topic: String, partition: Int): String = {
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    topicDirs.consumerOwnerDir + "/" + partition
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
          case e2 => throw e2
        }
        if (storedData == null || storedData != data) {
          info("conflict in " + path + " data: " + data + " stored data: " + storedData)
          throw e
        } else {
          // otherwise, the creation succeeded, return normally
          info(path + " exists with value " + data + " during connection loss; this is ok")
        }
      }
      case e2 => throw e2
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
  def updatePersistentPath(client: ZkClient, path: String, data: String): Int = {
    var stat: Stat = null
    try {
      stat = client.writeData(path, data)
      return stat.getVersion
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        try {
          client.createPersistent(path, data)
          // When the new path is created, its zkVersion always starts from 0
          return 0
        } catch {
          case e: ZkNodeExistsException =>
            stat = client.writeData(path, data)
            return  stat.getVersion
          case e2 => throw e2
        }
      }
      case e2 => throw e2
    }
  }

  /**
   * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the path doesn't
   * exist, the current version is not the expected version, etc.) return (false, -1)
   */
  def conditionalUpdatePersistentPath(client: ZkClient, path: String, data: String, expectVersion: Int): (Boolean, Int) = {
    try {
      val stat = client.writeData(path, data, expectVersion)
      info("Conditional update of zkPath %s with value %s and expected version %d succeeded, returning the new version: %d"
        .format(path, data, expectVersion, stat.getVersion))
      (true, stat.getVersion)
    } catch {
      case e: Exception =>
        error("Conditional update of zkPath %s with data %s and expected version %d failed".format(path, data,
          expectVersion), e)
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
      case e2 => throw e2
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
      case e2 => throw e2
    }
  }

  def deletePathRecursive(client: ZkClient, path: String) {
    try {
      client.deleteRecursive(path)
    } catch {
      case e: ZkNoNodeException =>
        // this can happen during a connection loss event, return normally
        info(path + " deleted during connection loss; this is ok")
      case e2 => throw e2
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
                        case e2 => throw e2
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
      case e2 => throw e2
    }
  }

  /**
   * Check if the given path exists
   */
  def pathExists(client: ZkClient, path: String): Boolean = {
    client.exists(path)
  }

  def getLastPart(path : String) : String = path.substring(path.lastIndexOf('/') + 1)

  def getCluster(zkClient: ZkClient) : Cluster = {
    val cluster = new Cluster
    val nodes = getChildrenParentMayNotExist(zkClient, BrokerIdsPath)
    for (node <- nodes) {
      val brokerZKString = readData(zkClient, BrokerIdsPath + "/" + node)._1
      cluster.add(Broker.createBroker(node.toInt, brokerZKString))
    }
    cluster
  }

  def getReplicaAssignmentForTopics(zkClient: ZkClient, topics: Iterator[String]):
  mutable.Map[(String, Int), Seq[Int]] = {
    val ret = new mutable.HashMap[(String, Int), Seq[Int]]
    topics.foreach { topic =>
      val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
      jsonPartitionMapOpt match {
        case Some(jsonPartitionMap) =>
          SyncJSON.parseFull(jsonPartitionMap) match {
            case Some(m) =>
              val replicaMap = m.asInstanceOf[Map[String, Seq[String]]]
              for((partition, replicas) <- replicaMap){
                ret.put((topic, partition.toInt), replicas.map(_.toInt))
                debug("Replicas assigned to topic [%s], partition [%s] are [%s]".format(topic, partition, replicas))
              }
            case None =>
          }
        case None =>
      }
    }
    ret
  }

  def getPartitionLeaderAndIsrForTopics(zkClient: ZkClient, topics: Seq[String]):
  mutable.Map[TopicAndPartition, LeaderAndIsr] = {
    val ret = new mutable.HashMap[TopicAndPartition, LeaderAndIsr]
    val partitionsForTopics = getPartitionsForTopics(zkClient, topics)
    for((topic, partitions) <- partitionsForTopics) {
      for(partition <- partitions) {
        ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition.toInt) match {
          case Some(leaderAndIsr) => ret.put(TopicAndPartition(topic, partition.toInt), leaderAndIsr)
          case None =>
        }
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
          SyncJSON.parseFull(jsonPartitionMap) match {
            case Some(m) =>
              val replicaMap = m.asInstanceOf[Map[String, Seq[String]]]
              for((partition, replicas) <- replicaMap){
                ret.put(TopicAndPartition(topic, partition.toInt), replicas.map(_.toInt))
                debug("Replicas assigned to topic [%s], partition [%s] are [%s]".format(topic, partition, replicas))
              }
            case None =>
          }
        case None =>
      }
    }
    ret
  }

  def getPartitionAssignmentForTopics(zkClient: ZkClient, topics: Seq[String]):
  mutable.Map[String, collection.Map[Int, Seq[Int]]] = {
    val ret = new mutable.HashMap[String, Map[Int, Seq[Int]]]()
    topics.foreach{ topic =>
      val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1
      val partitionMap = jsonPartitionMapOpt match {
        case Some(jsonPartitionMap) =>
          SyncJSON.parseFull(jsonPartitionMap) match {
            case Some(m) =>
              val m1 = m.asInstanceOf[Map[String, Seq[String]]]
              m1.map(p => (p._1.toInt, p._2.map(_.toInt)))
            case None => Map[Int, Seq[Int]]()
          }
        case None => Map[Int, Seq[Int]]()
      }
      debug("Partition map for /brokers/topics/%s is %s".format(topic, partitionMap))
      ret += (topic -> partitionMap)
    }
    ret
  }

  def getReplicaAssignmentFromPartitionAssignment(topicPartitionAssignment: mutable.Map[String, collection.Map[Int, Seq[Int]]]):
  mutable.Map[(String, Int), Seq[Int]] = {
    val ret = new mutable.HashMap[(String, Int), Seq[Int]]
    for((topic, partitionAssignment) <- topicPartitionAssignment){
      for((partition, replicaAssignment) <- partitionAssignment){
        ret.put((topic, partition), replicaAssignment)
      }
    }
    ret
  }

  def getPartitionsForTopics(zkClient: ZkClient, topics: Seq[String]): mutable.Map[String, Seq[Int]] = {
    getPartitionAssignmentForTopics(zkClient, topics).map
    { topicAndPartitionMap =>
      val topic = topicAndPartitionMap._1
      val partitionMap = topicAndPartitionMap._2
      debug("partition assignment of /brokers/topics/%s is %s".format(topic, partitionMap))
      (topic -> partitionMap.keys.toSeq.sortWith((s,t) => s < t))
    }
  }

  def getPartitionsAssignedToBroker(zkClient: ZkClient, topics: Seq[String], brokerId: Int):
    Seq[(String, Int)] = {
    val topicsAndPartitions = getPartitionAssignmentForTopics(zkClient, topics)
    topicsAndPartitions.map { topicAndPartitionMap =>
      val topic = topicAndPartitionMap._1
      val partitionMap = topicAndPartitionMap._2
      val relevantPartitionsMap = partitionMap.filter( m => m._2.contains(brokerId) )
      val relevantPartitions = relevantPartitionsMap.map(_._1)
      for(relevantPartition <- relevantPartitions) yield {
        (topic, relevantPartition)
      }
    }.flatten[(String, Int)].toSeq
  }

  def getPartitionsBeingReassigned(zkClient: ZkClient): Map[TopicAndPartition, ReassignedPartitionsContext] = {
    // read the partitions and their new replica list
    val jsonPartitionMapOpt = readDataMaybeNull(zkClient, ReassignPartitionsPath)._1
    jsonPartitionMapOpt match {
      case Some(jsonPartitionMap) =>
        val reassignedPartitions = parsePartitionReassignmentData(jsonPartitionMap)
        reassignedPartitions.map { p =>
          val newReplicas = p._2
          (p._1 -> new ReassignedPartitionsContext(newReplicas))
        }
      case None => Map.empty[TopicAndPartition, ReassignedPartitionsContext]
    }
  }

  def parsePartitionReassignmentData(jsonData: String):Map[TopicAndPartition, Seq[Int]] = {
    SyncJSON.parseFull(jsonData) match {
      case Some(m) =>
        val replicaMap = m.asInstanceOf[Map[String, Seq[String]]]
        replicaMap.map { reassignedPartitions =>
          val topic = reassignedPartitions._1.split(",").head
          val partition = reassignedPartitions._1.split(",").last.toInt
          val newReplicas = reassignedPartitions._2.map(_.toInt)
          TopicAndPartition(topic, partition) -> newReplicas
        }
      case None => Map.empty[TopicAndPartition, Seq[Int]]
    }
  }

  def updatePartitionReassignmentData(zkClient: ZkClient, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]]) {
    val zkPath = ZkUtils.ReassignPartitionsPath
    partitionsToBeReassigned.size match {
      case 0 => // need to delete the /admin/reassign_partitions path
        deletePath(zkClient, zkPath)
        info("No more partitions need to be reassigned. Deleting zk path %s".format(zkPath))
      case _ =>
        val jsonData = Utils.mapToJson(partitionsToBeReassigned.map(p =>
          ("%s,%s".format(p._1.topic, p._1.partition)) -> p._2.map(_.toString)))
        try {
          updatePersistentPath(zkClient, zkPath, jsonData)
          info("Updated partition reassignment path with %s".format(jsonData))
        }catch {
          case nne: ZkNoNodeException =>
            ZkUtils.createPersistentPath(zkClient, zkPath, jsonData)
            debug("Created path %s with %s for partition reassignment".format(zkPath, jsonData))
          case e2 => throw new AdministrationException(e2.toString)
        }
    }
  }

  def getAllReplicasOnBroker(zkClient: ZkClient, topics: Seq[String], brokerIds: Seq[Int]): Set[PartitionAndReplica] = {
    Set.empty[PartitionAndReplica] ++ brokerIds.map { brokerId =>
      // read all the partitions and their assigned replicas into a map organized by
      // { replica id -> partition 1, partition 2...
      val partitionsAssignedToThisBroker = getPartitionsAssignedToBroker(zkClient, topics, brokerId)
      if(partitionsAssignedToThisBroker.size == 0)
        info("No state transitions triggered since no partitions are assigned to brokers %s".format(brokerIds.mkString(",")))
      partitionsAssignedToThisBroker.map(p => new PartitionAndReplica(p._1, p._2, brokerId))
    }.flatten
  }
  
  def getPartitionsUndergoingPreferredReplicaElection(zkClient: ZkClient): Set[TopicAndPartition] = {
    // read the partitions and their new replica list
    val jsonPartitionListOpt = readDataMaybeNull(zkClient, PreferredReplicaLeaderElectionPath)._1
    jsonPartitionListOpt match {
      case Some(jsonPartitionList) => parsePreferredReplicaElectionData(jsonPartitionList)
      case None => Set.empty[TopicAndPartition]
    }
  }

  def parsePreferredReplicaElectionData(jsonData: String):Set[TopicAndPartition] = {
    SyncJSON.parseFull(jsonData) match {
      case Some(m) =>
        val topicAndPartitions = m.asInstanceOf[Array[Map[String, String]]]
        val partitions = topicAndPartitions.map { p =>
          val topicPartitionMap = p.asInstanceOf[Map[String, String]]
          val topic = topicPartitionMap.get("topic").get
          val partition = topicPartitionMap.get("partition").get.toInt
          TopicAndPartition(topic, partition)
        }
        Set.empty[TopicAndPartition] ++ partitions
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

  def getConsumersPerTopic(zkClient: ZkClient, group: String) : mutable.Map[String, List[String]] = {
    val dirs = new ZKGroupDirs(group)
    val consumers = getChildrenParentMayNotExist(zkClient, dirs.consumerRegistryDir)
    val consumersPerTopicMap = new mutable.HashMap[String, List[String]]
    for (consumer <- consumers) {
      val topicCount = TopicCount.constructTopicCount(group, consumer, zkClient)
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
   * @returns An optional Broker object encapsulating the broker metadata
   */
  def getBrokerInfo(zkClient: ZkClient, brokerId: Int): Option[Broker] = {
    ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
      case Some(brokerInfo) => Some(Broker.createBroker(brokerId, brokerInfo))
      case None => None
    }
  }

  def getAllTopics(zkClient: ZkClient): Seq[String] = {
    val topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath)
    if(topics == null) Seq.empty[String]
    else topics
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

class LeaderExistsOrChangedListener(topic: String,
                                    partition: Int,
                                    leaderLock: ReentrantLock,
                                    leaderExistsOrChanged: Condition,
                                    oldLeaderOpt: Option[Int] = None,
                                    zkClient: ZkClient = null) extends IZkDataListener with Logging {
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    val t = dataPath.split("/").takeRight(3).head
    val p = dataPath.split("/").takeRight(2).head.toInt
    leaderLock.lock()
    try {
      if(t == topic && p == partition){
        if(oldLeaderOpt == None){
          trace("In leader existence listener on partition [%s, %d], leader has been created".format(topic, partition))
          leaderExistsOrChanged.signal()
        }
        else {
          val newLeaderOpt = ZkUtils.getLeaderForPartition(zkClient, t, p)
          if(newLeaderOpt.isDefined && newLeaderOpt.get != oldLeaderOpt.get){
            trace("In leader change listener on partition [%s, %d], leader has been moved from %d to %d".format(topic, partition, oldLeaderOpt.get, newLeaderOpt.get))
            leaderExistsOrChanged.signal()
          }
        }
      }
    }
    finally {
      leaderLock.unlock()
    }
  }

  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
    leaderLock.lock()
    try {
      leaderExistsOrChanged.signal()
    }finally {
      leaderLock.unlock()
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
  val zkConnect = props.getString("zk.connect", null)

  /** zookeeper session timeout */
  val zkSessionTimeoutMs = props.getInt("zk.sessiontimeout.ms", 6000)

  /** the max time that the client waits to establish a connection to zookeeper */
  val zkConnectionTimeoutMs = props.getInt("zk.connectiontimeout.ms",zkSessionTimeoutMs)

  /** how far a ZK follower can be behind a ZK leader */
  val zkSyncTimeMs = props.getInt("zk.synctime.ms", 2000)
}
