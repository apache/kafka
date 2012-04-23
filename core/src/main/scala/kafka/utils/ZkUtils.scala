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

import org.I0Itec.zkclient.serialize.ZkSerializer
import kafka.cluster.{Broker, Cluster}
import scala.collection._
import java.util.Properties
import org.I0Itec.zkclient.exception.{ZkNodeExistsException, ZkNoNodeException, ZkMarshallingError}
import kafka.consumer.TopicCount
import org.I0Itec.zkclient.{IZkDataListener, ZkClient}
import java.util.concurrent.locks.Condition
import kafka.common.NoEpochForPartitionException

object ZkUtils extends Logging {
  val ConsumersPath = "/consumers"
  val BrokerIdsPath = "/brokers/ids"
  val BrokerTopicsPath = "/brokers/topics"
  val BrokerStatePath = "/brokers/state"

  def getTopicPath(topic: String): String ={
    BrokerTopicsPath + "/" + topic
  }

  def getTopicPartitionsPath(topic: String): String ={
    getTopicPath(topic) + "/partitions"
  }

  def getTopicPartitionPath(topic: String, partitionId: String): String ={
    getTopicPartitionsPath(topic) + "/" + partitionId
  }

  def getTopicVersion(zkClient: ZkClient, topic: String): String ={
    readDataMaybeNull(zkClient, getTopicPath(topic))
  }

  def getTopicPartitionReplicasPath(topic: String, partitionId: String): String ={
    getTopicPartitionPath(topic, partitionId) + "/" + "replicas"
  }

  def getTopicPartitionInSyncPath(topic: String, partitionId: String): String ={
    getTopicPartitionPath(topic, partitionId) + "/" + "isr"
  }

  def getTopicPartitionLeaderPath(topic: String, partitionId: String): String ={
    getTopicPartitionPath(topic, partitionId) + "/" + "leader"
  }

  def getBrokerStateChangePath(brokerId: Int): String = {
    BrokerStatePath + "/" + brokerId
  }

  def getSortedBrokerList(zkClient: ZkClient): Seq[String] ={
      ZkUtils.getChildren(zkClient, ZkUtils.BrokerIdsPath).sorted
  }

  def getAllBrokersInCluster(zkClient: ZkClient): Seq[Broker] = {
    val brokerIds = ZkUtils.getChildren(zkClient, ZkUtils.BrokerIdsPath).sorted
    getBrokerInfoFromIds(zkClient, brokerIds.map(b => b.toInt))
  }

  def getLeaderForPartition(zkClient: ZkClient, topic: String, partition: Int): Option[Int] = {
    val leaderAndEpoch = readDataMaybeNull(zkClient, getTopicPartitionLeaderPath(topic, partition.toString))
    if(leaderAndEpoch == null) None
    else {
      val leaderAndEpochInfo = leaderAndEpoch.split(";")
      Some(leaderAndEpochInfo.head.toInt)
    }
  }

  /**
   * This API should read the epoch in the ISR path. It is sufficient to read the epoch in the ISR path, since if the
   * leader fails after updating epoch in the leader path and before updating epoch in the ISR path, effectively some
   * other broker will retry becoming leader with the same new epoch value.
   */
  def getEpochForPartition(client: ZkClient, topic: String, partition: Int): Int = {
    val lastKnownEpoch = try {
      val isrAndEpoch = readData(client, getTopicPartitionInSyncPath(topic, partition.toString))
      if(isrAndEpoch != null) {
        val isrAndEpochInfo = isrAndEpoch.split(";")
        if(isrAndEpochInfo.last.isEmpty)
          throw new NoEpochForPartitionException("No epoch in ISR path for topic %s partition %d is empty".format(topic, partition))
        else
          isrAndEpochInfo.last.toInt
      }else {
        throw new NoEpochForPartitionException("ISR path for topic %s partition %d is empty".format(topic, partition))
      }
    }catch {
      case e: ZkNoNodeException =>
        throw new NoEpochForPartitionException("No epoch since leader never existed for topic %s partition %d".format(topic, partition))
      case e1 => throw e1
    }
    lastKnownEpoch
  }

  def getReplicasForPartition(zkClient: ZkClient, topic: String, partition: Int): Seq[String] = {
    val replicaListString = readDataMaybeNull(zkClient, getTopicPartitionReplicasPath(topic, partition.toString))
    if(replicaListString == null)
      Seq.empty[String]
    else {
      Utils.getCSVList(replicaListString)
    }
  }

  def getInSyncReplicasForPartition(client: ZkClient, topic: String, partition: Int): Seq[Int] = {
    val replicaListAndEpochString = readDataMaybeNull(client, getTopicPartitionInSyncPath(topic, partition.toString))
    if(replicaListAndEpochString == null)
      Seq.empty[Int]
    else {
      val replicasAndEpochInfo = replicaListAndEpochString.split(";")
      Utils.getCSVList(replicasAndEpochInfo.head).map(r => r.toInt)
    }
  }

  def isPartitionOnBroker(zkClient: ZkClient, topic: String, partition: Int, brokerId: Int): Boolean = {
    val replicas = getReplicasForPartition(zkClient, topic, partition)
    debug("The list of replicas for topic %s, partition %d is %s".format(topic, partition, replicas))
    replicas.contains(brokerId.toString)
  }

  def tryToBecomeLeaderForPartition(client: ZkClient, topic: String, partition: Int, brokerId: Int): Option[Int] = {
    try {
      // NOTE: first increment epoch, then become leader
      val newEpoch = incrementEpochForPartition(client, topic, partition, brokerId)
      createEphemeralPathExpectConflict(client, getTopicPartitionLeaderPath(topic, partition.toString),
        "%d;%d".format(brokerId, newEpoch))
      val currentISR = getInSyncReplicasForPartition(client, topic, partition)
      updatePersistentPath(client, getTopicPartitionInSyncPath(topic, partition.toString),
        "%s;%d".format(currentISR.mkString(","), newEpoch))
      info("Elected broker %d with epoch %d to be leader for topic %s partition %d".format(brokerId, newEpoch, topic, partition))
      Some(newEpoch)
    } catch {
      case e: ZkNodeExistsException => error("Leader exists for topic %s partition %d".format(topic, partition)); None
      case oe => None
    }
  }

  def incrementEpochForPartition(client: ZkClient, topic: String, partition: Int, leader: Int) = {
    // read previous epoch, increment it and write it to the leader path and the ISR path.
    val epoch = try {
      Some(getEpochForPartition(client, topic, partition))
    }catch {
      case e: NoEpochForPartitionException => None
      case e1 => throw e1
    }

    val newEpoch = epoch match {
      case Some(partitionEpoch) =>
        debug("Existing epoch for topic %s partition %d is %d".format(topic, partition, epoch))
        partitionEpoch + 1
      case None =>
        // this is the first time leader is elected for this partition. So set epoch to 1
        debug("First epoch is 1 for topic %s partition %d".format(topic, partition))
        1
    }
    newEpoch
  }

  def registerBrokerInZk(zkClient: ZkClient, id: Int, host: String, creator: String, port: Int) {
    val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + id
    val broker = new Broker(id, creator, host, port)
    try {
      createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString)
    } catch {
      case e: ZkNodeExistsException =>
        throw new RuntimeException("A broker is already registered on the path " + brokerIdPath + ". This probably " +
                                   "indicates that you either have configured a brokerid that is already in use, or " +
                                   "else you have shutdown this broker and restarted it faster than the zookeeper " +
                                   "timeout so it appears to be re-registering.")
    }
    info("Registering broker " + brokerIdPath + " succeeded with " + broker)
  }

  def getConsumerPartitionOwnerPath(group: String, topic: String, partition: String): String = {
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
    }
    catch {
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
    }
    catch {
      case e: ZkNodeExistsException => {
        // this can happen when there is connection loss; make sure the data is what we intend to write
        var storedData: String = null
        try {
          storedData = readData(client, path)
        }
        catch {
          case e1: ZkNoNodeException => // the node disappeared; treat as if node existed and let caller handles this
          case e2 => throw e2
        }
        if (storedData == null || storedData != data) {
          info("conflict in " + path + " data: " + data + " stored data: " + storedData)
          throw e
        }
        else {
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
    }
    catch {
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
   */
  def updatePersistentPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.writeData(path, data)
    }
    catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        try {
          client.createPersistent(path, data)
        }
        catch {
          case e: ZkNodeExistsException => client.writeData(path, data)
          case e2 => throw e2
        }
      }
      case e2 => throw e2
    }
  }

  /**
   * Update the value of a persistent node with the given path and data.
   * create parrent directory if necessary. Never throw NodeExistException.
   */
  def updateEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.writeData(path, data)
    }
    catch {
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
    }
    catch {
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
    }
    catch {
      case e: ZkNoNodeException =>
        // this can happen during a connection loss event, return normally
        info(path + " deleted during connection loss; this is ok")
      case e2 => throw e2
    }
  }

  def readData(client: ZkClient, path: String): String = {
    client.readData(path)
  }

  def readDataMaybeNull(client: ZkClient, path: String): String = {
    client.readData(path, true)
  }

  def getChildren(client: ZkClient, path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    // triggers implicit conversion from java list to scala Seq
    client.getChildren(path)
  }

  def getChildrenParentMayNotExist(client: ZkClient, path: String): Seq[String] = {
    import scala.collection.JavaConversions._
    // triggers implicit conversion from java list to scala Seq

    var ret: java.util.List[String] = null
    try {
      ret = client.getChildren(path)
    }
    catch {
      case e: ZkNoNodeException =>
        return Nil
      case e2 => throw e2
    }
    return ret
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
      val brokerZKString = readData(zkClient, BrokerIdsPath + "/" + node)
      cluster.add(Broker.createBroker(node.toInt, brokerZKString))
    }
    cluster
  }

  def getPartitionsForTopics(zkClient: ZkClient, topics: Iterator[String]): mutable.Map[String, Seq[String]] = {
    val ret = new mutable.HashMap[String, Seq[String]]()
    topics.foreach { topic =>
      // get the partitions that exist for topic
      val partitions = getChildrenParentMayNotExist(zkClient, getTopicPartitionsPath(topic))
      debug("children of /brokers/topics/%s are %s".format(topic, partitions))
      ret += (topic -> partitions.sortWith((s,t) => s < t))
    }
    ret
  }

  def getPartitionsAssignedToBroker(zkClient: ZkClient, topics: Seq[String], brokerId: Int): Map[String, Seq[Int]] = {
    val topicsAndPartitions = getPartitionsForTopics(zkClient, topics.iterator)

    topicsAndPartitions.map { tp =>
      val topic = tp._1
      val partitions = tp._2.map(p => p.toInt)
      val relevantPartitions = partitions.filter { partition =>
        val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partition).map(r => r.toInt)
        assignedReplicas.contains(brokerId)
      }
      (topic -> relevantPartitions)
    }
  }

  def getPartitionsAssignedToBroker(zkClient: ZkClient, topic: String, partitions: Seq[Int], broker: Int): Seq[Int] = {
    partitions.filter { p =>
      val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, p).map(r => r.toInt)
      assignedReplicas.contains(broker)
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

  def getTopicCount(zkClient: ZkClient, group: String, consumerId: String) : TopicCount = {
    val dirs = new ZKGroupDirs(group)
    val topicCountJson = ZkUtils.readData(zkClient, dirs.consumerRegistryDir + "/" + consumerId)
    TopicCount.constructTopicCount(consumerId, topicCountJson)
  }

  def getConsumerTopicMaps(zkClient: ZkClient, group: String): Map[String, TopicCount] = {
    val dirs = new ZKGroupDirs(group)
    val consumersInGroup = getConsumersInGroup(zkClient, group)
    val topicCountMaps = consumersInGroup.map(consumerId => TopicCount.constructTopicCount(consumerId,
      ZkUtils.readData(zkClient, dirs.consumerRegistryDir + "/" + consumerId)))
    consumersInGroup.zip(topicCountMaps).toMap
  }

  def getConsumersPerTopic(zkClient: ZkClient, group: String) : mutable.Map[String, List[String]] = {
    val dirs = new ZKGroupDirs(group)
    val consumers = getChildrenParentMayNotExist(zkClient, dirs.consumerRegistryDir)
    val consumersPerTopicMap = new mutable.HashMap[String, List[String]]
    for (consumer <- consumers) {
      val topicCount = getTopicCount(zkClient, group, consumer)
      for ((topic, consumerThreadIdSet) <- topicCount.getConsumerThreadIdsPerTopic()) {
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
   * For a given topic, this returns the sorted list of partition ids registered for this topic
   */
  def getSortedPartitionIdsForTopic(zkClient: ZkClient, topic: String): Seq[Int] = {
    val topicPartitionsPath = ZkUtils.getTopicPartitionsPath(topic)
    ZkUtils.getChildrenParentMayNotExist(zkClient, topicPartitionsPath).map(pid => pid.toInt).sortWith((s,t) => s < t)
  }

  def getBrokerInfoFromIds(zkClient: ZkClient, brokerIds: Seq[Int]): Seq[Broker] =
    brokerIds.map( bid => Broker.createBroker(bid, ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + bid)) )

  def getAllTopics(zkClient: ZkClient): Seq[String] = {
    val topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath)
    if(topics == null) Seq.empty[String]
    else topics
  }

}

class LeaderExists(topic: String, partition: Int, leaderExists: Condition) extends IZkDataListener {
  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    val t = dataPath.split("/").takeRight(3).head
    val p = dataPath.split("/").takeRight(2).head.toInt
    if(t == topic && p == partition)
      leaderExists.signal()
  }

  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
    leaderExists.signal()
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


class ZKConfig(props: Properties) {
  /** ZK host string */
  val zkConnect = Utils.getString(props, "zk.connect", null)

  /** zookeeper session timeout */
  val zkSessionTimeoutMs = Utils.getInt(props, "zk.sessiontimeout.ms", 6000)

  /** the max time that the client waits to establish a connection to zookeeper */
  val zkConnectionTimeoutMs = Utils.getInt(props, "zk.connectiontimeout.ms",zkSessionTimeoutMs)

  /** how far a ZK follower can be behind a ZK leader */
  val zkSyncTimeMs = Utils.getInt(props, "zk.synctime.ms", 2000)
}
