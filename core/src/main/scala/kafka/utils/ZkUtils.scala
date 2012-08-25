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
import kafka.api.LeaderAndISR
import kafka.common.NoEpochForPartitionException
import org.apache.zookeeper.data.Stat
import java.util.concurrent.locks.{ReentrantLock, Condition}

object ZkUtils extends Logging {
  val ConsumersPath = "/consumers"
  val BrokerIdsPath = "/brokers/ids"
  val BrokerTopicsPath = "/brokers/topics"
  val ControllerPath = "/controller"

  def getTopicPath(topic: String): String ={
    BrokerTopicsPath + "/" + topic
  }

  def getTopicPartitionsPath(topic: String): String ={
    getTopicPath(topic) + "/partitions"
  }

  def getController(zkClient: ZkClient): Int= {
    val controller = readDataMaybeNull(zkClient, ControllerPath)._1
    controller.toInt
  }

  def getTopicPartitionPath(topic: String, partitionId: Int): String ={
    getTopicPartitionsPath(topic) + "/" + partitionId
  }

  def getTopicPartitionLeaderAndISRPath(topic: String, partitionId: Int): String ={
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
    getBrokerInfoFromIds(zkClient, brokerIds.map(_.toInt))
  }


  def getLeaderAndISRForPartition(zkClient: ZkClient, topic: String, partition: Int):Option[LeaderAndISR] = {
    val leaderAndISRPath = getTopicPartitionLeaderAndISRPath(topic, partition)
    val ret = readDataMaybeNull(zkClient, leaderAndISRPath)
    val leaderAndISRStr: String = ret._1
    val stat = ret._2
    if(leaderAndISRStr == null) None
    else {
      SyncJSON.parseFull(leaderAndISRStr) match {
        case Some(m) =>
          val leader = m.asInstanceOf[Map[String, String]].get("leader").get.toInt
          val epoch = m.asInstanceOf[Map[String, String]].get("leaderEpoch").get.toInt
          val ISRString = m.asInstanceOf[Map[String, String]].get("ISR").get
          val ISR = Utils.getCSVList(ISRString).map(r => r.toInt)
          val zkPathVersion = stat.getVersion
          debug("Leader %d, Epoch %d, isr %s, zk path version %d for topic %s and partition %d".format(leader, epoch, ISR.toString(), zkPathVersion, topic, partition))
          Some(LeaderAndISR(leader, epoch, ISR.toList, zkPathVersion))
        case None => None
      }
    }
  }


  def getLeaderForPartition(zkClient: ZkClient, topic: String, partition: Int): Option[Int] = {
    val leaderAndISR = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndISRPath(topic, partition))._1
    if(leaderAndISR == null) None
    else {
      SyncJSON.parseFull(leaderAndISR) match {
        case Some(m) =>
          Some(m.asInstanceOf[Map[String, String]].get("leader").get.toInt)
        case None => None
      }
    }
  }

  /**
   * This API should read the epoch in the ISR path. It is sufficient to read the epoch in the ISR path, since if the
   * leader fails after updating epoch in the leader path and before updating epoch in the ISR path, effectively some
   * other broker will retry becoming leader with the same new epoch value.
   */
  def getEpochForPartition(zkClient: ZkClient, topic: String, partition: Int): Int = {
    val leaderAndISR = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndISRPath(topic, partition))._1
    if(leaderAndISR != null) {
      val epoch = SyncJSON.parseFull(leaderAndISR) match {
        case None => throw new NoEpochForPartitionException("No epoch, leaderAndISR data for topic %s partition %d is invalid".format(topic, partition))
        case Some(m) =>
          m.asInstanceOf[Map[String, String]].get("leaderEpoch").get.toInt
      }
      epoch
    }
    else
      throw new NoEpochForPartitionException("No epoch, ISR path for topic %s partition %d is empty".format(topic, partition))
  }

  /**
   * Gets the in-sync replicas (ISR) for a specific topic and partition
   */
  def getInSyncReplicasForPartition(zkClient: ZkClient, topic: String, partition: Int): Seq[Int] = {
    val leaderAndISR = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndISRPath(topic, partition))._1
    if(leaderAndISR == null) Seq.empty[Int]
    else {
      SyncJSON.parseFull(leaderAndISR) match {
        case Some(m) =>
          val ISRString = m.asInstanceOf[Map[String, String]].get("ISR").get
          Utils.getCSVList(ISRString).map(r => r.toInt)
        case None => Seq.empty[Int]
      }
    }
  }

  /**
   * Gets the assigned replicas (AR) for a specific topic and partition
   */
  def getReplicasForPartition(zkClient: ZkClient, topic: String, partition: Int): Seq[Int] = {
    val jsonPartitionMap = readDataMaybeNull(zkClient, getTopicPath(topic))._1
    val assignedReplicas = if (jsonPartitionMap == null) {
      Seq.empty[Int]
    } else {
      SyncJSON.parseFull(jsonPartitionMap) match {
        case Some(m) => m.asInstanceOf[Map[String, List[String]]].get(partition.toString) match {
          case None => Seq.empty[Int]
          case Some(seq) => seq.map(_.toInt)
        }
        case None => Seq.empty[Int]
      }
    }
    assignedReplicas
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
      info("Conditional update the zkPath %s with expected version %d succeed and return the new version: %d".format(path, expectVersion, stat.getVersion))
      (true, stat.getVersion)
    } catch {
      case e: Exception =>
        info("Conditional update the zkPath %s with expected version %d failed".format(path, expectVersion))
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

  def readDataMaybeNull(client: ZkClient, path: String): (String, Stat) = {
    val stat: Stat = new Stat()
    var dataStr: String = null
    try{
      dataStr = client.readData(path, stat)
      return (dataStr, stat)
    } catch {
      case e: ZkNoNodeException =>
        return (null, stat)
      case e2 => throw e2
    }
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

  def getReplicaAssignmentForTopics(zkClient: ZkClient, topics: Iterator[String]): mutable.Map[(String, Int), Seq[Int]] = {
    val ret = new mutable.HashMap[(String, Int), Seq[Int]]
    topics.foreach{ topic =>
      val jsonPartitionMap = readDataMaybeNull(zkClient, getTopicPath(topic))._1
      if (jsonPartitionMap != null) {
        SyncJSON.parseFull(jsonPartitionMap) match {
          case Some(m) =>
            val replicaMap = m.asInstanceOf[Map[String, Seq[String]]]
            for((partition, replicas) <- replicaMap){
              ret.put((topic, partition.toInt), replicas.map(_.toInt))
              debug("Replicas assigned to topic [%s], partition [%s] are [%s]".format(topic, partition, replicas))
            }
          case None =>
        }
      }
                  }
    ret
  }

  def getPartitionLeaderAndISRForTopics(zkClient: ZkClient, topics: Iterator[String]): mutable.Map[(String, Int), LeaderAndISR] = {
    val ret = new mutable.HashMap[(String, Int), LeaderAndISR]
    val partitionsForTopics = getPartitionsForTopics(zkClient, topics)
    for((topic, partitions) <- partitionsForTopics){
      for(partition <- partitions){
        val leaderAndISROpt = ZkUtils.getLeaderAndISRForPartition(zkClient, topic, partition.toInt)
        if(leaderAndISROpt.isDefined)
          ret.put((topic, partition.toInt), leaderAndISROpt.get)
      }
    }
    ret
  }

  def getPartitionAssignmentForTopics(zkClient: ZkClient, topics: Iterator[String]): mutable.Map[String, collection.Map[Int, Seq[Int]]] = {
    val ret = new mutable.HashMap[String, Map[Int, Seq[Int]]]()
    topics.foreach{ topic =>
      val jsonPartitionMap = readDataMaybeNull(zkClient, getTopicPath(topic))._1
      val partitionMap = if (jsonPartitionMap == null) {
        Map[Int, Seq[Int]]()
      } else {
        SyncJSON.parseFull(jsonPartitionMap) match {
          case Some(m) =>
            val m1 = m.asInstanceOf[Map[String, Seq[String]]]
            m1.map(p => (p._1.toInt, p._2.map(_.toInt)))
          case None => Map[Int, Seq[Int]]()
        }
      }
      debug("partition map for /brokers/topics/%s is %s".format(topic, partitionMap))
      ret += (topic -> partitionMap)
                  }
    ret
  }

  def getReplicaAssignmentFromPartitionAssignment(topicPartitionAssignment: mutable.Map[String, collection.Map[Int, Seq[Int]]]): mutable.Map[(String, Int), Seq[Int]] = {
    val ret = new mutable.HashMap[(String, Int), Seq[Int]]
    for((topic, partitionAssignment) <- topicPartitionAssignment){
      for((partition, replicaAssignment) <- partitionAssignment){
        ret.put((topic, partition), replicaAssignment)
      }
    }
    ret
  }

  def getPartitionsForTopics(zkClient: ZkClient, topics: Iterator[String]): mutable.Map[String, Seq[Int]] = {
    getPartitionAssignmentForTopics(zkClient, topics).map
    { topicAndPartitionMap =>
      val topic = topicAndPartitionMap._1
      val partitionMap = topicAndPartitionMap._2
      debug("partition assignment of /brokers/topics/%s is %s".format(topic, partitionMap))
      (topic -> partitionMap.keys.toSeq.sortWith((s,t) => s < t))
    }
  }

  def getPartitionsAssignedToBroker(zkClient: ZkClient, topics: Seq[String], brokerId: Int): Map[(String, Int), Seq[Int]] = {
    val ret = new mutable.HashMap[(String, Int), Seq[Int]]
    val topicsAndPartitions = getPartitionAssignmentForTopics(zkClient, topics.iterator)
    topicsAndPartitions.map
    {
      topicAndPartitionMap =>
        val topic = topicAndPartitionMap._1
        val partitionMap = topicAndPartitionMap._2
        val relevantPartitionsMap = partitionMap.filter( m => m._2.contains(brokerId) )
        for((relevantPartition, replicaAssignment) <- relevantPartitionsMap){
          ret.put((topic, relevantPartition), replicaAssignment)
        }
    }
    ret
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

  def getConsumerTopicMaps(zkClient: ZkClient, group: String): Map[String, TopicCount] = {
    val dirs = new ZKGroupDirs(group)
    val consumersInGroup = getConsumersInGroup(zkClient, group)
    val topicCountMaps = consumersInGroup.map(consumerId => TopicCount.constructTopicCount(consumerId,ZkUtils.readData(zkClient, dirs.consumerRegistryDir + "/" + consumerId)._1, zkClient))
    consumersInGroup.zip(topicCountMaps).toMap
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

  def getBrokerInfoFromIds(zkClient: ZkClient, brokerIds: Seq[Int]): Seq[Broker] = brokerIds.map( bid => Broker.createBroker(bid, ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + bid)._1))

  def getAllTopics(zkClient: ZkClient): Seq[String] = {
    val topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath)
    if(topics == null) Seq.empty[String]
    else topics
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
