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

package kafka.server

import java.net.InetAddress
import kafka.cluster.Replica
import kafka.utils._
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.I0Itec.zkclient.{IZkDataListener, IZkChildListener, IZkStateListener, ZkClient}
import kafka.admin.AdminUtils
import java.lang.{Thread, IllegalStateException}
import collection.mutable.HashSet
import kafka.common.{InvalidPartitionException, NoLeaderForPartitionException, NotLeaderForPartitionException, KafkaZookeeperClient}

/**
 * Handles the server's interaction with zookeeper. The server needs to register the following paths:
 *   /topics/[topic]/[node_id-partition_num]
 *   /brokers/[0...N] --> host:port
 *
 */
class KafkaZooKeeper(config: KafkaConfig,
                     addReplicaCbk: (String, Int, Set[Int]) => Replica,
                     getReplicaCbk: (String, Int) => Option[Replica],
                     becomeLeader: (Replica, Seq[Int]) => Unit,
                     becomeFollower: (Replica, Int, ZkClient) => Unit) extends Logging {

  val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + config.brokerId
  private var zkClient: ZkClient = null
  private val leaderChangeListener = new LeaderChangeListener
  private val topicPartitionsChangeListener = new TopicChangeListener
  private var stateChangeHandler: StateChangeCommandHandler = null

  private val topicListenerLock = new Object
  private val leaderChangeLock = new Object

  def startup() {
    /* start client */
    info("connecting to ZK: " + config.zkConnect)
    zkClient = KafkaZookeeperClient.getZookeeperClient(config)
    startStateChangeCommandHandler()
    zkClient.subscribeStateChanges(new SessionExpireListener)
    registerBrokerInZk()
    subscribeToTopicAndPartitionsChanges(true)
  }

  private def registerBrokerInZk() {
    info("Registering broker " + brokerIdPath)
    val hostName = if (config.hostName == null) InetAddress.getLocalHost.getHostAddress else config.hostName
    val creatorId = hostName + "-" + System.currentTimeMillis
    ZkUtils.registerBrokerInZk(zkClient, config.brokerId, hostName, creatorId, config.port)
  }

  private def startStateChangeCommandHandler() {
    val stateChangeQ = new ZkQueue(zkClient, ZkUtils.getBrokerStateChangePath(config.brokerId), config.stateChangeQSize)
    stateChangeHandler = new StateChangeCommandHandler("StateChangeCommandHandler", config, stateChangeQ,
      ensureStateChangeCommandValidityOnThisBroker, ensureEpochValidity)
    stateChangeHandler.start()
  }

  /**
   *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
   *  connection for us. We need to re-register this broker in the broker registry.
   */
  class SessionExpireListener() extends IZkStateListener {
    @throws(classOf[Exception])
    def handleStateChanged(state: KeeperState) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleNewSession() {
      info("re-registering broker info in ZK for broker " + config.brokerId)
      registerBrokerInZk()
      info("done re-registering broker")
      info("Subscribing to %s path to watch for new topics".format(ZkUtils.BrokerTopicsPath))
      zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicPartitionsChangeListener)
      val topics = ZkUtils.getAllTopics(zkClient)
      debug("Existing topics are %s".format(topics.mkString(",")))
      topics.foreach(topic => zkClient.subscribeChildChanges(ZkUtils.getTopicPartitionsPath(topic), topicPartitionsChangeListener))
      handleNewTopics(topics)
    }
  }

  def close() {
    if (zkClient != null) {
      stateChangeHandler.shutdown()
      info("Closing zookeeper client...")
      zkClient.close()
    }
  }

  def ensurePartitionLeaderOnThisBroker(topic: String, partition: Int) {
    // TODO: KAFKA-352 first check if this topic exists in the cluster
//    if(!topicPartitionsChangeListener.doesTopicExistInCluster(topic))
//      throw new UnknownTopicException("Topic %s doesn't exist in the cluster".format(topic))
    // check if partition id is invalid
    if(partition < 0)
      throw new InvalidPartitionException("Partition %d is invalid".format(partition))
    ZkUtils.getLeaderForPartition(zkClient, topic, partition) match {
      case Some(leader) =>
        if(leader != config.brokerId)
          throw new NotLeaderForPartitionException("Broker %d is not leader for partition %d for topic %s"
            .format(config.brokerId, partition, topic))
      case None =>
        throw new NoLeaderForPartitionException("There is no leader for topic %s partition %d".format(topic, partition))
    }
  }

  def getZookeeperClient = zkClient

  def handleNewTopics(topics: Seq[String]) {
    // get relevant partitions to this broker
    val topicsAndPartitionsOnThisBroker = ZkUtils.getPartitionsAssignedToBroker(zkClient, topics, config.brokerId)
    debug("Partitions assigned to broker %d are %s".format(config.brokerId, topicsAndPartitionsOnThisBroker.mkString(",")))
    for( (topic, partitionsAssignedToThisBroker) <- topicsAndPartitionsOnThisBroker ) {
      // subscribe to leader changes for these partitions
      subscribeToLeaderForPartitions(topic, partitionsAssignedToThisBroker)
      // start replicas for these partitions
      startReplicasForPartitions(topic, partitionsAssignedToThisBroker)
    }
  }

  def subscribeToTopicAndPartitionsChanges(startReplicas: Boolean) {
    info("Subscribing to %s path to watch for new topics".format(ZkUtils.BrokerTopicsPath))
    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicPartitionsChangeListener)
    val topics = ZkUtils.getAllTopics(zkClient)
    val topicsAndPartitionsOnThisBroker = ZkUtils.getPartitionsAssignedToBroker(zkClient, topics, config.brokerId)
    debug("Partitions assigned to broker %d are %s".format(config.brokerId, topicsAndPartitionsOnThisBroker.mkString(",")))
    for( (topic, partitionsAssignedToThisBroker) <- topicsAndPartitionsOnThisBroker ) {
      // subscribe to leader changes for these partitions
      subscribeToLeaderForPartitions(topic, partitionsAssignedToThisBroker)

      // start replicas for these partitions
      if(startReplicas)
        startReplicasForPartitions(topic, partitionsAssignedToThisBroker)
    }
  }

  private def subscribeToLeaderForPartitions(topic: String, partitions: Seq[Int]) {
    partitions.foreach { partition =>
      info("Broker %d subscribing to leader changes for topic %s partition %d".format(config.brokerId, topic, partition))
      // register leader change listener
      zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderPath(topic, partition.toString), leaderChangeListener)
    }
  }

  private def startReplicasForPartitions(topic: String, partitions: Seq[Int]) {
    partitions.foreach { partition =>
      val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partition).map(r => r.toInt)
      info("Assigned replicas list for topic %s partition %d is %s".format(topic, partition, assignedReplicas.mkString(",")))
      if(assignedReplicas.contains(config.brokerId)) {
        val replica = addReplicaCbk(topic, partition, assignedReplicas.toSet)
        startReplica(replica)
      } else
        warn("Ignoring partition %d of topic %s since broker %d doesn't host any replicas for it"
          .format(partition, topic, config.brokerId))
    }
  }

  private def startReplica(replica: Replica) {
    info("Starting replica for topic %s partition %d on broker %d"
      .format(replica.topic, replica.partition.partitionId, replica.brokerId))
    ZkUtils.getLeaderForPartition(zkClient, replica.topic, replica.partition.partitionId) match {
      case Some(leader) =>
        info("Topic %s partition %d has leader %d".format(replica.topic, replica.partition.partitionId,leader))
        // check if this broker is the leader, if not, then become follower
        if(leader != config.brokerId)
          becomeFollower(replica, leader, zkClient)
      case None => // leader election
        leaderElection(replica)
    }
  }

  def leaderElection(replica: Replica) {
    info("Broker %d electing leader for topic %s partition %d".format(config.brokerId, replica.topic, replica.partition.partitionId))
    // read the AR list for replica.partition from ZK
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, replica.topic, replica.partition.partitionId).map(_.toInt)
    val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, replica.topic, replica.partition.partitionId)
    val liveBrokers = ZkUtils.getSortedBrokerList(zkClient).map(_.toInt)
    if(canBecomeLeader(config.brokerId, replica.topic, replica.partition.partitionId, assignedReplicas, inSyncReplicas, liveBrokers)) {
      info("Broker %d will participate in leader election for topic %s partition %d"
        .format(config.brokerId, replica.topic, replica.partition.partitionId))
      // wait for some time if it is not the preferred replica
      try {
        if(replica.brokerId != assignedReplicas.head) {
          // sleep only if the preferred replica is alive
          if(liveBrokers.contains(assignedReplicas.head)) {
            info("Preferred replica %d for topic %s ".format(assignedReplicas.head, replica.topic) +
              "partition %d is alive. Waiting for %d ms to allow it to become leader"
              .format(replica.partition.partitionId, config.preferredReplicaWaitTime))
            Thread.sleep(config.preferredReplicaWaitTime)
          }
        }
      } catch {
        case e => // ignoring
      }
      val newLeaderEpochAndISR = ZkUtils.tryToBecomeLeaderForPartition(zkClient, replica.topic,
        replica.partition.partitionId, replica.brokerId)
      newLeaderEpochAndISR match {
        case Some(epochAndISR) =>
          info("Broker %d is leader for topic %s partition %d".format(replica.brokerId, replica.topic,
            replica.partition.partitionId))
          info("Current ISR for topic %s partition %d is %s".format(replica.topic, replica.partition.partitionId,
                                                                    epochAndISR._2.mkString(",")))
          becomeLeader(replica, epochAndISR._2)
        case None =>
          ZkUtils.getLeaderForPartition(zkClient, replica.topic, replica.partition.partitionId) match {
            case Some(leader) =>
              becomeFollower(replica, leader, zkClient)
            case None =>
              error("Lost leader for topic %s partition %d right after leader election".format(replica.topic,
                replica.partition.partitionId))
          }
      }
    }
  }

  private def canBecomeLeader(brokerId: Int, topic: String, partition: Int, assignedReplicas: Seq[Int],
                              inSyncReplicas: Seq[Int], liveBrokers: Seq[Int]): Boolean = {
    // TODO: raise alert, mark the partition offline if no broker in the assigned replicas list is alive
    assert(assignedReplicas.size > 0, "There should be at least one replica in the assigned replicas list for topic " +
      " %s partition %d".format(topic, partition))
    inSyncReplicas.size > 0 match {
      case true => // check if this broker is in the ISR. If yes, return true
        inSyncReplicas.contains(brokerId) match {
          case true =>
            info("Broker %d can become leader since it is in the ISR %s".format(brokerId, inSyncReplicas.mkString(",")) +
              " for topic %s partition %d".format(topic, partition))
            true
          case false =>
            // check if any broker in the ISR is alive. If not, return true only if this broker is in the AR
            val liveBrokersInISR = inSyncReplicas.filter(r => liveBrokers.contains(r))
            liveBrokersInISR.isEmpty match {
              case true =>
                if(assignedReplicas.contains(brokerId)) {
                  info("No broker in the ISR %s for topic %s".format(inSyncReplicas.mkString(","), topic) +
                    " partition %d is alive. Broker %d can become leader since it is in the assigned replicas %s"
                      .format(partition, brokerId, assignedReplicas.mkString(",")))
                  true
                } else {
                  info("No broker in the ISR %s for topic %s".format(inSyncReplicas.mkString(","), topic) +
                    " partition %d is alive. Broker %d can become leader since it is in the assigned replicas %s"
                      .format(partition, brokerId, assignedReplicas.mkString(",")))
                  false
                }
              case false =>
                info("ISR for topic %s partition %d is %s. Out of these %s brokers are alive. Broker %d "
                  .format(topic, partition, inSyncReplicas.mkString(",")) + "cannot become leader since it doesn't exist " +
                  "in the ISR")
                false  // let one of the live brokers in the ISR become the leader
            }
        }
      case false =>
        if(assignedReplicas.contains(brokerId)) {
          info("ISR for topic %s partition %d is empty. Broker %d can become leader since it "
            .format(topic, partition, brokerId) + "is part of the assigned replicas list")
          true
        } else {
          info("ISR for topic %s partition %d is empty. Broker %d cannot become leader since it "
            .format(topic, partition, brokerId) + "is not part of the assigned replicas list")
          false
        }
    }
  }

  class TopicChangeListener extends IZkChildListener with Logging {
    private val allTopics = new HashSet[String]()

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      import collection.JavaConversions
      topicListenerLock.synchronized {
        debug("Topic/partition change listener fired for path " + parentPath)
        val currentChildren = JavaConversions.asBuffer(curChilds).toSet
        val newTopics = currentChildren -- allTopics
        val deletedTopics = allTopics -- currentChildren
        allTopics.clear()
        allTopics ++ currentChildren

        debug("New topics: [%s]. Deleted topics: [%s]".format(newTopics.mkString(","), deletedTopics.mkString(",")))
        handleNewTopics(newTopics.toSeq)
        // TODO: Handle topic deletions
        //handleDeletedTopics(deletedTopics.toSeq)
      }
    }

    def doesTopicExistInCluster(topic: String): Boolean = {
      allTopics.contains(topic)
    }
  }

  private def ensureStateChangeCommandValidityOnThisBroker(stateChangeCommand: StateChangeCommand): Boolean = {
    // check if this broker hosts a replica for this topic and partition
    ZkUtils.isPartitionOnBroker(zkClient, stateChangeCommand.topic, stateChangeCommand.partition, config.brokerId)
  }

  private def ensureEpochValidity(stateChangeCommand: StateChangeCommand): Boolean = {
    // get the topic and partition that this request is meant for
    val topic = stateChangeCommand.topic
    val partition = stateChangeCommand.partition
    val epoch = stateChangeCommand.epoch

    val currentLeaderEpoch = ZkUtils.getEpochForPartition(zkClient, topic, partition)
    // check if the request's epoch matches the current leader's epoch OR the admin command's epoch
    val validEpoch = (currentLeaderEpoch == epoch) || (epoch == AdminUtils.AdminEpoch)
    if(epoch > currentLeaderEpoch)
      throw new IllegalStateException(("Illegal epoch state. Request's epoch %d larger than registered epoch %d for " +
        "topic %s partition %d").format(epoch, currentLeaderEpoch, topic, partition))
    validEpoch
  }

  class LeaderChangeListener extends IZkDataListener with Logging {

    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object) {
      // handle leader change event for path
      val newLeaderAndEpochInfo: String = data.asInstanceOf[String]
      val newLeader = newLeaderAndEpochInfo.split(";").head.toInt
      val newEpoch = newLeaderAndEpochInfo.split(";").last.toInt
      debug("Leader change listener fired for path %s. New leader is %d. New epoch is %d".format(dataPath, newLeader, newEpoch))
      val topicPartitionInfo = dataPath.split("/")
      val topic = topicPartitionInfo.takeRight(4).head
      val partition = topicPartitionInfo.takeRight(2).head.toInt
      info("Updating leader change information in replica for topic %s partition %d".format(topic, partition))
      val replica = getReplicaCbk(topic, partition).getOrElse(null)
      assert(replica != null, "Replica for topic %s partition %d should exist on broker %d"
        .format(topic, partition, config.brokerId))
      replica.partition.leaderId(Some(newLeader))
      assert(getReplicaCbk(topic, partition).get.partition.leaderId().get == newLeader, "New leader should be set correctly")
    }

    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
      leaderChangeLock.synchronized {
        // leader is deleted for topic partition
        val topic = dataPath.split("/").takeRight(4).head
        val partitionId = dataPath.split("/").takeRight(2).head.toInt
        debug("Leader deleted listener fired for topic %s partition %d on broker %d"
          .format(topic, partitionId, config.brokerId))
        val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partitionId).map(r => r.toInt)
        if(assignedReplicas.contains(config.brokerId)) {
          val replica = getReplicaCbk(topic, partitionId)
          replica match {
            case Some(r) => leaderElection(r)
            case None =>  error("No replica exists for topic %s partition %s on broker %d"
              .format(topic, partitionId, config.brokerId))
          }
        }
      }
    }
  }
}



