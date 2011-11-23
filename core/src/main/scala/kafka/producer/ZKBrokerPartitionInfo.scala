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
package kafka.producer

import kafka.utils.{ZKStringSerializer, ZkUtils, ZKConfig}
import collection.mutable.HashMap
import collection.mutable.Map
import kafka.utils.Logging
import collection.immutable.TreeSet
import kafka.cluster.{Broker, Partition}
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.I0Itec.zkclient.{IZkStateListener, IZkChildListener, ZkClient}
import collection.SortedSet

private[producer] object ZKBrokerPartitionInfo {

  /**
   * Generate a mapping from broker id to (brokerId, numPartitions) for the list of brokers
   * specified
   * @param topic the topic to which the brokers have registered
   * @param brokerList the list of brokers for which the partitions info is to be generated
   * @return a sequence of (brokerId, numPartitions) for brokers in brokerList
   */
  private def getBrokerPartitions(zkClient: ZkClient, topic: String, brokerList: List[Int]): SortedSet[Partition] = {
    val brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic
    val numPartitions = brokerList.map(bid => ZkUtils.readData(zkClient, brokerTopicPath + "/" + bid).toInt)
    val brokerPartitions = brokerList.zip(numPartitions)

    val sortedBrokerPartitions = brokerPartitions.sortWith((id1, id2) => id1._1 < id2._1)

    var brokerParts = SortedSet.empty[Partition]
    sortedBrokerPartitions.foreach { bp =>
      for(i <- 0 until bp._2) {
        val bidPid = new Partition(bp._1, i)
        brokerParts = brokerParts + bidPid
      }
    }
    brokerParts
  }
}

/**
 * If zookeeper based auto partition discovery is enabled, fetch broker info like
 * host, port, number of partitions from zookeeper
 */
private[producer] class ZKBrokerPartitionInfo(config: ZKConfig, producerCbk: (Int, String, Int) => Unit) extends BrokerPartitionInfo with Logging {
  private val zkWatcherLock = new Object
  private val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
    ZKStringSerializer)
  // maintain a map from topic -> list of (broker, num_partitions) from zookeeper
  private var topicBrokerPartitions = getZKTopicPartitionInfo
  // maintain a map from broker id to the corresponding Broker object
  private var allBrokers = getZKBrokerInfo

  // use just the brokerTopicsListener for all watchers
  private val brokerTopicsListener = new BrokerTopicsListener(topicBrokerPartitions, allBrokers)
  // register listener for change of topics to keep topicsBrokerPartitions updated
  zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, brokerTopicsListener)

  // register listener for change of brokers for each topic to keep topicsBrokerPartitions updated
  topicBrokerPartitions.keySet.foreach {topic =>
    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + topic, brokerTopicsListener)
    debug("Registering listener on path: " + ZkUtils.BrokerTopicsPath + "/" + topic)
  }

  // register listener for new broker
  zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, brokerTopicsListener)

  // register listener for session expired event
  zkClient.subscribeStateChanges(new ZKSessionExpirationListener(brokerTopicsListener))

  /**
   * Return a sequence of (brokerId, numPartitions)
   * @param topic the topic for which this information is to be returned
   * @return a sequence of (brokerId, numPartitions). Returns a zero-length
   * sequence if no brokers are available.
   */
  def getBrokerPartitionInfo(topic: String): SortedSet[Partition] = {
    zkWatcherLock synchronized {
      val brokerPartitions = topicBrokerPartitions.get(topic)
      var numBrokerPartitions = SortedSet.empty[Partition]
      brokerPartitions match {
        case Some(bp) =>
          bp.size match {
            case 0 => // no brokers currently registered for this topic. Find the list of all brokers in the cluster.
              numBrokerPartitions = bootstrapWithExistingBrokers(topic)
              topicBrokerPartitions += (topic -> numBrokerPartitions)
            case _ => numBrokerPartitions = TreeSet[Partition]() ++ bp
          }
        case None =>  // no brokers currently registered for this topic. Find the list of all brokers in the cluster.
          numBrokerPartitions = bootstrapWithExistingBrokers(topic)
          topicBrokerPartitions += (topic -> numBrokerPartitions)
      }
      numBrokerPartitions
    }
  }

  /**
   * Generate the host and port information for the broker identified
   * by the given broker id
   * @param brokerId the broker for which the info is to be returned
   * @return host and port of brokerId
   */
  def getBrokerInfo(brokerId: Int): Option[Broker] =  {
    zkWatcherLock synchronized {
      allBrokers.get(brokerId)
    }
  }

  /**
   * Generate a mapping from broker id to the host and port for all brokers
   * @return mapping from id to host and port of all brokers
   */
  def getAllBrokerInfo: Map[Int, Broker] = allBrokers

  def close = zkClient.close

  def updateInfo = {
    zkWatcherLock synchronized {
      topicBrokerPartitions = getZKTopicPartitionInfo
      allBrokers = getZKBrokerInfo
    }
  }

  private def bootstrapWithExistingBrokers(topic: String): scala.collection.immutable.SortedSet[Partition] = {
   debug("Currently, no brokers are registered under topic: " + topic)
    debug("Bootstrapping topic: " + topic + " with available brokers in the cluster with default " +
      "number of partitions = 1")
    val allBrokersIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath)
    trace("List of all brokers currently registered in zookeeper = " + allBrokersIds.toString)
    // since we do not have the in formation about number of partitions on these brokers, just assume single partition
    // i.e. pick partition 0 from each broker as a candidate
    val numBrokerPartitions = TreeSet[Partition]() ++ allBrokersIds.map(b => new Partition(b.toInt, 0))
    // add the rest of the available brokers with default 1 partition for this topic, so all of the brokers
    // participate in hosting this topic.
    debug("Adding following broker id, partition id for NEW topic: " + topic + "=" + numBrokerPartitions.toString)
    numBrokerPartitions
  }

  /**
   * Generate a sequence of (brokerId, numPartitions) for all topics
   * registered in zookeeper
   * @return a mapping from topic to sequence of (brokerId, numPartitions)
   */
  private def getZKTopicPartitionInfo(): collection.mutable.Map[String, SortedSet[Partition]] = {
    val brokerPartitionsPerTopic = new HashMap[String, SortedSet[Partition]]()
    ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.BrokerTopicsPath)
    val topics = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerTopicsPath)
    topics.foreach { topic =>
    // find the number of broker partitions registered for this topic
      val brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic
      val brokerList = ZkUtils.getChildrenParentMayNotExist(zkClient, brokerTopicPath)
      val numPartitions = brokerList.map(bid => ZkUtils.readData(zkClient, brokerTopicPath + "/" + bid).toInt)
      val brokerPartitions = brokerList.map(bid => bid.toInt).zip(numPartitions)
      val sortedBrokerPartitions = brokerPartitions.sortWith((id1, id2) => id1._1 < id2._1)
      debug("Broker ids and # of partitions on each for topic: " + topic + " = " + sortedBrokerPartitions.toString)

      var brokerParts = SortedSet.empty[Partition]
      sortedBrokerPartitions.foreach { bp =>
        for(i <- 0 until bp._2) {
          val bidPid = new Partition(bp._1, i)
          brokerParts = brokerParts + bidPid
        }
      }
      brokerPartitionsPerTopic += (topic -> brokerParts)
      debug("Sorted list of broker ids and partition ids on each for topic: " + topic + " = " + brokerParts.toString)
    }
    brokerPartitionsPerTopic
  }

  /**
   * Generate a mapping from broker id to (brokerId, numPartitions) for all brokers
   * registered in zookeeper
   * @return a mapping from brokerId to (host, port)
   */
  private def getZKBrokerInfo(): Map[Int, Broker] = {
    val brokers = new HashMap[Int, Broker]()
    val allBrokerIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath).map(bid => bid.toInt)
    allBrokerIds.foreach { bid =>
      val brokerInfo = ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + bid)
      brokers += (bid -> Broker.createBroker(bid, brokerInfo))
    }
    brokers
  }

  /**
   * Listens to new broker registrations under a particular topic, in zookeeper and
   * keeps the related data structures updated
   */
  class BrokerTopicsListener(val originalBrokerTopicsPartitionsMap: collection.mutable.Map[String, SortedSet[Partition]],
                             val originalBrokerIdMap: Map[Int, Broker]) extends IZkChildListener with Logging {
    private var oldBrokerTopicPartitionsMap = collection.mutable.Map.empty[String, SortedSet[Partition]] ++
                                              originalBrokerTopicsPartitionsMap
    private var oldBrokerIdMap = collection.mutable.Map.empty[Int, Broker] ++ originalBrokerIdMap

    debug("[BrokerTopicsListener] Creating broker topics listener to watch the following paths - \n" +
      "/broker/topics, /broker/topics/topic, /broker/ids")
    debug("[BrokerTopicsListener] Initialized this broker topics listener with initial mapping of broker id to " +
      "partition id per topic with " + oldBrokerTopicPartitionsMap.toString)

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, currentChildren : java.util.List[String]) {
      val curChilds: java.util.List[String] = if(currentChildren != null) currentChildren
                                              else new java.util.ArrayList[String]()

      zkWatcherLock synchronized {
        trace("Watcher fired for path: " + parentPath + " with change " + curChilds.toString)
        import scala.collection.JavaConversions._

        parentPath match {
          case "/brokers/topics" =>        // this is a watcher for /broker/topics path
            val updatedTopics = asBuffer(curChilds)
            debug("[BrokerTopicsListener] List of topics changed at " + parentPath + " Updated topics -> " +
                curChilds.toString)
            debug("[BrokerTopicsListener] Old list of topics: " + oldBrokerTopicPartitionsMap.keySet.toString)
            debug("[BrokerTopicsListener] Updated list of topics: " + updatedTopics.toSet.toString)
            val newTopics = updatedTopics.toSet &~ oldBrokerTopicPartitionsMap.keySet
            debug("[BrokerTopicsListener] List of newly registered topics: " + newTopics.toString)
            newTopics.foreach { topic =>
              val brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic
              val brokerList = ZkUtils.getChildrenParentMayNotExist(zkClient, brokerTopicPath)
              processNewBrokerInExistingTopic(topic, brokerList)
              zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + topic,
                brokerTopicsListener)
            }
          case "/brokers/ids"    =>        // this is a watcher for /broker/ids path
            debug("[BrokerTopicsListener] List of brokers changed in the Kafka cluster " + parentPath +
                "\t Currently registered list of brokers -> " + curChilds.toString)
            processBrokerChange(parentPath, curChilds)
          case _ =>
            val pathSplits = parentPath.split("/")
            val topic = pathSplits.last
            if(pathSplits.length == 4 && pathSplits(2).equals("topics")) {
              debug("[BrokerTopicsListener] List of brokers changed at " + parentPath + "\t Currently registered " +
                  " list of brokers -> " + curChilds.toString + " for topic -> " + topic)
              processNewBrokerInExistingTopic(topic, asBuffer(curChilds))
            }
        }

        // update the data structures tracking older state values
        oldBrokerTopicPartitionsMap = collection.mutable.Map.empty[String, SortedSet[Partition]] ++ topicBrokerPartitions
        oldBrokerIdMap = collection.mutable.Map.empty[Int, Broker] ++  allBrokers
      }
    }

    def processBrokerChange(parentPath: String, curChilds: Seq[String]) {
      if(parentPath.equals(ZkUtils.BrokerIdsPath)) {
        import scala.collection.JavaConversions._
        val updatedBrokerList = asBuffer(curChilds).map(bid => bid.toInt)
        val newBrokers = updatedBrokerList.toSet &~ oldBrokerIdMap.keySet
        debug("[BrokerTopicsListener] List of newly registered brokers: " + newBrokers.toString)
        newBrokers.foreach { bid =>
          val brokerInfo = ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + bid)
          val brokerHostPort = brokerInfo.split(":")
          allBrokers += (bid -> new Broker(bid, brokerHostPort(1), brokerHostPort(1), brokerHostPort(2).toInt))
          debug("[BrokerTopicsListener] Invoking the callback for broker: " + bid)
          producerCbk(bid, brokerHostPort(1), brokerHostPort(2).toInt)
        }
        // remove dead brokers from the in memory list of live brokers
        val deadBrokers = oldBrokerIdMap.keySet &~ updatedBrokerList.toSet
        debug("[BrokerTopicsListener] Deleting broker ids for dead brokers: " + deadBrokers.toString)
        deadBrokers.foreach {bid =>
          allBrokers = allBrokers - bid
          // also remove this dead broker from particular topics
          topicBrokerPartitions.keySet.foreach{ topic =>
            topicBrokerPartitions.get(topic) match {
              case Some(oldBrokerPartitionList) =>
                val aliveBrokerPartitionList = oldBrokerPartitionList.filter(bp => bp.brokerId != bid)
                topicBrokerPartitions += (topic -> aliveBrokerPartitionList)
                debug("[BrokerTopicsListener] Removing dead broker ids for topic: " + topic + "\t " +
                  "Updated list of broker id, partition id = " + aliveBrokerPartitionList.toString)
              case None =>
            }
          }
        }
      }
    }

    /**
     * Generate the updated mapping of (brokerId, numPartitions) for the new list of brokers
     * registered under some topic
     * @param parentPath the path of the topic under which the brokers have changed
     * @param curChilds the list of changed brokers
     */
    def processNewBrokerInExistingTopic(topic: String, curChilds: Seq[String]) = {
      // find the old list of brokers for this topic
      oldBrokerTopicPartitionsMap.get(topic) match {
        case Some(brokersParts) =>
          debug("[BrokerTopicsListener] Old list of brokers: " + brokersParts.map(bp => bp.brokerId).toString)
        case None =>
      }

      val updatedBrokerList = curChilds.map(b => b.toInt)
      import ZKBrokerPartitionInfo._
      val updatedBrokerParts:SortedSet[Partition] = getBrokerPartitions(zkClient, topic, updatedBrokerList.toList)
      debug("[BrokerTopicsListener] Currently registered list of brokers for topic: " + topic + " are " +
          curChilds.toString)
      // update the number of partitions on existing brokers
      var mergedBrokerParts: SortedSet[Partition] = TreeSet[Partition]() ++ updatedBrokerParts
      topicBrokerPartitions.get(topic) match {
        case Some(oldBrokerParts) =>
          debug("[BrokerTopicsListener] Unregistered list of brokers for topic: " + topic + " are " +
            oldBrokerParts.toString)
          mergedBrokerParts = oldBrokerParts ++ updatedBrokerParts
        case None =>
      }
      // keep only brokers that are alive
      mergedBrokerParts = mergedBrokerParts.filter(bp => allBrokers.contains(bp.brokerId))
      topicBrokerPartitions += (topic -> mergedBrokerParts)
      debug("[BrokerTopicsListener] List of broker partitions for topic: " + topic + " are " +
          mergedBrokerParts.toString)
    }

    def resetState = {
      trace("[BrokerTopicsListener] Before reseting broker topic partitions state " +
          oldBrokerTopicPartitionsMap.toString)
      oldBrokerTopicPartitionsMap = collection.mutable.Map.empty[String, SortedSet[Partition]] ++ topicBrokerPartitions
      debug("[BrokerTopicsListener] After reseting broker topic partitions state " +
          oldBrokerTopicPartitionsMap.toString)
      trace("[BrokerTopicsListener] Before reseting broker id map state " + oldBrokerIdMap.toString)
      oldBrokerIdMap = collection.mutable.Map.empty[Int, Broker] ++  allBrokers
      debug("[BrokerTopicsListener] After reseting broker id map state " + oldBrokerIdMap.toString)
    }
  }

  /**
   * Handles the session expiration event in zookeeper
   */
  class ZKSessionExpirationListener(val brokerTopicsListener: BrokerTopicsListener)
    extends IZkStateListener {

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
      /**
       *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
       *  connection for us.
       */
      info("ZK expired; release old list of broker partitions for topics ")
      topicBrokerPartitions = getZKTopicPartitionInfo
      allBrokers = getZKBrokerInfo
      brokerTopicsListener.resetState

      // register listener for change of brokers for each topic to keep topicsBrokerPartitions updated
      // NOTE: this is probably not required here. Since when we read from getZKTopicPartitionInfo() above,
      // it automatically recreates the watchers there itself
      topicBrokerPartitions.keySet.foreach(topic => zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + topic,
        brokerTopicsListener))
      // there is no need to re-register other listeners as they are listening on the child changes of
      // permanent nodes
    }

  }

}
