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

package kafka.consumer

import java.util.concurrent._
import java.util.concurrent.atomic._
import locks.ReentrantLock
import scala.collection._
import kafka.cluster._
import kafka.utils._
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import java.net.InetAddress
import org.I0Itec.zkclient.{IZkStateListener, IZkChildListener, ZkClient}
import org.apache.zookeeper.Watcher.Event.KeeperState
import kafka.api.OffsetRequest
import java.util.UUID
import kafka.serializer.Decoder
import kafka.common.{ConsumerRebalanceFailedException, InvalidConfigException}
import java.lang.IllegalStateException
import kafka.utils.ZkUtils._


/**
 * This class handles the consumers interaction with zookeeper
 *
 * Directories:
 * 1. Consumer id registry:
 * /consumers/[group_id]/ids[consumer_id] -> topic1,...topicN
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
 * and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
 * A consumer subscribes to event changes of the consumer id registry within its group.
 *
 * The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
 * ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
 * whether the creation of a sequential znode has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 *
 * 2. Broker node registry:
 * /brokers/[0...N] --> { "host" : "host:port",
 *                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
 *                                    "topicN": ["partition1" ... "partitionN"] } }
 * This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
 * node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
 * is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
 * the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
 * A consumer subscribes to event changes of the broker node registry.
 *
 * 3. Partition owner registry:
 * /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] --> consumer_node_id
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
 * within a consumer group. The mapping is reestablished after each rebalancing.
 *
 * 4. Consumer offset tracking:
 * /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value
 * Each consumer tracks the offset of the latest message consumed for each partition.
 *
 */
private[kafka] object ZookeeperConsumerConnector {
  val shutdownCommand: FetchedDataChunk = new FetchedDataChunk(null, null, -1L)
}

/**
 *  JMX interface for monitoring consumer
 */
trait ZookeeperConsumerConnectorMBean {
  def getPartOwnerStats: String
  def getConsumerGroup: String
  def getOffsetLag(topic: String, brokerId: Int, partitionId: Int): Long
  def getConsumedOffset(topic: String, brokerId: Int, partitionId: Int): Long
  def getLatestOffset(topic: String, brokerId: Int, partitionId: Int): Long
}

private[kafka] class ZookeeperConsumerConnector(val config: ConsumerConfig,
                                                val enableFetcher: Boolean) // for testing only
        extends ConsumerConnector with ZookeeperConsumerConnectorMBean
        with Logging {
  private val isShuttingDown = new AtomicBoolean(false)
  private val rebalanceLock = new Object
  private var fetcher: Option[Fetcher] = None
  private var zkClient: ZkClient = null
  private var topicRegistry = new Pool[String, Pool[Partition, PartitionTopicInfo]]
  // topicThreadIdAndQueues : (topic,consumerThreadId) -> queue
  private val topicThreadIdAndQueues = new Pool[Tuple2[String,String], BlockingQueue[FetchedDataChunk]]
  private val scheduler = new KafkaScheduler(1, "Kafka-consumer-autocommit-", false)
  private val messageStreamCreated = new AtomicBoolean(false)

  private var sessionExpirationListener: ZKSessionExpireListener = null
  private var loadBalancerListener: ZKRebalancerListener = null

  private var wildcardTopicWatcher: ZookeeperTopicEventWatcher = null

  val consumerIdString = {
    var consumerUuid : String = null
    config.consumerId match {
      case Some(consumerId) // for testing only
      => consumerUuid = consumerId
      case None // generate unique consumerId automatically
      => val uuid = UUID.randomUUID()
      consumerUuid = "%s-%d-%s".format(
        InetAddress.getLocalHost.getHostName, System.currentTimeMillis,
        uuid.getMostSignificantBits().toHexString.substring(0,8))
    }
    config.groupId + "_" + consumerUuid
  }
  this.logIdent = consumerIdString + " "

  connectZk()
  createFetcher()
  if (config.autoCommit) {
    info("starting auto committer every " + config.autoCommitIntervalMs + " ms")
    scheduler.scheduleWithRate(autoCommit, config.autoCommitIntervalMs, config.autoCommitIntervalMs)
  }

  def this(config: ConsumerConfig) = this(config, true)

  def createMessageStreams[T](topicCountMap: Map[String,Int],
                              decoder: Decoder[T])
      : Map[String,List[KafkaStream[T]]] = {
    if (messageStreamCreated.getAndSet(true))
      throw new RuntimeException(this.getClass.getSimpleName +
                                   " can create message streams at most once")
    consume(topicCountMap, decoder)
  }

  def createMessageStreamsByFilter[T](topicFilter: TopicFilter, numStreams: Int, decoder: Decoder[T]) = {
    val wildcardStreamsHandler = new WildcardStreamsHandler[T](topicFilter, numStreams, decoder)
    wildcardStreamsHandler.streams
  }

  private def createFetcher() {
    if (enableFetcher)
      fetcher = Some(new Fetcher(config, zkClient))
  }

  private def connectZk() {
    info("Connecting to zookeeper instance at " + config.zkConnect)
    zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)
  }

  def shutdown() {
    val canShutdown = isShuttingDown.compareAndSet(false, true);
    if (canShutdown) {
      info("ZKConsumerConnector shutting down")

      if (wildcardTopicWatcher != null)
        wildcardTopicWatcher.shutdown()
      try {
        scheduler.shutdownNow()
        fetcher match {
          case Some(f) => f.stopConnectionsToAllBrokers
          case None =>
        }
        sendShutdownToAllQueues()
        if (config.autoCommit)
          commitOffsets()
        if (zkClient != null) {
          zkClient.close()
          zkClient = null
        }
      }
      catch {
        case e =>
          fatal("error during consumer connector shutdown", e)
      }
      info("ZKConsumerConnector shut down completed")
    }
  }

  def consume[T](topicCountMap: scala.collection.Map[String,Int],
                 decoder: Decoder[T])
      : Map[String,List[KafkaStream[T]]] = {
    debug("entering consume ")
    if (topicCountMap == null)
      throw new RuntimeException("topicCountMap is null")

    val topicCount = TopicCount.constructTopicCount(consumerIdString, topicCountMap)

    val topicThreadIds = topicCount.getConsumerThreadIdsPerTopic

    // make a list of (queue,stream) pairs, one pair for each threadId
    val queuesAndStreams = topicThreadIds.values.map(threadIdSet =>
      threadIdSet.map(_ => {
        val queue =  new LinkedBlockingQueue[FetchedDataChunk](config.maxQueuedChunks)
        val stream = new KafkaStream[T](
          queue, config.consumerTimeoutMs, decoder, config.enableShallowIterator)
        (queue, stream)
      })
    ).flatten.toList

    val dirs = new ZKGroupDirs(config.groupId)
    registerConsumerInZK(dirs, consumerIdString, topicCount)
    reinitializeConsumer(topicCount, queuesAndStreams)

    loadBalancerListener.kafkaMessageAndMetadataStreams.asInstanceOf[Map[String, List[KafkaStream[T]]]]
  }

  private def registerConsumerInZK(dirs: ZKGroupDirs, consumerIdString: String, topicCount: TopicCount) {
    info("begin registering consumer " + consumerIdString + " in ZK")
    createEphemeralPathExpectConflict(zkClient,
                                      dirs.consumerRegistryDir + "/" + consumerIdString,
                                      topicCount.dbString)
    info("end registering consumer " + consumerIdString + " in ZK")
  }

  private def sendShutdownToAllQueues() = {
    for (queue <- topicThreadIdAndQueues.values) {
      debug("Clearing up queue")
      queue.clear()
      queue.put(ZookeeperConsumerConnector.shutdownCommand)
      debug("Cleared queue and sent shutdown command")
    }
  }

  def autoCommit() {
    trace("auto committing")
    try {
      commitOffsets()
    }
    catch {
      case t: Throwable =>
      // log it and let it go
        error("exception during autoCommit: ", t)
    }
  }

  def commitOffsets() {
    if (zkClient == null) {
      error("zk client is null. Cannot commit offsets")
      return
    }
    for ((topic, infos) <- topicRegistry) {
      val topicDirs = new ZKGroupTopicDirs(config.groupId, topic)
      for (info <- infos.values) {
        val newOffset = info.getConsumeOffset
        try {
          updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + info.partition.name,
            newOffset.toString)
        }
        catch {
          case t: Throwable =>
          // log it and let it go
            warn("exception during commitOffsets",  t)
        }
        debug("Committed offset " + newOffset + " for topic " + info)
      }
    }
  }

  // for JMX
  def getPartOwnerStats(): String = {
    val builder = new StringBuilder
    for ((topic, infos) <- topicRegistry) {
      builder.append("\n" + topic + ": [")
      val topicDirs = new ZKGroupTopicDirs(config.groupId, topic)
      for(partition <- infos.values) {
        builder.append("\n    {")
        builder.append{partition.partition.name}
        builder.append(",fetch offset:" + partition.getFetchOffset)
        builder.append(",consumer offset:" + partition.getConsumeOffset)
        builder.append("}")
      }
      builder.append("\n        ]")
    }
    builder.toString
  }

  // for JMX
  def getConsumerGroup(): String = config.groupId

  def getOffsetLag(topic: String, brokerId: Int, partitionId: Int): Long =
    getLatestOffset(topic, brokerId, partitionId) - getConsumedOffset(topic, brokerId, partitionId)

  def getConsumedOffset(topic: String, brokerId: Int, partitionId: Int): Long = {
    val partition = new Partition(brokerId, partitionId)
    val partitionInfos = topicRegistry.get(topic)
    if (partitionInfos != null) {
      val partitionInfo = partitionInfos.get(partition)
      if (partitionInfo != null)
        return partitionInfo.getConsumeOffset
    }

    //otherwise, try to get it from zookeeper
    try {
      val topicDirs = new ZKGroupTopicDirs(config.groupId, topic)
      val znode = topicDirs.consumerOffsetDir + "/" + partition.name
      val offsetString = readDataMaybeNull(zkClient, znode)
      if (offsetString != null)
        return offsetString.toLong
      else
        return -1
    }
    catch {
      case e =>
        error("error in getConsumedOffset JMX ", e)
    }
    return -2
  }

  def getLatestOffset(topic: String, brokerId: Int, partitionId: Int): Long =
    earliestOrLatestOffset(topic, brokerId, partitionId, OffsetRequest.LatestTime)

  private def earliestOrLatestOffset(topic: String, brokerId: Int, partitionId: Int, earliestOrLatest: Long): Long = {
    var simpleConsumer: SimpleConsumer = null
    var producedOffset: Long = -1L
    try {
      val cluster = getCluster(zkClient)
      val broker = cluster.getBroker(brokerId) match {
        case Some(b) => b
        case None => throw new IllegalStateException("Broker " + brokerId + " is unavailable. Cannot issue " +
          "getOffsetsBefore request")
      }
      simpleConsumer = new SimpleConsumer(broker.host, broker.port, ConsumerConfig.SocketTimeout,
                                            ConsumerConfig.SocketBufferSize)
      val offsets = simpleConsumer.getOffsetsBefore(topic, partitionId, earliestOrLatest, 1)
      producedOffset = offsets(0)
    }
    catch {
      case e =>
        error("error in earliestOrLatestOffset() ", e)
    }
    finally {
      if (simpleConsumer != null)
        simpleConsumer.close
    }
    producedOffset
  }

  class ZKSessionExpireListener(val dirs: ZKGroupDirs,
                                 val consumerIdString: String,
                                 val topicCount: TopicCount,
                                 val loadBalancerListener: ZKRebalancerListener)
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
       *  connection for us. We need to release the ownership of the current consumer and re-register this
       *  consumer in the consumer registry and trigger a rebalance.
       */
      info("ZK expired; release old broker parition ownership; re-register consumer " + consumerIdString)
      loadBalancerListener.resetState()
      registerConsumerInZK(dirs, consumerIdString, topicCount)
      // explicitly trigger load balancing for this consumer
      loadBalancerListener.syncedRebalance()

      // There is no need to resubscribe to child and state changes.
      // The child change watchers will be set inside rebalance when we read the children list.
    }

  }

  class ZKRebalancerListener(val group: String, val consumerIdString: String,
                             val kafkaMessageAndMetadataStreams: mutable.Map[String,List[KafkaStream[_]]])
    extends IZkChildListener {
    private var isWatcherTriggered = false
    private val lock = new ReentrantLock
    private val cond = lock.newCondition()
    private val watcherExecutorThread = new Thread(consumerIdString + "_watcher_executor") {
      override def run() {
        info("starting watcher executor thread for consumer " + consumerIdString)
        var doRebalance = false
        while (!isShuttingDown.get) {
          try {
            lock.lock()
            try {
              if (!isWatcherTriggered)
                cond.await(1000, TimeUnit.MILLISECONDS) // wake up periodically so that it can check the shutdown flag
            } finally {
              doRebalance = isWatcherTriggered
              isWatcherTriggered = false
              lock.unlock()
            }
            if (doRebalance)
              syncedRebalance
          } catch {
            case t => error("error during syncedRebalance", t)
          }
        }
        info("stopping watcher executor thread for consumer " + consumerIdString)
      }
    }
    watcherExecutorThread.start()

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      lock.lock()
      try {
        isWatcherTriggered = true
        cond.signalAll()
      } finally {
        lock.unlock()
      }
    }

    private def deletePartitionOwnershipFromZK(topic: String, partition: String) {
      val topicDirs = new ZKGroupTopicDirs(group, topic)
      val znode = topicDirs.consumerOwnerDir + "/" + partition
      deletePath(zkClient, znode)
      debug("Consumer " + consumerIdString + " releasing " + znode)
    }

    private def releasePartitionOwnership(localTopicRegistry: Pool[String, Pool[Partition, PartitionTopicInfo]])= {
      info("Releasing partition ownership")
      for ((topic, infos) <- localTopicRegistry) {
        for(partition <- infos.keys)
          deletePartitionOwnershipFromZK(topic, partition.toString)
        localTopicRegistry.remove(topic)
      }
    }

    def resetState() {
      topicRegistry.clear
    }

    def syncedRebalance() {
      rebalanceLock synchronized {
        for (i <- 0 until config.maxRebalanceRetries) {
          info("begin rebalancing consumer " + consumerIdString + " try #" + i)
          var done = false
          val cluster = getCluster(zkClient)
          try {
            done = rebalance(cluster)
          }
          catch {
            case e =>
              /** occasionally, we may hit a ZK exception because the ZK state is changing while we are iterating.
               * For example, a ZK node can disappear between the time we get all children and the time we try to get
               * the value of a child. Just let this go since another rebalance will be triggered.
               **/
              info("exception during rebalance ", e)
          }
          info("end rebalancing consumer " + consumerIdString + " try #" + i)
          if (done) {
            return
          }else {
              /* Here the cache is at a risk of being stale. To take future rebalancing decisions correctly, we should
               * clear the cache */
              info("Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered")
          }
          // stop all fetchers and clear all the queues to avoid data duplication
          closeFetchersForQueues(cluster, kafkaMessageAndMetadataStreams, topicThreadIdAndQueues.map(q => q._2))
          Thread.sleep(config.rebalanceBackoffMs)
        }
      }

      throw new ConsumerRebalanceFailedException(consumerIdString + " can't rebalance after " + config.maxRebalanceRetries +" retries")
    }

    private def rebalance(cluster: Cluster): Boolean = {
      val myTopicThreadIdsMap = TopicCount.constructTopicCount(group, consumerIdString, zkClient).getConsumerThreadIdsPerTopic
      val consumersPerTopicMap = getConsumersPerTopic(zkClient, group)
      val partitionsPerTopicMap = getPartitionsForTopics(zkClient, myTopicThreadIdsMap.keys.iterator)

      /**
       * fetchers must be stopped to avoid data duplication, since if the current
       * rebalancing attempt fails, the partitions that are released could be owned by another consumer.
       * But if we don't stop the fetchers first, this consumer would continue returning data for released
       * partitions in parallel. So, not stopping the fetchers leads to duplicate data.
       */
      closeFetchers(cluster, kafkaMessageAndMetadataStreams, myTopicThreadIdsMap)

      releasePartitionOwnership(topicRegistry)

      var partitionOwnershipDecision = new collection.mutable.HashMap[(String, String), String]()
      var currentTopicRegistry = new Pool[String, Pool[Partition, PartitionTopicInfo]]

      for ((topic, consumerThreadIdSet) <- myTopicThreadIdsMap) {
        currentTopicRegistry.put(topic, new Pool[Partition, PartitionTopicInfo])

        val topicDirs = new ZKGroupTopicDirs(group, topic)
        val curConsumers = consumersPerTopicMap.get(topic).get
        var curPartitions: List[String] = partitionsPerTopicMap.get(topic).get

        val nPartsPerConsumer = curPartitions.size / curConsumers.size
        val nConsumersWithExtraPart = curPartitions.size % curConsumers.size

        info("Consumer " + consumerIdString + " rebalancing the following partitions: " + curPartitions +
          " for topic " + topic + " with consumers: " + curConsumers)

        for (consumerThreadId <- consumerThreadIdSet) {
          val myConsumerPosition = curConsumers.findIndexOf(_ == consumerThreadId)
          assert(myConsumerPosition >= 0)
          val startPart = nPartsPerConsumer*myConsumerPosition + myConsumerPosition.min(nConsumersWithExtraPart)
          val nParts = nPartsPerConsumer + (if (myConsumerPosition + 1 > nConsumersWithExtraPart) 0 else 1)

          /**
           *   Range-partition the sorted partitions to consumers for better locality.
           *  The first few consumers pick up an extra partition, if any.
           */
          if (nParts <= 0)
            warn("No broker partitions consumed by consumer thread " + consumerThreadId + " for topic " + topic)
          else {
            for (i <- startPart until startPart + nParts) {
              val partition = curPartitions(i)
              info(consumerThreadId + " attempting to claim partition " + partition)
              addPartitionTopicInfo(currentTopicRegistry, topicDirs, partition, topic, consumerThreadId)
              // record the partition ownership decision
              partitionOwnershipDecision += ((topic, partition) -> consumerThreadId)
            }
          }
        }
      }

      /**
       * move the partition ownership here, since that can be used to indicate a truly successful rebalancing attempt
       * A rebalancing attempt is completed successfully only after the fetchers have been started correctly
       */
      if(reflectPartitionOwnershipDecision(partitionOwnershipDecision.toMap)) {
        info("Updating the cache")
        debug("Partitions per topic cache " + partitionsPerTopicMap)
        debug("Consumers per topic cache " + consumersPerTopicMap)
        topicRegistry = currentTopicRegistry
        updateFetcher(cluster)
        true
      }else {
        false
      }
    }

    private def closeFetchersForQueues(cluster: Cluster,
                                       messageStreams: Map[String,List[KafkaStream[_]]],
                                       queuesToBeCleared: Iterable[BlockingQueue[FetchedDataChunk]]) {
      var allPartitionInfos = topicRegistry.values.map(p => p.values).flatten
      fetcher match {
        case Some(f) => f.stopConnectionsToAllBrokers
        f.clearFetcherQueues(allPartitionInfos, cluster, queuesToBeCleared, messageStreams)
        info("Committing all offsets after clearing the fetcher queues")
        /**
        * here, we need to commit offsets before stopping the consumer from returning any more messages
        * from the current data chunk. Since partition ownership is not yet released, this commit offsets
        * call will ensure that the offsets committed now will be used by the next consumer thread owning the partition
        * for the current data chunk. Since the fetchers are already shutdown and this is the last chunk to be iterated
        * by the consumer, there will be no more messages returned by this iterator until the rebalancing finishes
        * successfully and the fetchers restart to fetch more data chunks
        **/
        commitOffsets
        case None =>
      }
    }

    private def closeFetchers(cluster: Cluster, messageStreams: Map[String,List[KafkaStream[_]]],
                              relevantTopicThreadIdsMap: Map[String, Set[String]]) {
      // only clear the fetcher queues for certain topic partitions that *might* no longer be served by this consumer
      // after this rebalancing attempt
      val queuesTobeCleared = topicThreadIdAndQueues.filter(q => relevantTopicThreadIdsMap.contains(q._1._1)).map(q => q._2)
      closeFetchersForQueues(cluster, messageStreams, queuesTobeCleared)
    }

    private def updateFetcher(cluster: Cluster) {
      // update partitions for fetcher
      var allPartitionInfos : List[PartitionTopicInfo] = Nil
      for (partitionInfos <- topicRegistry.values)
        for (partition <- partitionInfos.values)
          allPartitionInfos ::= partition
      info("Consumer " + consumerIdString + " selected partitions : " +
        allPartitionInfos.sortWith((s,t) => s.partition < t.partition).map(_.toString).mkString(","))

      fetcher match {
        case Some(f) =>
          f.startConnections(allPartitionInfos, cluster)
        case None =>
      }
    }

    private def reflectPartitionOwnershipDecision(partitionOwnershipDecision: Map[(String, String), String]): Boolean = {
      var successfullyOwnedPartitions : List[(String, String)] = Nil
      val partitionOwnershipSuccessful = partitionOwnershipDecision.map { partitionOwner =>
        val topic = partitionOwner._1._1
        val partition = partitionOwner._1._2
        val consumerThreadId = partitionOwner._2
        val topicDirs = new ZKGroupTopicDirs(group, topic)
        val partitionOwnerPath = topicDirs.consumerOwnerDir + "/" + partition
        try {
          createEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId)
          info(consumerThreadId + " successfully owned partition " + partition + " for topic " + topic)
          successfullyOwnedPartitions ::= (topic, partition)
          true
        }
        catch {
          case e: ZkNodeExistsException =>
            // The node hasn't been deleted by the original owner. So wait a bit and retry.
            info("waiting for the partition ownership to be deleted: " + partition)
            false
          case e2 => throw e2
        }
      }
      val hasPartitionOwnershipFailed = partitionOwnershipSuccessful.foldLeft(0)((sum, decision) => sum + (if(decision) 0 else 1))
      /* even if one of the partition ownership attempt has failed, return false */
      if(hasPartitionOwnershipFailed > 0) {
        // remove all paths that we have owned in ZK
        successfullyOwnedPartitions.foreach(topicAndPartition => deletePartitionOwnershipFromZK(topicAndPartition._1, topicAndPartition._2))
        false
      }
      else true
    }

    private def addPartitionTopicInfo(currentTopicRegistry: Pool[String, Pool[Partition, PartitionTopicInfo]],
                                      topicDirs: ZKGroupTopicDirs, partitionString: String,
                                      topic: String, consumerThreadId: String) {
      val partition = Partition.parse(partitionString)
      val partTopicInfoMap = currentTopicRegistry.get(topic)

      val znode = topicDirs.consumerOffsetDir + "/" + partition.name
      val offsetString = readDataMaybeNull(zkClient, znode)
      // If first time starting a consumer, set the initial offset based on the config
      var offset : Long = 0L
      if (offsetString == null)
        offset = config.autoOffsetReset match {
              case OffsetRequest.SmallestTimeString =>
                  earliestOrLatestOffset(topic, partition.brokerId, partition.partId, OffsetRequest.EarliestTime)
              case OffsetRequest.LargestTimeString =>
                  earliestOrLatestOffset(topic, partition.brokerId, partition.partId, OffsetRequest.LatestTime)
              case _ =>
                  throw new InvalidConfigException("Wrong value in autoOffsetReset in ConsumerConfig")
        }
      else
        offset = offsetString.toLong
      val queue = topicThreadIdAndQueues.get((topic, consumerThreadId))
      val consumedOffset = new AtomicLong(offset)
      val fetchedOffset = new AtomicLong(offset)
      val partTopicInfo = new PartitionTopicInfo(topic,
                                                 partition.brokerId,
                                                 partition,
                                                 queue,
                                                 consumedOffset,
                                                 fetchedOffset,
                                                 new AtomicInteger(config.fetchSize))
      partTopicInfoMap.put(partition, partTopicInfo)
      debug(partTopicInfo + " selected new offset " + offset)
    }
  }

  private def reinitializeConsumer[T](
      topicCount: TopicCount,
      queuesAndStreams: List[(LinkedBlockingQueue[FetchedDataChunk],KafkaStream[T])]) {

    val dirs = new ZKGroupDirs(config.groupId)

    // listener to consumer and partition changes
    if (loadBalancerListener == null) {
      val topicStreamsMap = new mutable.HashMap[String,List[KafkaStream[T]]]
      loadBalancerListener = new ZKRebalancerListener(
        config.groupId, consumerIdString, topicStreamsMap.asInstanceOf[scala.collection.mutable.Map[String, List[KafkaStream[_]]]])
    }

    // register listener for session expired event
    if (sessionExpirationListener == null)
      sessionExpirationListener = new ZKSessionExpireListener(
        dirs, consumerIdString, topicCount, loadBalancerListener)

    val topicStreamsMap = loadBalancerListener.kafkaMessageAndMetadataStreams

    // map of {topic -> Set(thread-1, thread-2, ...)}
    val consumerThreadIdsPerTopic: Map[String, Set[String]] =
      topicCount.getConsumerThreadIdsPerTopic

    /*
     * This usage of map flatten breaks up consumerThreadIdsPerTopic into
     * a set of (topic, thread-id) pairs that we then use to construct
     * the updated (topic, thread-id) -> queues map
     */
    implicit def getTopicThreadIds(v: (String, Set[String])): Set[(String, String)] = v._2.map((v._1, _))

    // iterator over (topic, thread-id) tuples
    val topicThreadIds: Iterable[(String, String)] =
      consumerThreadIdsPerTopic.flatten

    // list of (pairs of pairs): e.g., ((topic, thread-id),(queue, stream))
    val threadQueueStreamPairs = topicCount match {
      case wildTopicCount: WildcardTopicCount =>
        for (tt <- topicThreadIds; qs <- queuesAndStreams) yield (tt -> qs)
      case statTopicCount: StaticTopicCount => {
        require(topicThreadIds.size == queuesAndStreams.size,
          "Mismatch between thread ID count (%d) and queue count (%d)".format(
          topicThreadIds.size, queuesAndStreams.size))
        topicThreadIds.zip(queuesAndStreams)
      }
    }

    threadQueueStreamPairs.foreach(e => {
      val topicThreadId = e._1
      val q = e._2._1
      topicThreadIdAndQueues.put(topicThreadId, q)
    })

    val groupedByTopic = threadQueueStreamPairs.groupBy(_._1._1)
    groupedByTopic.foreach(e => {
      val topic = e._1
      val streams = e._2.map(_._2._2).toList
      topicStreamsMap += (topic -> streams)
      debug("adding topic %s and %d streams to map.".format(topic, streams.size))
    })

    // listener to consumer and partition changes
    zkClient.subscribeStateChanges(sessionExpirationListener)

    zkClient.subscribeChildChanges(dirs.consumerRegistryDir, loadBalancerListener)

    topicStreamsMap.foreach { topicAndStreams =>
      // register on broker partition path changes
      val partitionPath = BrokerTopicsPath + "/" + topicAndStreams._1
      zkClient.subscribeChildChanges(partitionPath, loadBalancerListener)
    }

    // explicitly trigger load balancing for this consumer
    loadBalancerListener.syncedRebalance()
  }

  class WildcardStreamsHandler[T](topicFilter: TopicFilter,
                                  numStreams: Int,
                                  decoder: Decoder[T])
                                extends TopicEventHandler[String] {

    if (messageStreamCreated.getAndSet(true))
      throw new RuntimeException("Each consumer connector can create " +
        "message streams by filter at most once.")

    private val wildcardQueuesAndStreams = (1 to numStreams)
      .map(e => {
        val queue = new LinkedBlockingQueue[FetchedDataChunk](config.maxQueuedChunks)
        val stream = new KafkaStream[T](
          queue, config.consumerTimeoutMs, decoder, config.enableShallowIterator)
        (queue, stream)
    }).toList

     // bootstrap with existing topics
    private var wildcardTopics =
      getChildrenParentMayNotExist(zkClient, BrokerTopicsPath)
        .filter(topicFilter.isTopicAllowed)

    private val wildcardTopicCount = TopicCount.constructTopicCount(
      consumerIdString, topicFilter, numStreams, zkClient)

    val dirs = new ZKGroupDirs(config.groupId)
    registerConsumerInZK(dirs, consumerIdString, wildcardTopicCount)
    reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams)

    if (!topicFilter.requiresTopicEventWatcher) {
      info("Not creating event watcher for trivial whitelist " + topicFilter)
    }
    else {
      info("Creating topic event watcher for whitelist " + topicFilter)
      wildcardTopicWatcher = new ZookeeperTopicEventWatcher(config, this)

      /*
       * Topic events will trigger subsequent synced rebalances. Also, the
       * consumer will get registered only after an allowed topic becomes
       * available.
       */
    }

    def handleTopicEvent(allTopics: Seq[String]) {
      debug("Handling topic event")

      val updatedTopics = allTopics.filter(topicFilter.isTopicAllowed)

      val addedTopics = updatedTopics filterNot (wildcardTopics contains)
      if (addedTopics.nonEmpty)
        info("Topic event: added topics = %s"
                             .format(addedTopics))

      /*
       * TODO: Deleted topics are interesting (and will not be a concern until
       * 0.8 release). We may need to remove these topics from the rebalance
       * listener's map in reinitializeConsumer.
       */
      val deletedTopics = wildcardTopics filterNot (updatedTopics contains)
      if (deletedTopics.nonEmpty)
        info("Topic event: deleted topics = %s"
                             .format(deletedTopics))

      wildcardTopics = updatedTopics
      info("Topics to consume = %s".format(wildcardTopics))

      if (addedTopics.nonEmpty || deletedTopics.nonEmpty)
        reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams)
    }

    def streams: Seq[KafkaStream[T]] =
      wildcardQueuesAndStreams.map(_._2)
  }
}

