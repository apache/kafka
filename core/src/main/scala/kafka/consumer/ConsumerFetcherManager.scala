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

import org.I0Itec.zkclient.ZkClient
import kafka.server.{AbstractFetcherThread, AbstractFetcherManager}
import kafka.cluster.{Cluster, Broker}
import scala.collection.immutable
import collection.mutable.HashMap
import scala.collection.mutable
import java.util.concurrent.locks.ReentrantLock
import kafka.utils.ZkUtils._
import kafka.utils.{ShutdownableThread, SystemTime}
import kafka.common.TopicAndPartition
import kafka.client.ClientUtils

/**
 *  Usage:
 *  Once ConsumerFetcherManager is created, startConnections() and stopAllConnections() can be called repeatedly
 *  until shutdown() is called.
 */
class ConsumerFetcherManager(private val consumerIdString: String,
                             private val config: ConsumerConfig,
                             private val zkClient : ZkClient)
        extends AbstractFetcherManager("ConsumerFetcherManager-%d".format(SystemTime.milliseconds), 1) {
  private var partitionMap: immutable.Map[TopicAndPartition, PartitionTopicInfo] = null
  private var cluster: Cluster = null
  private val noLeaderPartitionSet = new mutable.HashSet[TopicAndPartition]
  private val lock = new ReentrantLock
  private val cond = lock.newCondition()
  private val leaderFinderThread = new ShutdownableThread(consumerIdString + "-leader-finder-thread"){
    // thread responsible for adding the fetcher to the right broker when leader is available
    override def doWork() {
      lock.lock()
      try {
        if (noLeaderPartitionSet.isEmpty)
          cond.await()

        try {
          trace("Partitions without leader %s".format(noLeaderPartitionSet))
          val brokers = getAllBrokersInCluster(zkClient)
          val topicsMetadata = ClientUtils.fetchTopicMetadata(noLeaderPartitionSet.map(m => m.topic).toSet,
                                                              brokers,
                                                              config.clientId,
                                                              config.socketTimeoutMs).topicsMetadata
          val leaderForPartitionsMap = new HashMap[TopicAndPartition, Broker]
          topicsMetadata.foreach(
            tmd => {
              val topic = tmd.topic
              tmd.partitionsMetadata.foreach(
              pmd => {
                val topicAndPartition = TopicAndPartition(topic, pmd.partitionId)
                if(pmd.leader.isDefined && noLeaderPartitionSet.contains(topicAndPartition)) {
                  val leaderBroker = pmd.leader.get
                  leaderForPartitionsMap.put(topicAndPartition, leaderBroker)
                }
              })
            })

          leaderForPartitionsMap.foreach{
            case(topicAndPartition, leaderBroker) =>
              val pti = partitionMap(topicAndPartition)
              try {
                  addFetcher(topicAndPartition.topic, topicAndPartition.partition, pti.getFetchOffset(), leaderBroker)
                  noLeaderPartitionSet -= topicAndPartition
              } catch {
                case t => warn("Failed to add fetcher for %s to broker %s".format(topicAndPartition, leaderBroker), t)
              }
          }

          shutdownIdleFetcherThreads()
        } catch {
          case t => warn("Failed to find leader for %s".format(noLeaderPartitionSet), t)
        }
      } finally {
        lock.unlock()
      }
      Thread.sleep(config.refreshLeaderBackoffMs)
    }
  }
  leaderFinderThread.start()


  override def createFetcherThread(fetcherId: Int, sourceBroker: Broker): AbstractFetcherThread = {
    new ConsumerFetcherThread(
      "ConsumerFetcherThread-%s-%d-%d".format(consumerIdString, fetcherId, sourceBroker.id),
      config, sourceBroker, partitionMap, this)
  }

  def startConnections(topicInfos: Iterable[PartitionTopicInfo], cluster: Cluster) {
    if (!leaderFinderThread.isRunning.get())
      throw new RuntimeException("%s already shutdown".format(name))
    lock.lock()
    try {
      partitionMap = topicInfos.map(tpi => (TopicAndPartition(tpi.topic, tpi.partitionId), tpi)).toMap
      this.cluster = cluster
      noLeaderPartitionSet ++= topicInfos.map(tpi => TopicAndPartition(tpi.topic, tpi.partitionId))
      cond.signalAll()
    } finally {
      lock.unlock()
    }
  }

  def stopAllConnections() {
    lock.lock()
    // first, clear noLeaderPartitionSet so that no more fetchers can be added to leader_finder_thread
    noLeaderPartitionSet.clear()
    // second, clear partitionMap
    partitionMap = null
    lock.unlock()

    // third, stop all existing fetchers
    closeAllFetchers()
  }

  def addPartitionsWithError(partitionList: Iterable[TopicAndPartition]) {
    debug("adding partitions with error %s".format(partitionList))
    lock.lock()
    try {
      if (partitionMap != null) {
        partitionList.foreach(tp => removeFetcher(tp.topic, tp.partition))
        noLeaderPartitionSet ++= partitionList
        cond.signalAll()
      }
    } finally {
      lock.unlock()
    }
  }

  def shutdown() {
    info("shutting down")
    leaderFinderThread.shutdown()
    stopAllConnections()
    info("shutdown completed")
  }
}