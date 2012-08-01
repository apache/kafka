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
import scala.collection.mutable
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.AtomicBoolean
import kafka.utils.ZkUtils._
import kafka.utils.SystemTime
import java.util.concurrent.CountDownLatch

/**
 *  Usage:
 *  Once ConsumerFetcherManager is created, startConnections() and stopAllConnections() can be called repeatedly
 *  until shutdown() is called.
 */
class ConsumerFetcherManager(private val consumerIdString: String,
                             private val config: ConsumerConfig,
                             private val zkClient : ZkClient)
        extends AbstractFetcherManager("ConsumerFetcherManager-%d".format(SystemTime.milliseconds), 1) {
  private var partitionMap: immutable.Map[(String, Int), PartitionTopicInfo] = null
  private var cluster: Cluster = null
  private val noLeaderPartitionSet = new mutable.HashSet[(String, Int)]
  private val lock = new ReentrantLock
  private val cond = lock.newCondition()
  private val isShuttingDown = new AtomicBoolean(false)
  private val leaderFinderThreadShutdownLatch = new CountDownLatch(1)  
  private val leaderFinderThread = new Thread(consumerIdString + "_leader_finder_thread") {
    // thread responsible for adding the fetcher to the right broker when leader is available
    override def run() {
      info("starting %s".format(getName))
      while (!isShuttingDown.get) {
        try {
          lock.lock()
          try {
            if (noLeaderPartitionSet.isEmpty)
              cond.await()
            for ((topic, partitionId) <- noLeaderPartitionSet) {
              // find the leader for this partition
              getLeaderForPartition(zkClient, topic, partitionId) match {
                case Some(leaderId) =>
                  cluster.getBroker(leaderId) match {
                    case Some(broker) =>
                      val pti = partitionMap((topic, partitionId))
                      addFetcher(topic, partitionId, pti.getFetchOffset(), broker)
                      noLeaderPartitionSet.remove((topic, partitionId))
                    case None =>
                      error("Broker %d is unavailable, fetcher for topic %s partition %d could not be started"
                            .format(leaderId, topic, partitionId))
                  }
                case None => // let it go since we will keep retrying
              }
            }
          } finally {
            lock.unlock()
          }
          Thread.sleep(config.refreshLeaderBackoffMs)
        } catch {
          case t =>
            if (!isShuttingDown.get())
              error("error in %s".format(getName), t)
        }
      }
      leaderFinderThreadShutdownLatch.countDown()
      info("stopping %s".format(getName))
    }
  }
  leaderFinderThread.start()


  override def createFetcherThread(fetcherId: Int, sourceBroker: Broker): AbstractFetcherThread = {
    new ConsumerFetcherThread("ConsumerFetcherThread-%s-%d-%d".format(consumerIdString, fetcherId, sourceBroker.id), config, sourceBroker, this)
  }

  def startConnections(topicInfos: Iterable[PartitionTopicInfo], cluster: Cluster) {
    if (isShuttingDown.get)
      throw new RuntimeException("%s already shutdown".format(name))
    lock.lock()
    try {
      partitionMap = topicInfos.map(tpi => ((tpi.topic, tpi.partitionId), tpi)).toMap
      this.cluster = cluster
      noLeaderPartitionSet ++= topicInfos.map(tpi => (tpi.topic, tpi.partitionId))
      cond.signalAll()
    } finally {
      lock.unlock()
    }
  }

  def stopAllConnections() {
    // first, clear noLeaderPartitionSet so that no more fetchers can be added to leader_finder_thread
    lock.lock()
    noLeaderPartitionSet.clear()
    lock.unlock()

    // second, stop all existing fetchers
    closeAllFetchers()

    // finally clear partitionMap
    lock.lock()
    partitionMap = null
    lock.unlock()
  }

  def getPartitionTopicInfo(key: (String, Int)) : PartitionTopicInfo = {
    var pti :PartitionTopicInfo =null
    lock.lock()
    try {
      pti = partitionMap(key)
    } finally {
      lock.unlock()
    }
    pti      
  }

  def addPartitionsWithError(partitionList: Iterable[(String, Int)]) {
    debug("adding partitions with error %s".format(partitionList))
    lock.lock()
    try {
      if (partitionMap != null) {
        noLeaderPartitionSet ++= partitionList
        cond.signalAll()
      }
    } finally {
      lock.unlock()
    }
  }

  def shutdown() {
    info("shutting down")
    isShuttingDown.set(true)
    leaderFinderThread.interrupt()
    leaderFinderThreadShutdownLatch.await()
    stopAllConnections()
    info("shutdown completed")
  }
}