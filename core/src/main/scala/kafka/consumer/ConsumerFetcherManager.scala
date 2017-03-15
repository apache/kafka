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

import kafka.server.{AbstractFetcherManager, AbstractFetcherThread, BrokerAndInitialOffset}
import kafka.cluster.{BrokerEndPoint, Cluster}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import java.util.concurrent.locks.ReentrantLock

import kafka.utils.CoreUtils.inLock
import kafka.utils.ZkUtils
import kafka.utils.ShutdownableThread
import kafka.client.ClientUtils
import java.util.concurrent.atomic.AtomicInteger

/**
 *  Usage:
 *  Once ConsumerFetcherManager is created, startConnections() and stopAllConnections() can be called repeatedly
 *  until shutdown() is called.
 */
class ConsumerFetcherManager(private val consumerIdString: String,
                             private val config: ConsumerConfig,
                             private val zkUtils : ZkUtils)
        extends AbstractFetcherManager("ConsumerFetcherManager-%d".format(Time.SYSTEM.milliseconds),
                                       config.clientId, config.numConsumerFetchers) {
  private var partitionMap: Map[TopicPartition, PartitionTopicInfo] = null
  private var cluster: Cluster = null
  private val noLeaderPartitionSet = new scala.collection.mutable.HashSet[TopicPartition]
  private val lock = new ReentrantLock
  private val cond = lock.newCondition()
  private var leaderFinderThread: ShutdownableThread = null
  private val correlationId = new AtomicInteger(0)

  private class LeaderFinderThread(name: String) extends ShutdownableThread(name) {
    // thread responsible for adding the fetcher to the right broker when leader is available
    override def doWork() {
      lock.lock()
      val leaderForPartitionsMap = try {
        while (noLeaderPartitionSet.isEmpty) {
          trace("No partition for leader election.")
          cond.await()
        }

        trace(s"Partitions without leader: $noLeaderPartitionSet")
        val brokers = ClientUtils.getPlaintextBrokerEndPoints(zkUtils)
        val topicsMetadata =
          ClientUtils.fetchTopicMetadata(
            noLeaderPartitionSet.map(m => m.topic).toSet,
            brokers,
            config.clientId,
            config.socketTimeoutMs,
            correlationId.getAndIncrement
          ).topicsMetadata

        debug(s"${topicsMetadata.mkString("\n")}")

        val leaderForPartitionSeq = for {
          tmd <- topicsMetadata
          pmd <- tmd.partitionsMetadata
          topicPartition = new TopicPartition(tmd.topic, pmd.partitionId) if noLeaderPartitionSet.contains(topicPartition)
          leaderBroker <- pmd.leader
        } yield {
          noLeaderPartitionSet.remove(topicPartition)
          (topicPartition, leaderBroker)
        }

        leaderForPartitionSeq.toMap
      } catch {
        case t: Throwable => {
          if (!isRunning.get()) {
            throw t /* If this thread is stopped, propagate this exception to kill the thread. */
          } else {
            warn(s"Failed to find leader for $noLeaderPartitionSet", t)
          }
          Map.empty[TopicPartition, BrokerEndPoint]
        }
      } finally {
        lock.unlock()
      }

      try {
        addFetcherForPartitions(
          leaderForPartitionsMap.map {
            case (topicPartition, broker) =>
              topicPartition -> BrokerAndInitialOffset(broker, partitionMap(topicPartition).getFetchOffset())
          }
        )
      } catch {
        case t: Throwable =>
          if (!isRunning.get())
            throw t /* If this thread is stopped, propagate this exception to kill the thread. */
          else {
            warn("Failed to add leader for partitions %s; will retry".format(leaderForPartitionsMap.keySet.mkString(",")), t)
            lock.lock()
            noLeaderPartitionSet ++= leaderForPartitionsMap.keySet
            lock.unlock()
          }
        }

      shutdownIdleFetcherThreads()
      Thread.sleep(config.refreshLeaderBackoffMs)
    }
  }

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
    new ConsumerFetcherThread(
      "ConsumerFetcherThread-%s-%d-%d".format(consumerIdString, fetcherId, sourceBroker.id),
      config, sourceBroker, partitionMap, this)
  }

  def startConnections(topicInfos: Iterable[PartitionTopicInfo], cluster: Cluster) {
    leaderFinderThread = new LeaderFinderThread(consumerIdString + "-leader-finder-thread")
    leaderFinderThread.start()

    inLock(lock) {
      partitionMap = topicInfos.map(tpi => (new TopicPartition(tpi.topic, tpi.partitionId), tpi)).toMap
      this.cluster = cluster
      noLeaderPartitionSet ++= topicInfos.map(tpi => new TopicPartition(tpi.topic, tpi.partitionId))
      cond.signalAll()
    }
  }

  def stopConnections() {
    /*
     * Stop the leader finder thread first before stopping fetchers. Otherwise, if there are more partitions without
     * leader, then the leader finder thread will process these partitions (before shutting down) and add fetchers for
     * these partitions.
     */
    info("Stopping leader finder thread")
    if (leaderFinderThread != null) {
      leaderFinderThread.shutdown()
      leaderFinderThread = null
    }

    info("Stopping all fetchers")
    closeAllFetchers()

    // no need to hold the lock for the following since leaderFindThread and all fetchers have been stopped
    partitionMap = null
    noLeaderPartitionSet.clear()

    info("All connections stopped")
  }

  def addPartitionsWithError(partitionList: Iterable[TopicPartition]) {
    debug("adding partitions with error %s".format(partitionList))
    inLock(lock) {
      if (partitionMap != null) {
        noLeaderPartitionSet ++= partitionList
        cond.signalAll()
      }
    }
  }
}
