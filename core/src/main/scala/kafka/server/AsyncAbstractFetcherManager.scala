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

import com.yammer.metrics.core.Gauge
import kafka.cluster.BrokerEndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Logging
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{KafkaFuture, TopicPartition}

import scala.collection.{Map, mutable}

abstract class AsyncAbstractFetcherManager[T <: FetcherEventManager](val name: String, clientId: String, numFetchers: Int)
  extends FetcherManagerTrait with Logging with KafkaMetricsGroup {
  // map of (source broker_id, fetcher_id per source broker) => fetcher.
  // package private for test
  private[server] val fetcherThreadMap = new mutable.HashMap[BrokerIdAndFetcherId, T]
  private val lock = new Object
  private val numFetchersPerBroker = numFetchers
  val failedPartitions = new FailedPartitions
  this.logIdent = "[" + name + "] "

  newGauge(
    "MaxLag",
    new Gauge[Long] {
      // current max lag across all fetchers/topics/partitions
      def value: Long = fetcherThreadMap.foldLeft(0L)((curMaxAll, fetcherThreadMapEntry) => {
        fetcherThreadMapEntry._2.fetcherLagStats.stats.foldLeft(0L)((curMaxThread, fetcherLagStatsEntry) => {
          curMaxThread.max(fetcherLagStatsEntry._2.lag)
        }).max(curMaxAll)
      })
    },
    Map("clientId" -> clientId)
  )

  newGauge(
  "MinFetchRate", {
    new Gauge[Double] {
      // current min fetch rate across all fetchers/topics/partitions
      def value: Double = {
        val headRate: Double =
          fetcherThreadMap.headOption.map(_._2.fetcherStats.requestRate.oneMinuteRate).getOrElse(0)

        fetcherThreadMap.foldLeft(headRate)((curMinAll, fetcherThreadMapEntry) => {
          fetcherThreadMapEntry._2.fetcherStats.requestRate.oneMinuteRate.min(curMinAll)
        })
      }
    }
  },
  Map("clientId" -> clientId)
  )

  newGauge(
    "FetchFailureRate", {
      new Gauge[Double] {
        // fetch failure rate sum across all fetchers/topics/partitions
        def value: Double = {
          val headRate: Double =
            fetcherThreadMap.headOption.map(_._2.fetcherStats.requestFailureRate.oneMinuteRate).getOrElse(0)

          fetcherThreadMap.foldLeft(headRate)((curSum, fetcherThreadMapEntry) => {
            fetcherThreadMapEntry._2.fetcherStats.requestRate.oneMinuteRate + curSum
          })
        }
      }
    },
    Map("clientId" -> clientId)
  )

  val failedPartitionsCount = newGauge(
    "FailedPartitionsCount", {
      new Gauge[Int] {
        def value: Int = failedPartitions.size
      }
    },
    Map("clientId" -> clientId)
  )

  newGauge("DeadThreadCount", {
    new Gauge[Int] {
      def value: Int = {
        deadThreadCount
      }
    }
  }, Map("clientId" -> clientId))

  private[server] def deadThreadCount: Int = lock synchronized { fetcherThreadMap.values.count(_.isThreadFailed) }

  /*
  def resizeThreadPool(newSize: Int): Unit = {
    def migratePartitions(newSize: Int): Unit = {
      fetcherThreadMap.foreach { case (id, thread) =>
        val removedPartitions = thread.partitionsAndOffsets
        removeFetcherForPartitions(removedPartitions.keySet)
        if (id.fetcherId >= newSize)
          thread.shutdown()
        addFetcherForPartitions(removedPartitions)
      }
    }
    lock synchronized {
      val currentSize = numFetchersPerBroker
      info(s"Resizing fetcher thread pool size from $currentSize to $newSize")
      numFetchersPerBroker = newSize
      if (newSize != currentSize) {
        // We could just migrate some partitions explicitly to new threads. But this is currently
        // reassigning all partitions using the new thread size so that hash-based allocation
        // works with partition add/delete as it did before.
        migratePartitions(newSize)
      }
      shutdownIdleFetcherThreads()
    }
  }

  // Visible for testing
  private[server] def getFetcher(topicPartition: TopicPartition): Option[T] = {
    lock synchronized {
      fetcherThreadMap.values.find { fetcherThread =>
        fetcherThread.fetchState(topicPartition).isDefined
      }
    }
  }
   */

  // Visibility for testing
  private[server] def getFetcherId(topicPartition: TopicPartition): Int = {
    lock synchronized {
      Utils.abs(31 * topicPartition.topic.hashCode() + topicPartition.partition) % numFetchersPerBroker
    }
  }

  /*
  // This method is only needed by ReplicaAlterDirManager
  def markPartitionsForTruncation(brokerId: Int, topicPartition: TopicPartition, truncationOffset: Long): Unit = {
    lock synchronized {
      val fetcherId = getFetcherId(topicPartition)
      val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerId, fetcherId)
      fetcherThreadMap.get(brokerIdAndFetcherId).foreach { thread =>
        thread.markPartitionsForTruncation(topicPartition, truncationOffset)
      }
    }
  }
   */

  // to be defined in subclass to create a specific fetcher
  def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): T

  def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState],
    partitionModifications: PartitionModifications): Unit = {
    lock synchronized {
      val partitionsPerFetcher = partitionAndOffsets.groupBy { case (topicPartition, brokerAndInitialFetchOffset) =>
        BrokerAndFetcherId(brokerAndInitialFetchOffset.leader, getFetcherId(topicPartition))
      }

      def addAndStartFetcherThread(brokerAndFetcherId: BrokerAndFetcherId,
                                   brokerIdAndFetcherId: BrokerIdAndFetcherId): T = {
        val fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
        fetcherThreadMap.put(brokerIdAndFetcherId, fetcherThread)
        fetcherThread.start()
        fetcherThread
      }

      for ((brokerAndFetcherId, initialFetchOffsets) <- partitionsPerFetcher) {
        val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerAndFetcherId.broker.id, brokerAndFetcherId.fetcherId)
        fetcherThreadMap.get(brokerIdAndFetcherId) match {
          case Some(currentFetcherThread) if currentFetcherThread.sourceBroker == brokerAndFetcherId.broker =>
            // reuse the fetcher thread
          case Some(f) =>
            f.close()
            addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
          case None =>
            addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
        }

        val initialOffsetAndEpochs = initialFetchOffsets.map { case (tp, brokerAndInitOffset) =>
          tp -> FollowerPartitionStateInFetcher(brokerIdAndFetcherId, OffsetAndEpoch(brokerAndInitOffset.initOffset, brokerAndInitOffset.currentLeaderEpoch))
        }

        partitionModifications.partitionsToMakeFollowerWithOffsetAndEpoch ++= initialOffsetAndEpochs
      }
    }
  }

  def modifyPartitionsAndShutdownIdleFetchers(partitionModifications: PartitionModifications): Unit = {
    lock synchronized {
      val futures = mutable.Map[BrokerIdAndFetcherId, KafkaFuture[Int]]()

      for ((key, fetcher) <- fetcherThreadMap) {
        futures.put(key, fetcher.modifyPartitionsAndGetCount(partitionModifications))
      }

      for ((key, partitionCountFuture) <- futures) {
        if (partitionCountFuture.get() == 0) {
          fetcherThreadMap.get(key).get.close()
          fetcherThreadMap -= key
        }
      }
      failedPartitions.removeAll(partitionModifications.partitionsToRemove)
    }

    if (partitionModifications.partitionsToRemove.nonEmpty) {
      info(s"Removed fetcher for partitions ${partitionModifications.partitionsToRemove}")
    }
    if (partitionModifications.partitionsToMakeFollowerWithOffsetAndEpoch.nonEmpty) {
      info(s"Added fetcher for partitions ${partitionModifications.partitionsToMakeFollowerWithOffsetAndEpoch}")
    }
  }

  def closeAllFetchers(): Unit = {
    lock synchronized {
      for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.close()
      }

      fetcherThreadMap.clear()
    }
  }
}
