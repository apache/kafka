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

import kafka.cluster.BrokerEndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Logging
import org.apache.kafka.common.{KafkaFuture, TopicPartition}
import org.apache.kafka.common.utils.Utils

import scala.collection.{Map, Set, mutable}

abstract class AsyncAbstractFetcherManager[T <: FetcherEventManager](val name: String, clientId: String, numFetchers: Int)
  extends FetcherManagerTrait with Logging with KafkaMetricsGroup {
  // map of (source broker_id, fetcher_id per source broker) => fetcher.
  // package private for test
  private[server] val fetcherThreadMap = new mutable.HashMap[BrokerIdAndFetcherId, T]
  private val lock = new Object
  private val numFetchersPerBroker = numFetchers
  val failedPartitions = new FailedPartitions
  this.logIdent = "[" + name + "] "

  private val tags = Map("clientId" -> clientId)

  newGauge("MaxLag", () => {
    // current max lag across all fetchers/topics/partitions
    fetcherThreadMap.values.foldLeft(0L) { (curMaxLagAll, fetcherThread) =>
      val maxLagThread = fetcherThread.fetcherLagStats.stats.values.foldLeft(0L)((curMaxLagThread, lagMetrics) =>
        math.max(curMaxLagThread, lagMetrics.lag))
      math.max(curMaxLagAll, maxLagThread)
    }
  }, tags)

  newGauge("FetchFailureRate", () => {
    // fetch failure rate sum across all fetchers/topics/partitions
    val headRate = fetcherThreadMap.headOption.map(_._2.fetcherStats.requestFailureRate.oneMinuteRate).getOrElse(0.0)
    fetcherThreadMap.foldLeft(headRate)((curSum, fetcherThreadMapEntry) => {
      fetcherThreadMapEntry._2.fetcherStats.requestRate.oneMinuteRate + curSum
    })
  }, tags)

  newGauge("MinFetchRate", () => {
    // current min fetch rate across all fetchers/topics/partitions
    val headRate = fetcherThreadMap.values.headOption.map(_.fetcherStats.requestRate.oneMinuteRate).getOrElse(0.0)
    fetcherThreadMap.values.foldLeft(headRate)((curMinAll, fetcherThread) =>
      math.min(curMinAll, fetcherThread.fetcherStats.requestRate.oneMinuteRate))
  }, tags)

  newGauge("FailedPartitionsCount", () => failedPartitions.size, tags)

  newGauge("DeadThreadCount", () => deadThreadCount, tags)

  private[server] def deadThreadCount: Int = lock synchronized { fetcherThreadMap.values.count(_.isThreadFailed) }

  /*
  def resizeThreadPool(newSize: Int): Unit = {
    def migratePartitions(newSize: Int): Unit = {
      fetcherThreadMap.forKeyValue { (id, thread) =>
        val partitionStates = removeFetcherForPartitions(thread.partitions)
        if (id.fetcherId >= newSize)
          thread.shutdown()
        val fetchStates = partitionStates.map { case (topicPartition, currentFetchState) =>
          val initialFetchState = InitialFetchState(thread.sourceBroker,
            currentLeaderEpoch = currentFetchState.currentLeaderEpoch,
            initOffset = currentFetchState.fetchOffset)
          topicPartition -> initialFetchState
        }
        addFetcherForPartitions(fetchStates)
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
  */

  // Visible for testing
  private[server] def getFetcher(topicPartition: TopicPartition): Option[T] = {
    lock synchronized {
      fetcherThreadMap.values.find { fetcherThread =>
        fetcherThread.fetchState(topicPartition).isDefined
      }
    }
  }

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

  def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
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

      val addPartitionFutures = mutable.Buffer[KafkaFuture[Void]]()
      for ((brokerAndFetcherId, initialFetchOffsets) <- partitionsPerFetcher) {
        val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerAndFetcherId.broker.id, brokerAndFetcherId.fetcherId)
        val fetcherThread = fetcherThreadMap.get(brokerIdAndFetcherId) match {
          case Some(currentFetcherThread) if currentFetcherThread.sourceBroker == brokerAndFetcherId.broker =>
            // reuse the fetcher thread
            currentFetcherThread
          case Some(f) =>
            f.close()
            addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
          case None =>
            addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
        }

        addPartitionFutures += addPartitionsToFetcherThread(fetcherThread, initialFetchOffsets)
      }

      // wait for all futures to finish
      KafkaFuture.allOf(addPartitionFutures:_*).get()
    }
  }

  def addFailedPartition(topicPartition: TopicPartition): Unit = {
    lock synchronized {
      failedPartitions.add(topicPartition)
    }
  }

  protected def addPartitionsToFetcherThread(fetcherThread: T,
                                             initialOffsetAndEpochs: collection.Map[TopicPartition, InitialFetchState]): KafkaFuture[Void] = {
    val future = fetcherThread.addPartitions(initialOffsetAndEpochs)
    info(s"Added fetcher to broker ${fetcherThread.sourceBroker.id} for partitions $initialOffsetAndEpochs")
    future
  }

  def removeFetcherForPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, PartitionFetchState] = {
    val fetchStates = mutable.Map.empty[TopicPartition, PartitionFetchState]
    lock synchronized {
      val futures = mutable.Buffer[KafkaFuture[Map[TopicPartition, PartitionFetchState]]]()
      for (fetcher <- fetcherThreadMap.values)
        futures += fetcher.removePartitions(partitions)
      for (future <- futures)
        fetchStates ++= future.get()
      failedPartitions.removeAll(partitions)
    }

    if (partitions.nonEmpty)
      info(s"Removed fetcher for partitions $partitions")
    fetchStates
  }

  def shutdownIdleFetcherThreads(): Unit = {
    lock synchronized {
      val futures = mutable.Map[BrokerIdAndFetcherId, KafkaFuture[Int]]()

      for ((key, fetcher) <- fetcherThreadMap) {
        futures.put(key, fetcher.getPartitionsCount())
      }

      for ((key, partitionCountFuture) <- futures) {
        if (partitionCountFuture.get() == 0) {
          fetcherThreadMap.get(key).get.close()
          fetcherThreadMap -= key
        }
      }
    }
  }

  def closeAllFetchers(): Unit = {
    lock synchronized {
      for ((_, fetcher) <- fetcherThreadMap) {
        fetcher.initiateShutdown()
      }

      for ((_, fetcher) <- fetcherThreadMap) {
        fetcher.awaitShutdown()
      }
      fetcherThreadMap.clear()
    }
  }
}
