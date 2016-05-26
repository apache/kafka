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

import scala.collection.mutable
import scala.collection.Set
import scala.collection.Map
import kafka.utils.Logging
import kafka.cluster.BrokerEndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.common.TopicAndPartition
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.utils.Utils

abstract class AbstractFetcherManager(protected val name: String, clientId: String, numFetchers: Int = 1)
  extends Logging with KafkaMetricsGroup {
  // map of (source broker_id, fetcher_id per source broker) => fetcher
  private val fetcherThreadMap = new mutable.HashMap[BrokerAndFetcherId, AbstractFetcherThread]
  private val mapLock = new Object
  this.logIdent = "[" + name + "] "

  newGauge(
    "MaxLag",
    new Gauge[Long] {
      // current max lag across all fetchers/topics/partitions
      def value = fetcherThreadMap.foldLeft(0L)((curMaxAll, fetcherThreadMapEntry) => {
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
      def value = {
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

  private def getFetcherId(topic: String, partitionId: Int) : Int = {
    Utils.abs(31 * topic.hashCode() + partitionId) % numFetchers
  }

  // to be defined in subclass to create a specific fetcher
  def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread

  def addFetcherForPartitions(partitionAndOffsets: Map[TopicAndPartition, BrokerAndInitialOffset]) {
    mapLock synchronized {
      val partitionsPerFetcher = partitionAndOffsets.groupBy{ case(topicAndPartition, brokerAndInitialOffset) =>
        BrokerAndFetcherId(brokerAndInitialOffset.broker, getFetcherId(topicAndPartition.topic, topicAndPartition.partition))}
      for ((brokerAndFetcherId, partitionAndOffsets) <- partitionsPerFetcher) {
        var fetcherThread: AbstractFetcherThread = null
        fetcherThreadMap.get(brokerAndFetcherId) match {
          case Some(f) => fetcherThread = f
          case None =>
            fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
            fetcherThreadMap.put(brokerAndFetcherId, fetcherThread)
            fetcherThread.start
        }

        fetcherThreadMap(brokerAndFetcherId).addPartitions(partitionAndOffsets.map { case (topicAndPartition, brokerAndInitOffset) =>
          topicAndPartition -> brokerAndInitOffset.initOffset
        })
      }
    }

    info("Added fetcher for partitions %s".format(partitionAndOffsets.map{ case (topicAndPartition, brokerAndInitialOffset) =>
      "[" + topicAndPartition + ", initOffset " + brokerAndInitialOffset.initOffset + " to broker " + brokerAndInitialOffset.broker + "] "}))
  }

  def removeFetcherForPartitions(partitions: Set[TopicAndPartition]) {
    mapLock synchronized {
      for ((key, fetcher) <- fetcherThreadMap) {
        fetcher.removePartitions(partitions)
      }
    }
    info("Removed fetcher for partitions %s".format(partitions.mkString(",")))
  }

  def shutdownIdleFetcherThreads() {
    mapLock synchronized {
      val keysToBeRemoved = new mutable.HashSet[BrokerAndFetcherId]
      for ((key, fetcher) <- fetcherThreadMap) {
        if (fetcher.partitionCount <= 0) {
          fetcher.shutdown()
          keysToBeRemoved += key
        }
      }
      fetcherThreadMap --= keysToBeRemoved
    }
  }

  def closeAllFetchers() {
    mapLock synchronized {
      for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.shutdown()
      }
      fetcherThreadMap.clear()
    }
  }
}

case class BrokerAndFetcherId(broker: BrokerEndPoint, fetcherId: Int)

case class BrokerAndInitialOffset(broker: BrokerEndPoint, initOffset: Long)
