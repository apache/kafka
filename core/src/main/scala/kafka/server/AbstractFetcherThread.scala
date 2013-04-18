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

import kafka.cluster.Broker
import kafka.common.{ClientIdAndBroker, TopicAndPartition, ErrorMapping}
import collection.mutable
import kafka.message.ByteBufferMessageSet
import kafka.message.MessageAndOffset
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import java.util.concurrent.atomic.AtomicLong
import kafka.utils.{Pool, ShutdownableThread}
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kafka.consumer.{PartitionTopicInfo, SimpleConsumer}
import kafka.api.{FetchRequest, FetchResponse, FetchResponsePartitionData, FetchRequestBuilder}


/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class AbstractFetcherThread(name: String, clientId: String, sourceBroker: Broker, socketTimeout: Int, socketBufferSize: Int,
                                     fetchSize: Int, fetcherBrokerId: Int = -1, maxWait: Int = 0, minBytes: Int = 1,
                                     isInterruptible: Boolean = true)
  extends ShutdownableThread(name, isInterruptible) {
  private val partitionMap = new mutable.HashMap[TopicAndPartition, Long] // a (topic, partition) -> offset map
  private val partitionMapLock = new ReentrantLock
  private val partitionMapCond = partitionMapLock.newCondition()
  val simpleConsumer = new SimpleConsumer(sourceBroker.host, sourceBroker.port, socketTimeout, socketBufferSize, clientId)
  private val brokerInfo = "host_%s-port_%s".format(sourceBroker.host, sourceBroker.port)
  private val metricId = new ClientIdAndBroker(clientId, brokerInfo)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)
  val fetchRequestBuilder = new FetchRequestBuilder().
          clientId(clientId).
          replicaId(fetcherBrokerId).
          maxWait(maxWait).
          minBytes(minBytes)

  /* callbacks to be defined in subclass */

  // process fetched data
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long,
                           partitionData: FetchResponsePartitionData)

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long

  // deal with partitions with errors, potentially due to leadership changes
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition])

  override def shutdown(){
    super.shutdown()
    simpleConsumer.close()
  }

  override def doWork() {
    partitionMapLock.lock()
    try {
      if (partitionMap.isEmpty)
        partitionMapCond.await(200L, TimeUnit.MILLISECONDS)
      partitionMap.foreach {
        case((topicAndPartition, offset)) =>
          fetchRequestBuilder.addFetch(topicAndPartition.topic, topicAndPartition.partition,
                           offset, fetchSize)
      }
    } finally {
      partitionMapLock.unlock()
    }

    val fetchRequest = fetchRequestBuilder.build()
    if (!fetchRequest.requestInfo.isEmpty)
      processFetchRequest(fetchRequest)
  }

  private def processFetchRequest(fetchRequest: FetchRequest) {
    val partitionsWithError = new mutable.HashSet[TopicAndPartition]
    var response: FetchResponse = null
    try {
      trace("issuing to broker %d of fetch request %s".format(sourceBroker.id, fetchRequest))
      response = simpleConsumer.fetch(fetchRequest)
    } catch {
      case t =>
        debug("error in fetch %s".format(fetchRequest), t)
        if (isRunning.get) {
          partitionMapLock synchronized {
            partitionsWithError ++= partitionMap.keys
          }
        }
    }
    fetcherStats.requestRate.mark()

    if (response != null) {
      // process fetched data
      partitionMapLock.lock()
      try {
        response.data.foreach {
          case(topicAndPartition, partitionData) =>
            val (topic, partitionId) = topicAndPartition.asTuple
            val currentOffset = partitionMap.get(topicAndPartition)
            // we append to the log if the current offset is defined and it is the same as the offset requested during fetch
            if (currentOffset.isDefined && fetchRequest.requestInfo(topicAndPartition).offset == currentOffset.get) {
              partitionData.error match {
                case ErrorMapping.NoError =>
                  val messages = partitionData.messages.asInstanceOf[ByteBufferMessageSet]
                  val validBytes = messages.validBytes
                  val newOffset = messages.lastOption match {
                    case Some(m: MessageAndOffset) => m.nextOffset
                    case None => currentOffset.get
                  }
                  partitionMap.put(topicAndPartition, newOffset)
                  fetcherLagStats.getFetcherLagStats(topic, partitionId).lag = partitionData.hw - newOffset
                  fetcherStats.byteRate.mark(validBytes)
                  // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                  processPartitionData(topicAndPartition, currentOffset.get, partitionData)
                case ErrorMapping.OffsetOutOfRangeCode =>
                  try {
                    val newOffset = handleOffsetOutOfRange(topicAndPartition)
                    partitionMap.put(topicAndPartition, newOffset)
                    warn("current offset %d for topic %s partition %d out of range; reset offset to %d"
                      .format(currentOffset.get, topic, partitionId, newOffset))
                  } catch {
                    case e =>
                      warn("error getting offset for %s %d to broker %d".format(topic, partitionId, sourceBroker.id), e)
                      partitionsWithError += topicAndPartition
                  }
                case _ =>
                  warn("error for %s %d to broker %d".format(topic, partitionId, sourceBroker.id),
                    ErrorMapping.exceptionFor(partitionData.error))
                  partitionsWithError += topicAndPartition
              }
            }
        }
      } finally {
        partitionMapLock.unlock()
      }
    }

    if(partitionsWithError.size > 0) {
      debug("handling partitions with error for %s".format(partitionsWithError))
      handlePartitionsWithErrors(partitionsWithError)
    }
  }

  def addPartition(topic: String, partitionId: Int, initialOffset: Long) {
    partitionMapLock.lock()
    try {
      val topicPartition = TopicAndPartition(topic, partitionId)
      partitionMap.put(
          topicPartition,
          if (PartitionTopicInfo.isOffsetInvalid(initialOffset)) handleOffsetOutOfRange(topicPartition) else initialOffset)
      partitionMapCond.signalAll()
    } finally {
      partitionMapLock.unlock()
    }
  }

  def removePartition(topic: String, partitionId: Int) {
    partitionMapLock.lock()
    try {
      partitionMap.remove(TopicAndPartition(topic, partitionId))
    } finally {
      partitionMapLock.unlock()
    }
  }

  def hasPartition(topic: String, partitionId: Int): Boolean = {
    partitionMapLock.lock()
    try {
      partitionMap.get(TopicAndPartition(topic, partitionId)).isDefined
    } finally {
      partitionMapLock.unlock()
    }
  }

  def partitionCount() = {
    partitionMapLock.lock()
    try {
      partitionMap.size
    } finally {
      partitionMapLock.unlock()
    }
  }
}

class FetcherLagMetrics(metricId: ClientIdBrokerTopicPartition) extends KafkaMetricsGroup {
  private[this] var lagVal = new AtomicLong(-1L)
  newGauge(
    metricId + "-ConsumerLag",
    new Gauge[Long] {
      def getValue = lagVal.get
    }
  )

  def lag_=(newLag: Long) {
    lagVal.set(newLag)
  }

  def lag = lagVal.get
}

class FetcherLagStats(metricId: ClientIdAndBroker) {
  private val valueFactory = (k: ClientIdBrokerTopicPartition) => new FetcherLagMetrics(k)
  private val stats = new Pool[ClientIdBrokerTopicPartition, FetcherLagMetrics](Some(valueFactory))

  def getFetcherLagStats(topic: String, partitionId: Int): FetcherLagMetrics = {
    stats.getAndMaybePut(new ClientIdBrokerTopicPartition(metricId.clientId, metricId.brokerInfo, topic, partitionId))
  }
}

class FetcherStats(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val requestRate = newMeter(metricId + "-RequestsPerSec", "requests", TimeUnit.SECONDS)
  val byteRate = newMeter(metricId + "-BytesPerSec", "bytes", TimeUnit.SECONDS)
}

case class ClientIdBrokerTopicPartition(clientId: String, brokerInfo: String, topic: String, partitionId: Int) {
  override def toString = "%s-%s-%s-%d".format(clientId, brokerInfo, topic, partitionId)
}

