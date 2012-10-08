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
import kafka.consumer.SimpleConsumer
import kafka.common.{TopicAndPartition, ErrorMapping}
import collection.mutable
import kafka.message.ByteBufferMessageSet
import kafka.message.MessageAndOffset
import kafka.api.{FetchResponse, FetchResponsePartitionData, FetchRequestBuilder}
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import java.util.concurrent.atomic.AtomicLong
import kafka.utils.{Pool, ShutdownableThread}
import java.util.concurrent.TimeUnit


/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class  AbstractFetcherThread(name: String, sourceBroker: Broker, socketTimeout: Int, socketBufferSize: Int,
                                     fetchSize: Int, fetcherBrokerId: Int = -1, maxWait: Int = 0, minBytes: Int = 1)
  extends ShutdownableThread(name) {

  private val fetchMap = new mutable.HashMap[TopicAndPartition, Long] // a (topic, partition) -> offset map
  private val fetchMapLock = new Object
  val simpleConsumer = new SimpleConsumer(sourceBroker.host, sourceBroker.port, socketTimeout, socketBufferSize)
  val fetcherMetrics = FetcherStat.getFetcherStat(name + "-" + sourceBroker.id)
  
  /* callbacks to be defined in subclass */

  // process fetched data
  def processPartitionData(topic: String, fetchOffset: Long, partitionData: FetchResponsePartitionData)

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topic: String, partitionId: Int): Long

  // deal with partitions with errors, potentially due to leadership changes
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition])

  override def shutdown(){
    super.shutdown()
    simpleConsumer.close()
  }

  override def doWork() {
    val builder = new FetchRequestBuilder().
            clientId(name).
            replicaId(fetcherBrokerId).
            maxWait(maxWait).
            minBytes(minBytes)

    fetchMapLock synchronized {
      fetchMap.foreach {
        case((topicAndPartition, offset)) =>
          builder.addFetch(topicAndPartition.topic, topicAndPartition.partition,
                           offset, fetchSize)
      }
    }

    val fetchRequest = builder.build()
    val partitionsWithError = new mutable.HashSet[TopicAndPartition]
    var response: FetchResponse = null
    try {
      trace("issuing to broker %d of fetch request %s".format(sourceBroker.id, fetchRequest))
      response = simpleConsumer.fetch(fetchRequest)
    } catch {
      case t =>
        debug("error in fetch %s".format(fetchRequest), t)
        if (isRunning.get) {
          fetchMapLock synchronized {
            partitionsWithError ++= fetchMap.keys
            fetchMap.clear()
          }
        }
    }
    fetcherMetrics.requestRate.mark()

    if (response != null) {
      // process fetched data
      fetchMapLock synchronized {
        response.data.foreach {
          case(topicAndPartition, partitionData) =>
            val (topic, partitionId) = topicAndPartition.asTuple
            val currentOffset = fetchMap.get(topicAndPartition)
            if (currentOffset.isDefined) {
              partitionData.error match {
                case ErrorMapping.NoError =>
                  val messages = partitionData.messages.asInstanceOf[ByteBufferMessageSet]
                  val validBytes = messages.validBytes
                  val newOffset = messages.lastOption match {
                    case Some(m: MessageAndOffset) => m.nextOffset
                    case None => currentOffset.get
                  }
                  fetchMap.put(topicAndPartition, newOffset)
                  FetcherLagMetrics.getFetcherLagMetrics(topic, partitionId).lag = partitionData.hw - newOffset
                  fetcherMetrics.byteRate.mark(validBytes)
                  // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                  processPartitionData(topic, currentOffset.get, partitionData)
                case ErrorMapping.OffsetOutOfRangeCode =>
                  val newOffset = handleOffsetOutOfRange(topic, partitionId)
                  fetchMap.put(topicAndPartition, newOffset)
                  warn("current offset %d for topic %s partition %d out of range; reset offset to %d"
                    .format(currentOffset.get, topic, partitionId, newOffset))
                case _ =>
                  error("error for %s %d to broker %d".format(topic, partitionId, sourceBroker.id),
                    ErrorMapping.exceptionFor(partitionData.error))
                  partitionsWithError += topicAndPartition
                  fetchMap.remove(topicAndPartition)
              }
            }
        }
      }
    }

    if(partitionsWithError.size > 0) {
      debug("handling partitions with error for %s".format(partitionsWithError))
      handlePartitionsWithErrors(partitionsWithError)
    }
  }

  def addPartition(topic: String, partitionId: Int, initialOffset: Long) {
    fetchMapLock synchronized {
      fetchMap.put(TopicAndPartition(topic, partitionId), initialOffset)
    }
  }

  def removePartition(topic: String, partitionId: Int) {
    fetchMapLock synchronized {
      fetchMap.remove(TopicAndPartition(topic, partitionId))
    }
  }

  def hasPartition(topic: String, partitionId: Int): Boolean = {
    fetchMapLock synchronized {
      fetchMap.get(TopicAndPartition(topic, partitionId)).isDefined
    }
  }

  def partitionCount() = {
    fetchMapLock synchronized {
      fetchMap.size
    }
  }
}

class FetcherLagMetrics(name: (String, Int)) extends KafkaMetricsGroup {
  private[this] var lagVal = new AtomicLong(-1L)
  newGauge(
    name._1 + "-" + name._2 + "-ConsumerLag",
    new Gauge[Long] {
      def value() = lagVal.get
    }
  )

  def lag_=(newLag: Long) {
    lagVal.set(newLag)
  }

  def lag = lagVal.get
}

object FetcherLagMetrics {
  private val valueFactory = (k: (String, Int)) => new FetcherLagMetrics(k)
  private val stats = new Pool[(String, Int), FetcherLagMetrics](Some(valueFactory))

  def getFetcherLagMetrics(topic: String, partitionId: Int): FetcherLagMetrics = {
    stats.getAndMaybePut( (topic, partitionId) )
  }
}

class FetcherStat(name: String) extends KafkaMetricsGroup {
  val requestRate = newMeter(name + "RequestsPerSec",  "requests", TimeUnit.SECONDS)
  val byteRate = newMeter(name + "BytesPerSec",  "bytes", TimeUnit.SECONDS)
}

object FetcherStat {
  private val valueFactory = (k: String) => new FetcherStat(k)
  private val stats = new Pool[String, FetcherStat](Some(valueFactory))

  def getFetcherStat(name: String): FetcherStat = {
    stats.getAndMaybePut(name)
  }
}
