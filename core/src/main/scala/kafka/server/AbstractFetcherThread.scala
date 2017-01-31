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

import java.util.concurrent.locks.ReentrantLock

import kafka.cluster.BrokerEndPoint
import kafka.consumer.PartitionTopicInfo
import kafka.utils.{DelayedItem, Pool, ShutdownableThread}
import kafka.common.{ClientIdAndBroker, KafkaException}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.protocol.Errors
import AbstractFetcherThread._

import scala.collection.{Map, Set, mutable}
import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.{FatalExitError, PartitionStates}
import org.apache.kafka.common.record.MemoryRecords

/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class AbstractFetcherThread(name: String,
                                     clientId: String,
                                     sourceBroker: BrokerEndPoint,
                                     fetchBackOffMs: Int = 0,
                                     isInterruptible: Boolean = true)
  extends ShutdownableThread(name, isInterruptible) {

  type REQ <: FetchRequest
  type PD <: PartitionData

  private val partitionStates = new PartitionStates[PartitionFetchState]
  private val partitionMapLock = new ReentrantLock
  private val partitionMapCond = partitionMapLock.newCondition()

  private val metricId = new ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)

  /* callbacks to be defined in subclass */

  // process fetched data
  def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: PD)

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topicPartition: TopicPartition): Long

  // deal with partitions with errors, potentially due to leadership changes
  def handlePartitionsWithErrors(partitions: Iterable[TopicPartition])

  protected def buildFetchRequest(partitionMap: Seq[(TopicPartition, PartitionFetchState)]): REQ

  protected def fetch(fetchRequest: REQ): Seq[(TopicPartition, PD)]

  override def shutdown(){
    initiateShutdown()
    inLock(partitionMapLock) {
      partitionMapCond.signalAll()
    }
    awaitShutdown()

    // we don't need the lock since the thread has finished shutdown and metric removal is safe
    fetcherStats.unregister()
    fetcherLagStats.unregister()
  }

  override def doWork() {

    val fetchRequest = inLock(partitionMapLock) {
      val fetchRequest = buildFetchRequest(partitionStates.partitionStates.asScala.map { state =>
        state.topicPartition -> state.value
      })
      if (fetchRequest.isEmpty) {
        trace("There are no active partitions. Back off for %d ms before sending a fetch request".format(fetchBackOffMs))
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }
      fetchRequest
    }
    if (!fetchRequest.isEmpty)
      processFetchRequest(fetchRequest)
  }

  private def processFetchRequest(fetchRequest: REQ) {
    val partitionsWithError = mutable.Set[TopicPartition]()

    def updatePartitionsWithError(partition: TopicPartition): Unit = {
      partitionsWithError += partition
      partitionStates.moveToEnd(partition)
    }

    var responseData: Seq[(TopicPartition, PD)] = Seq.empty

    try {
      trace("Issuing to broker %d of fetch request %s".format(sourceBroker.id, fetchRequest))
      responseData = fetch(fetchRequest)
    } catch {
      case t: Throwable =>
        if (isRunning.get) {
          warn(s"Error in fetch $fetchRequest", t)
          inLock(partitionMapLock) {
            partitionStates.partitionSet.asScala.foreach(updatePartitionsWithError)
            // there is an error occurred while fetching partitions, sleep a while
            // note that `ReplicaFetcherThread.handlePartitionsWithError` will also introduce the same delay for every
            // partition with error effectively doubling the delay. It would be good to improve this.
            partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
          }
        }
    }
    fetcherStats.requestRate.mark()

    if (responseData.nonEmpty) {
      // process fetched data
      inLock(partitionMapLock) {

        responseData.foreach { case (topicPartition, partitionData) =>
          val topic = topicPartition.topic
          val partitionId = topicPartition.partition
          Option(partitionStates.stateValue(topicPartition)).foreach(currentPartitionFetchState =>
            // we append to the log if the current offset is defined and it is the same as the offset requested during fetch
            if (fetchRequest.offset(topicPartition) == currentPartitionFetchState.offset) {
              Errors.forCode(partitionData.errorCode) match {
                case Errors.NONE =>
                  try {
                    val records = partitionData.toRecords
                    val newOffset = records.shallowEntries.asScala.lastOption.map(_.nextOffset).getOrElse(
                      currentPartitionFetchState.offset)

                    fetcherLagStats.getAndMaybePut(topic, partitionId).lag = Math.max(0L, partitionData.highWatermark - newOffset)
                    // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                    processPartitionData(topicPartition, currentPartitionFetchState.offset, partitionData)

                    val validBytes = records.validBytes
                    if (validBytes > 0) {
                      // Update partitionStates only if there is no exception during processPartitionData
                      partitionStates.updateAndMoveToEnd(topicPartition, new PartitionFetchState(newOffset))
                      fetcherStats.byteRate.mark(validBytes)
                    }
                  } catch {
                    case ime: CorruptRecordException =>
                      // we log the error and continue. This ensures two things
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                      // should get fixed in the subsequent fetches
                      logger.error("Found invalid messages during fetch for partition [" + topic + "," + partitionId + "] offset " + currentPartitionFetchState.offset  + " error " + ime.getMessage)
                      updatePartitionsWithError(topicPartition);
                    case e: Throwable =>
                      throw new KafkaException("error processing data for partition [%s,%d] offset %d"
                        .format(topic, partitionId, currentPartitionFetchState.offset), e)
                  }
                case Errors.OFFSET_OUT_OF_RANGE =>
                  try {
                    val newOffset = handleOffsetOutOfRange(topicPartition)
                    partitionStates.updateAndMoveToEnd(topicPartition, new PartitionFetchState(newOffset))
                    error("Current offset %d for partition [%s,%d] out of range; reset offset to %d"
                      .format(currentPartitionFetchState.offset, topic, partitionId, newOffset))
                  } catch {
                    case e: FatalExitError => throw e
                    case e: Throwable =>
                      error("Error getting offset for partition [%s,%d] to broker %d".format(topic, partitionId, sourceBroker.id), e)
                      updatePartitionsWithError(topicPartition)
                  }
                case _ =>
                  if (isRunning.get) {
                    error("Error for partition [%s,%d] to broker %d:%s".format(topic, partitionId, sourceBroker.id,
                      partitionData.exception.get))
                    updatePartitionsWithError(topicPartition)
                  }
              }
            })
        }
      }
    }

    if (partitionsWithError.nonEmpty) {
      debug("handling partitions with error for %s".format(partitionsWithError))
      handlePartitionsWithErrors(partitionsWithError)
    }
  }

  def addPartitions(partitionAndOffsets: Map[TopicPartition, Long]) {
    partitionMapLock.lockInterruptibly()
    try {
      // If the partitionMap already has the topic/partition, then do not update the map with the old offset
      val newPartitionToState = partitionAndOffsets.filter { case (tp, _) =>
        !partitionStates.contains(tp)
      }.map { case (tp, offset) =>
        val fetchState =
          if (PartitionTopicInfo.isOffsetInvalid(offset)) new PartitionFetchState(handleOffsetOutOfRange(tp))
          else new PartitionFetchState(offset)
        tp -> fetchState
      }
      val existingPartitionToState = partitionStates.partitionStates.asScala.map { state =>
        state.topicPartition -> state.value
      }.toMap
      partitionStates.set((existingPartitionToState ++ newPartitionToState).asJava)
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  def delayPartitions(partitions: Iterable[TopicPartition], delay: Long) {
    partitionMapLock.lockInterruptibly()
    try {
      for (partition <- partitions) {
        Option(partitionStates.stateValue(partition)).foreach (currentPartitionFetchState =>
          if (currentPartitionFetchState.isActive)
            partitionStates.updateAndMoveToEnd(partition, new PartitionFetchState(currentPartitionFetchState.offset, new DelayedItem(delay)))
        )
      }
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  def removePartitions(topicPartitions: Set[TopicPartition]) {
    partitionMapLock.lockInterruptibly()
    try {
      topicPartitions.foreach { topicPartition =>
        partitionStates.remove(topicPartition)
        fetcherLagStats.unregister(topicPartition.topic, topicPartition.partition)
      }
    } finally partitionMapLock.unlock()
  }

  def partitionCount() = {
    partitionMapLock.lockInterruptibly()
    try partitionStates.size
    finally partitionMapLock.unlock()
  }

}

object AbstractFetcherThread {

  trait FetchRequest {
    def isEmpty: Boolean
    def offset(topicPartition: TopicPartition): Long
  }

  trait PartitionData {
    def errorCode: Short
    def exception: Option[Throwable]
    def toRecords: MemoryRecords
    def highWatermark: Long
  }

}

object FetcherMetrics {
  val ConsumerLag = "ConsumerLag"
  val RequestsPerSec = "RequestsPerSec"
  val BytesPerSec = "BytesPerSec"
}

class FetcherLagMetrics(metricId: ClientIdTopicPartition) extends KafkaMetricsGroup {

  private[this] val lagVal = new AtomicLong(-1L)
  private[this] val tags = Map(
    "clientId" -> metricId.clientId,
    "topic" -> metricId.topic,
    "partition" -> metricId.partitionId.toString)

  newGauge(FetcherMetrics.ConsumerLag,
    new Gauge[Long] {
      def value = lagVal.get
    },
    tags
  )

  def lag_=(newLag: Long) {
    lagVal.set(newLag)
  }

  def lag = lagVal.get

  def unregister() {
    removeMetric(FetcherMetrics.ConsumerLag, tags)
  }
}

class FetcherLagStats(metricId: ClientIdAndBroker) {
  private val valueFactory = (k: ClientIdTopicPartition) => new FetcherLagMetrics(k)
  val stats = new Pool[ClientIdTopicPartition, FetcherLagMetrics](Some(valueFactory))

  def getAndMaybePut(topic: String, partitionId: Int): FetcherLagMetrics = {
    stats.getAndMaybePut(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
  }

  def isReplicaInSync(topic: String, partitionId: Int): Boolean = {
    val fetcherLagMetrics = stats.get(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
    if (fetcherLagMetrics != null)
      fetcherLagMetrics.lag <= 0
    else
      false
  }

  def unregister(topic: String, partitionId: Int) {
    val lagMetrics = stats.remove(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
    if (lagMetrics != null) lagMetrics.unregister()
  }

  def unregister() {
    stats.keys.toBuffer.foreach { key: ClientIdTopicPartition =>
      unregister(key.topic, key.partitionId)
    }
  }
}

class FetcherStats(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val tags = Map("clientId" -> metricId.clientId,
    "brokerHost" -> metricId.brokerHost,
    "brokerPort" -> metricId.brokerPort.toString)

  val requestRate = newMeter(FetcherMetrics.RequestsPerSec, "requests", TimeUnit.SECONDS, tags)

  val byteRate = newMeter(FetcherMetrics.BytesPerSec, "bytes", TimeUnit.SECONDS, tags)

  def unregister() {
    removeMetric(FetcherMetrics.RequestsPerSec, tags)
    removeMetric(FetcherMetrics.BytesPerSec, tags)
  }

}

case class ClientIdTopicPartition(clientId: String, topic: String, partitionId: Int) {
  override def toString = "%s-%s-%d".format(clientId, topic, partitionId)
}

/**
  * case class to keep partition offset and its state(active, inactive)
  */
case class PartitionFetchState(offset: Long, delay: DelayedItem) {

  def this(offset: Long) = this(offset, new DelayedItem(0))

  def isActive: Boolean = delay.getDelay(TimeUnit.MILLISECONDS) == 0

  override def toString = "%d-%b".format(offset, isActive)
}
