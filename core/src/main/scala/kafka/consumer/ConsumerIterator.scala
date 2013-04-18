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

import kafka.utils.{IteratorTemplate, Logging, Utils}
import java.util.concurrent.{TimeUnit, BlockingQueue}
import kafka.serializer.Decoder
import java.util.concurrent.atomic.AtomicReference
import kafka.message.{MessageAndOffset, MessageAndMetadata}
import kafka.common.{KafkaException, MessageSizeTooLargeException}


/**
 * An iterator that blocks until a value can be read from the supplied queue.
 * The iterator takes a shutdownCommand object which can be added to the queue to trigger a shutdown
 *
 */
class ConsumerIterator[K, V](private val channel: BlockingQueue[FetchedDataChunk],
                             consumerTimeoutMs: Int,
                             private val keyDecoder: Decoder[K],
                             private val valueDecoder: Decoder[V],
                             val clientId: String)
  extends IteratorTemplate[MessageAndMetadata[K, V]] with Logging {

  private var current: AtomicReference[Iterator[MessageAndOffset]] = new AtomicReference(null)
  private var currentTopicInfo: PartitionTopicInfo = null
  private var consumedOffset: Long = -1L
  private val consumerTopicStats = ConsumerTopicStatsRegistry.getConsumerTopicStat(clientId)

  override def next(): MessageAndMetadata[K, V] = {
    val item = super.next()
    if(consumedOffset < 0)
      throw new KafkaException("Offset returned by the message set is invalid %d".format(consumedOffset))
    currentTopicInfo.resetConsumeOffset(consumedOffset)
    val topic = currentTopicInfo.topic
    trace("Setting %s consumed offset to %d".format(topic, consumedOffset))
    consumerTopicStats.getConsumerTopicStats(topic).messageRate.mark()
    consumerTopicStats.getConsumerAllTopicStats().messageRate.mark()
    item
  }

  protected def makeNext(): MessageAndMetadata[K, V] = {
    var currentDataChunk: FetchedDataChunk = null
    // if we don't have an iterator, get one
    var localCurrent = current.get()
    if(localCurrent == null || !localCurrent.hasNext) {
      if (consumerTimeoutMs < 0)
        currentDataChunk = channel.take
      else {
        currentDataChunk = channel.poll(consumerTimeoutMs, TimeUnit.MILLISECONDS)
        if (currentDataChunk == null) {
          // reset state to make the iterator re-iterable
          resetState()
          throw new ConsumerTimeoutException
        }
      }
      if(currentDataChunk eq ZookeeperConsumerConnector.shutdownCommand) {
        debug("Received the shutdown command")
        channel.offer(currentDataChunk)
        return allDone
      } else {
        currentTopicInfo = currentDataChunk.topicInfo
        val cdcFetchOffset = currentDataChunk.fetchOffset
        val ctiConsumeOffset = currentTopicInfo.getConsumeOffset
        if (ctiConsumeOffset < cdcFetchOffset) {
          error("consumed offset: %d doesn't match fetch offset: %d for %s;\n Consumer may lose data"
            .format(ctiConsumeOffset, cdcFetchOffset, currentTopicInfo))
          currentTopicInfo.resetConsumeOffset(cdcFetchOffset)
        }
        localCurrent = currentDataChunk.messages.iterator

        current.set(localCurrent)
      }
      // if we just updated the current chunk and it is empty that means the fetch size is too small!
      if(currentDataChunk.messages.validBytes == 0)
        throw new MessageSizeTooLargeException("Found a message larger than the maximum fetch size of this consumer on topic " +
                                               "%s partition %d at fetch offset %d. Increase the fetch size, or decrease the maximum message size the broker will allow."
                                               .format(currentDataChunk.topicInfo.topic, currentDataChunk.topicInfo.partitionId, currentDataChunk.fetchOffset))
    }
    var item = localCurrent.next()
    // reject the messages that have already been consumed
    while (item.offset < currentTopicInfo.getConsumeOffset && localCurrent.hasNext) {
      item = localCurrent.next()
    }
    consumedOffset = item.nextOffset

    item.message.ensureValid() // validate checksum of message to ensure it is valid

    val keyBuffer = item.message.key
    val key = if(keyBuffer == null) null.asInstanceOf[K] else keyDecoder.fromBytes(Utils.readBytes(keyBuffer))
    val value = if(item.message.isNull) null.asInstanceOf[V] else valueDecoder.fromBytes(Utils.readBytes(item.message.payload))
    new MessageAndMetadata(key, value, currentTopicInfo.topic, currentTopicInfo.partitionId, item.offset)
  }

  def clearCurrentChunk() {
    try {
      info("Clearing the current data chunk for this consumer iterator")
      current.set(null)
    }
  }
}

class ConsumerTimeoutException() extends RuntimeException()

