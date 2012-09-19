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

import kafka.utils.{IteratorTemplate, Logging}
import java.util.concurrent.{TimeUnit, BlockingQueue}
import kafka.serializer.Decoder
import java.util.concurrent.atomic.AtomicReference
import kafka.message.{MessageAndOffset, MessageAndMetadata}


/**
 * An iterator that blocks until a value can be read from the supplied queue.
 * The iterator takes a shutdownCommand object which can be added to the queue to trigger a shutdown
 *
 */
class ConsumerIterator[T](private val channel: BlockingQueue[FetchedDataChunk],
                          consumerTimeoutMs: Int,
                          private val decoder: Decoder[T],
                          val enableShallowIterator: Boolean)
  extends IteratorTemplate[MessageAndMetadata[T]] with Logging {

  private var current: AtomicReference[Iterator[MessageAndOffset]] = new AtomicReference(null)
  private var currentTopicInfo:PartitionTopicInfo = null
  private var consumedOffset: Long = -1L

  override def next(): MessageAndMetadata[T] = {
    val item = super.next()
    if(consumedOffset < 0)
      throw new IllegalStateException("Offset returned by the message set is invalid %d".format(consumedOffset))
    currentTopicInfo.resetConsumeOffset(consumedOffset)
    val topic = currentTopicInfo.topic
    trace("Setting %s consumed offset to %d".format(topic, consumedOffset))
    ConsumerTopicStat.getConsumerTopicStat(topic).recordMessagesPerTopic(1)
    ConsumerTopicStat.getConsumerAllTopicStat().recordMessagesPerTopic(1)
    item
  }

  protected def makeNext(): MessageAndMetadata[T] = {
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
        if (currentTopicInfo.getConsumeOffset != currentDataChunk.fetchOffset) {
          error("consumed offset: %d doesn't match fetch offset: %d for %s;\n Consumer may lose data"
                        .format(currentTopicInfo.getConsumeOffset, currentDataChunk.fetchOffset, currentTopicInfo))
          currentTopicInfo.resetConsumeOffset(currentDataChunk.fetchOffset)
        }
        localCurrent = if (enableShallowIterator) currentDataChunk.messages.shallowIterator
                       else currentDataChunk.messages.iterator
        current.set(localCurrent)
      }
    }
    val item = localCurrent.next()
    consumedOffset = item.offset

    new MessageAndMetadata(decoder.toEvent(item.message), currentTopicInfo.topic)
  }

  def clearCurrentChunk() {
    try {
      info("Clearing the current data chunk for this consumer iterator")
      current.set(null)
    }
  }
}

class ConsumerTimeoutException() extends RuntimeException()

