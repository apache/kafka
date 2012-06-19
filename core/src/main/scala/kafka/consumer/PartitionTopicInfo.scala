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

import java.util.concurrent._
import java.util.concurrent.atomic._
import kafka.message._
import kafka.cluster._
import kafka.utils.Logging
import kafka.common.ErrorMapping

private[consumer] class PartitionTopicInfo(val topic: String,
                                           val brokerId: Int,
                                           val partition: Partition,
                                           private val chunkQueue: BlockingQueue[FetchedDataChunk],
                                           private val consumedOffset: AtomicLong,
                                           private val fetchedOffset: AtomicLong,
                                           private val fetchSize: AtomicInteger) extends Logging {

  debug("initial consumer offset of " + this + " is " + consumedOffset.get)
  debug("initial fetch offset of " + this + " is " + fetchedOffset.get)

  def getConsumeOffset() = consumedOffset.get

  def getFetchOffset() = fetchedOffset.get

  def resetConsumeOffset(newConsumeOffset: Long) = {
    consumedOffset.set(newConsumeOffset)
    debug("reset consume offset of " + this + " to " + newConsumeOffset)
  }

  def resetFetchOffset(newFetchOffset: Long) = {
    fetchedOffset.set(newFetchOffset)
    debug("reset fetch offset of ( %s ) to %d".format(this, newFetchOffset))
  }

  /**
   * Enqueue a message set for processing
   * @return the number of valid bytes
   */
  def enqueue(messages: ByteBufferMessageSet, fetchOffset: Long): Long = {
    val size = messages.validBytes
    if(size > 0) {
      // update fetched offset to the compressed data chunk size, not the decompressed message set size
      trace("Updating fetch offset = " + fetchedOffset.get + " with size = " + size)
      chunkQueue.put(new FetchedDataChunk(messages, this, fetchOffset))
      val newOffset = fetchedOffset.addAndGet(size)
      debug("updated fetch offset of ( %s ) to %d".format(this, newOffset))
      ConsumerTopicStat.getConsumerTopicStat(topic).recordBytesPerTopic(size)
      ConsumerTopicStat.getConsumerAllTopicStat().recordBytesPerTopic(size)
    }
    size
  }

  /**
   *  add an empty message with the exception to the queue so that client can see the error
   */
  def enqueueError(e: Throwable, fetchOffset: Long) = {
    val messages = new ByteBufferMessageSet(buffer = ErrorMapping.EmptyByteBuffer, initialOffset = 0,
      errorCode = ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    chunkQueue.put(new FetchedDataChunk(messages, this, fetchOffset))
  }

  override def toString(): String = topic + ":" + partition.toString + ": fetched offset = " + fetchedOffset.get +
    ": consumed offset = " + consumedOffset.get
}
