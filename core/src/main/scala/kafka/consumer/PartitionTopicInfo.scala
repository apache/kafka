/*
 * Copyright 2010 LinkedIn
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.nio.channels._
import java.util.concurrent._
import java.util.concurrent.atomic._
import kafka.message._
import kafka.cluster._
import kafka.common.ErrorMapping
import org.apache.log4j.Logger

private[consumer] class PartitionTopicInfo(val topic: String,
                                           val brokerId: Int,
                                           val partition: Partition,
                                           private val chunkQueue: BlockingQueue[FetchedDataChunk],
                                           private val consumedOffset: AtomicLong,
                                           private val fetchedOffset: AtomicLong,
                                           private val fetchSize: AtomicInteger) {
  private val logger = Logger.getLogger(getClass())
  if (logger.isDebugEnabled) {
    logger.debug("initial consumer offset of " + this + " is " + consumedOffset.get)
    logger.debug("initial fetch offset of " + this + " is " + fetchedOffset.get)
  }

  def getConsumeOffset() = consumedOffset.get

  def getFetchOffset() = fetchedOffset.get

  def resetConsumeOffset(newConsumeOffset: Long) = {
    consumedOffset.set(newConsumeOffset)
    if (logger.isDebugEnabled)
      logger.debug("reset consume offset of " + this + " to " + newConsumeOffset)
  }

  def resetFetchOffset(newFetchOffset: Long) = {
    fetchedOffset.set(newFetchOffset)
    if (logger.isDebugEnabled)
      logger.debug("reset fetch offset of ( %s ) to %d".format(this, newFetchOffset))
  }

  /**
   * Enqueue a message set for processing
   * @return the number of valid bytes
   */
  def enqueue(messages: ByteBufferMessageSet, fetchOffset: Long): Long = {
    val size = messages.shallowValidBytes
    if(size > 0) {
      // update fetched offset to the compressed data chunk size, not the decompressed message set size
      if(logger.isTraceEnabled)
        logger.trace("Updating fetch offset = " + fetchedOffset.get + " with size = " + size)
      val newOffset = fetchedOffset.addAndGet(size)
      if (logger.isDebugEnabled)
        logger.debug("updated fetch offset of ( %s ) to %d".format(this, newOffset))
      chunkQueue.put(new FetchedDataChunk(messages, this, fetchOffset))
    }
    size
  }

  /**
   *  add an empty message with the exception to the queue so that client can see the error
   */
  def enqueueError(e: Throwable, fetchOffset: Long) = {
    val messages = new ByteBufferMessageSet(ErrorMapping.EmptyByteBuffer, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
    chunkQueue.put(new FetchedDataChunk(messages, this, fetchOffset))
  }

  override def toString(): String = topic + ":" + partition.toString + ": fetched offset = " + fetchedOffset.get +
    ": consumed offset = " + consumedOffset.get
}
