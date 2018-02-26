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

package kafka.message

import java.nio.ByteBuffer

import kafka.common.LongRef
import kafka.utils.Logging
import org.apache.kafka.common.record._

import scala.collection.JavaConverters._

object ByteBufferMessageSet {

  private def create(offsetAssigner: OffsetAssigner,
                     compressionCodec: CompressionCodec,
                     timestampType: TimestampType,
                     messages: Message*): ByteBuffer = {
    if (messages.isEmpty)
      MessageSet.Empty.buffer
    else {
      val buffer = ByteBuffer.allocate(math.min(math.max(MessageSet.messageSetSize(messages) / 2, 1024), 1 << 16))
      val builder = MemoryRecords.builder(buffer, messages.head.magic, CompressionType.forId(compressionCodec.codec),
        timestampType, offsetAssigner.baseOffset)

      for (message <- messages)
        builder.appendWithOffset(offsetAssigner.nextAbsoluteOffset(), message.asRecord)

      builder.build().buffer
    }
  }

}

private object OffsetAssigner {

  def apply(offsetCounter: LongRef, size: Int): OffsetAssigner =
    new OffsetAssigner(offsetCounter.value to offsetCounter.addAndGet(size))

}

private class OffsetAssigner(offsets: Seq[Long]) {
  private var index = 0

  def nextAbsoluteOffset(): Long = {
    val result = offsets(index)
    index += 1
    result
  }

  def baseOffset = offsets.head

  def toInnerOffset(offset: Long): Long = offset - offsets.head

}

/**
 * A sequence of messages stored in a byte buffer
 *
 * There are two ways to create a ByteBufferMessageSet
 *
 * Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method.
 *
 * Option 2: Give it a list of messages along with instructions relating to serialization format. Producers will use this method.
 *
 *
 * Message format v1 has the following changes:
 * - For non-compressed messages, timestamp and timestamp type attributes have been added. The offsets of
 *   the messages remain absolute offsets.
 * - For compressed messages, timestamp and timestamp type attributes have been added and inner offsets (IO) are used
 *   for inner messages of compressed messages (see offset calculation details below). The timestamp type
 *   attribute is only set in wrapper messages. Inner messages always have CreateTime as the timestamp type in attributes.
 *
 * We set the timestamp in the following way:
 * For non-compressed messages: the timestamp and timestamp type message attributes are set and used.
 * For compressed messages:
 * 1. Wrapper messages' timestamp type attribute is set to the proper value
 * 2. Wrapper messages' timestamp is set to:
 *    - the max timestamp of inner messages if CreateTime is used
 *    - the current server time if wrapper message's timestamp = LogAppendTime.
 *      In this case the wrapper message timestamp is used and all the timestamps of inner messages are ignored.
 * 3. Inner messages' timestamp will be:
 *    - used when wrapper message's timestamp type is CreateTime
 *    - ignored when wrapper message's timestamp type is LogAppendTime
 * 4. Inner messages' timestamp type will always be ignored with one exception: producers must set the inner message
 *    timestamp type to CreateTime, otherwise the messages will be rejected by broker.
 *
 * Absolute offsets are calculated in the following way:
 * Ideally the conversion from relative offset(RO) to absolute offset(AO) should be:
 *
 * AO = AO_Of_Last_Inner_Message + RO
 *
 * However, note that the message sets sent by producers are compressed in a streaming way.
 * And the relative offset of an inner message compared with the last inner message is not known until
 * the last inner message is written.
 * Unfortunately we are not able to change the previously written messages after the last message is written to
 * the message set when stream compression is used.
 *
 * To solve this issue, we use the following solution:
 *
 * 1. When the producer creates a message set, it simply writes all the messages into a compressed message set with
 *    offset 0, 1, ... (inner offset).
 * 2. The broker will set the offset of the wrapper message to the absolute offset of the last message in the
 *    message set.
 * 3. When a consumer sees the message set, it first decompresses the entire message set to find out the inner
 *    offset (IO) of the last inner message. Then it computes RO and AO of previous messages:
 *
 *    RO = IO_of_a_message - IO_of_the_last_message
 *    AO = AO_Of_Last_Inner_Message + RO
 *
 * 4. This solution works for compacted message sets as well.
 *
 */
class ByteBufferMessageSet(val buffer: ByteBuffer) extends MessageSet with Logging {

  private[kafka] def this(compressionCodec: CompressionCodec,
                          offsetCounter: LongRef,
                          timestampType: TimestampType,
                          messages: Message*) {
    this(ByteBufferMessageSet.create(OffsetAssigner(offsetCounter, messages.size), compressionCodec,
      timestampType, messages:_*))
  }

  def this(compressionCodec: CompressionCodec, offsetCounter: LongRef, messages: Message*) {
    this(compressionCodec, offsetCounter, TimestampType.CREATE_TIME, messages:_*)
  }

  def this(compressionCodec: CompressionCodec, offsetSeq: Seq[Long], messages: Message*) {
    this(ByteBufferMessageSet.create(new OffsetAssigner(offsetSeq), compressionCodec,
      TimestampType.CREATE_TIME, messages:_*))
  }

  def this(compressionCodec: CompressionCodec, messages: Message*) {
    this(compressionCodec, new LongRef(0L), messages: _*)
  }

  def this(messages: Message*) {
    this(NoCompressionCodec, messages: _*)
  }

  def getBuffer = buffer

  override def asRecords: MemoryRecords = MemoryRecords.readableRecords(buffer.duplicate())

  /** default iterator that iterates over decompressed messages */
  override def iterator: Iterator[MessageAndOffset] = internalIterator()

  /** iterator over compressed messages without decompressing */
  def shallowIterator: Iterator[MessageAndOffset] = internalIterator(isShallow = true)

  /** When flag isShallow is set to be true, we do a shallow iteration: just traverse the first level of messages. **/
  private def internalIterator(isShallow: Boolean = false): Iterator[MessageAndOffset] = {
    if (isShallow)
      asRecords.batches.asScala.iterator.map(MessageAndOffset.fromRecordBatch)
    else
      asRecords.records.asScala.iterator.map(MessageAndOffset.fromRecord)
  }

  /**
   * The total number of bytes in this message set, including any partial trailing messages
   */
  def sizeInBytes: Int = buffer.limit()

  /**
   * The total number of bytes in this message set not including any partial, trailing messages
   */
  def validBytes: Int = asRecords.validBytes

  /**
   * Two message sets are equal if their respective byte buffers are equal
   */
  override def equals(other: Any): Boolean = {
    other match {
      case that: ByteBufferMessageSet =>
        buffer.equals(that.buffer)
      case _ => false
    }
  }

  override def hashCode: Int = buffer.hashCode

}
