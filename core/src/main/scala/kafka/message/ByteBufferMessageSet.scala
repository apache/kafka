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

import kafka.api.{KAFKA_0_10_0_DV0, ApiVersion}
import kafka.message.TimestampType.TimestampType
import kafka.utils.{IteratorTemplate, Logging}
import kafka.common.KafkaException

import java.nio.ByteBuffer
import java.nio.channels._
import java.io._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.apache.kafka.common.utils.{Crc32, Utils}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ByteBufferMessageSet {

  private def create(offsetAssignor: OffsetAssigner,
                     compressionCodec: CompressionCodec,
                     messageSetTimestampAssignor: (Seq[Message]) => Long,
                     timestampType: TimestampType,
                     messages: Message*): ByteBuffer = {
    if(messages.size == 0) {
      MessageSet.Empty.buffer
    } else if(compressionCodec == NoCompressionCodec) {
      val buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
      for(message <- messages)
        writeMessage(buffer, message, offsetAssignor.nextAbsoluteOffset)
      buffer.rewind()
      buffer
    } else {
      val messageSetTimestamp = messageSetTimestampAssignor(messages)
      var offset = -1L
      val magicValue = messages.head.magic
      val messageWriter = new MessageWriter(math.min(math.max(MessageSet.messageSetSize(messages) / 2, 1024), 1 << 16))
      messageWriter.write(codec = compressionCodec, timestamp = messageSetTimestamp, timestampType = timestampType, magicValue = magicValue) { outputStream =>
        val output = new DataOutputStream(CompressionFactory(compressionCodec, outputStream))
        try {
          for (message <- messages) {
            offset = offsetAssignor.nextAbsoluteOffset
            if (message.magic != magicValue)
              throw new IllegalArgumentException("Messages in the same compressed message set must have same magic value")
            // Use relative offset if magic value is greater than 0
            if (magicValue > Message.MagicValue_V0)
              output.writeLong(offsetAssignor.toRelativeOffset(offset))
            else
              output.writeLong(offset)
            output.writeInt(message.size)
            output.write(message.buffer.array, message.buffer.arrayOffset, message.buffer.limit)
          }
        } finally {
          output.close()
        }
      }
      val buffer = ByteBuffer.allocate(messageWriter.size + MessageSet.LogOverhead)
      writeMessage(buffer, messageWriter, offset)
      buffer.rewind()
      buffer
    }
  }

  /** Deep iterator that decompresses the message sets and adjust timestamp and offset if needed. */
  def deepIterator(wrapperMessageAndOffset: MessageAndOffset): Iterator[MessageAndOffset] = {

    import Message._

    new IteratorTemplate[MessageAndOffset] {

      val wrapperMessageOffset = wrapperMessageAndOffset.offset
      val wrapperMessage = wrapperMessageAndOffset.message
      val wrapperMessageTimestampOpt: Option[Long] =
        if (wrapperMessage.magic > MagicValue_V0) Some(wrapperMessage.timestamp) else None
      val wrapperMessageTimestampTypeOpt: Option[TimestampType] =
        if (wrapperMessage.magic > MagicValue_V0) Some(wrapperMessage.timestampType) else None
      if (wrapperMessage.payload == null)
        throw new RuntimeException("wrapper message = " + wrapperMessage)
      val inputStream: InputStream = new ByteBufferBackedInputStream(wrapperMessage.payload)
      val compressed: DataInputStream = new DataInputStream(CompressionFactory(wrapperMessage.compressionCodec, inputStream))
      var lastInnerOffset = -1L
      // When magic value is greater than 0, relative offset will be used.
      // Ideally the conversion from relative offset(RO) to absolute offset(AO) should be:
      //
      // AO = AO_Of_Last_Inner_Message + RO
      //
      // However, Note that the message sets sent by producers are compressed in a stream compressing way.
      // And the relative offset of an inner message compared with the last inner message is not known until
      // the last inner message is written.
      // Unfortunately we are not able to change the previously written messages after the last message is writtern to
      // the message set when stream compressing is used.
      //
      // To solve this issue, we use the following solution:
      // 1. When producer create a message set, it simply write all the messages into a compressed message set with
      //    offset 0, 1, ... (inner offset).
      // 2. The broker will set the offset of the wrapper message to the absolute offset of the last message in the
      //    message set.
      // 3. When a consumer sees the message set, it first decompresses the entire message set to find out the inner
      //    offset (IO) of the last inner message. Then it computes RO and AO of previous messages:
      //
      //    RO = Inner_Offset_of_a_message - Inner_Offset_of_the_last_message
      //    AO = AO_Of_Last_Inner_Message + RO
      //
      // 4. This solution works for compacted message set as well
      val messageAndOffsets = if (wrapperMessageAndOffset.message.magic > MagicValue_V0) {
        var innerMessageAndOffsets = new mutable.Queue[MessageAndOffset]()
        try {
          while (true) {
            innerMessageAndOffsets += readMessageFromStream()
          }
        } catch {
          case eofe: EOFException =>
            compressed.close()
          case ioe: IOException =>
            throw new KafkaException(ioe)
        }
        Some(innerMessageAndOffsets)
      } else {
        None
      }

      private def readMessageFromStream() = {
        // read the offset
        val innerOffset = compressed.readLong()
        // read record size
        val size = compressed.readInt()

        if (size < MinHeaderSize)
          throw new InvalidMessageException("Message found with corrupt size (" + size + ") in deep iterator")

        // read the record into an intermediate record buffer
        // and hence has to do extra copy
        val bufferArray = new Array[Byte](size)
        compressed.readFully(bufferArray, 0, size)
        val buffer = ByteBuffer.wrap(bufferArray)

        // Override the timestamp if necessary
        val newMessage = new Message(buffer, wrapperMessageTimestampOpt, wrapperMessageTimestampTypeOpt)

        // Inner message and wrapper message must have same magic value
        if (newMessage.magic != wrapperMessage.magic)
          throw new IllegalStateException(s"Compressed message has magic value ${wrapperMessage.magic} " +
            s"but inner message has magic value ${newMessage.magic}")
        lastInnerOffset = innerOffset
        new MessageAndOffset(newMessage, innerOffset)
      }

      override def makeNext(): MessageAndOffset = {
        messageAndOffsets match {
          // Using relative offset and timestamps
          case Some(innerMessageAndOffsets) =>
            if (innerMessageAndOffsets.isEmpty)
              allDone()
            else {
              val messageAndOffset = messageAndOffsets.get.dequeue()
              val message = messageAndOffset.message
              val relativeOffset = messageAndOffset.offset - lastInnerOffset
              val absoluteOffset = wrapperMessageOffset + relativeOffset
              new MessageAndOffset(message, absoluteOffset)
            }
          // Not using relative offset and timestamps
          case None =>
            try {
              readMessageFromStream()
            } catch {
              case eofe: EOFException =>
                compressed.close()
                allDone()
              case ioe: IOException =>
                throw new KafkaException(ioe)
            }
        }
      }
    }
  }

  private[kafka] def writeMessage(buffer: ByteBuffer, message: Message, offset: Long) {
    buffer.putLong(offset)
    buffer.putInt(message.size)
    buffer.put(message.buffer)
    message.buffer.rewind()
  }

  private[kafka] def writeMessage(buffer: ByteBuffer, messageWriter: MessageWriter, offset: Long) {
    buffer.putLong(offset)
    buffer.putInt(messageWriter.size)
    messageWriter.writeTo(buffer)
  }
}

private class OffsetAssigner(offsets: Seq[Long]) {
  val index = new AtomicInteger(0)

  def this(offsetCounter: AtomicLong, size: Int) {
    this((offsetCounter.get() to offsetCounter.get + size).toSeq)
    offsetCounter.addAndGet(size)
  }

  def nextAbsoluteOffset = offsets(index.getAndIncrement)

  def toRelativeOffset(offset: Long) = offset - offsets(0)

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
 */
class ByteBufferMessageSet(val buffer: ByteBuffer) extends MessageSet with Logging {
  private var shallowValidByteCount = -1

  def this(compressionCodec: CompressionCodec, messages: Message*) {
    this(ByteBufferMessageSet.create(new OffsetAssigner(new AtomicLong(0), messages.size), compressionCodec,
      MessageSet.validateMagicValuesAndGetTimestamp, TimestampType.CreateTime, messages:_*))
  }

  def this(compressionCodec: CompressionCodec, offsetCounter: AtomicLong, messages: Message*) {
    this(ByteBufferMessageSet.create(new OffsetAssigner(offsetCounter, messages.size), compressionCodec,
      MessageSet.validateMagicValuesAndGetTimestamp, TimestampType.CreateTime, messages:_*))
  }

  def this(compressionCodec: CompressionCodec,
           offsetCounter: AtomicLong,
           messageSetTimestampAssignor: (Message*) => Long,
           timestampType: TimestampType,
           messages: Message*) {
    this(ByteBufferMessageSet.create(new OffsetAssigner(offsetCounter, messages.size), compressionCodec,
      messageSetTimestampAssignor, timestampType, messages:_*))
  }

  def this(compressionCodec: CompressionCodec, offsetSeq: Seq[Long], messages: Message*) {
    this(ByteBufferMessageSet.create(new OffsetAssigner(offsetSeq), compressionCodec,
      MessageSet.validateMagicValuesAndGetTimestamp, TimestampType.CreateTime, messages:_*))
  }

  def this(messages: Message*) {
    this(NoCompressionCodec, new AtomicLong(0), messages: _*)
  }

  def getBuffer = buffer

  private def shallowValidBytes: Int = {
    if(shallowValidByteCount < 0) {
      var bytes = 0
      val iter = this.internalIterator(true)
      while(iter.hasNext) {
        val messageAndOffset = iter.next
        bytes += MessageSet.entrySize(messageAndOffset.message)
      }
      this.shallowValidByteCount = bytes
    }
    shallowValidByteCount
  }

  /** Write the messages in this set to the given channel */
  def writeTo(channel: GatheringByteChannel, offset: Long, size: Int): Int = {
    // Ignore offset and size from input. We just want to write the whole buffer to the channel.
    buffer.mark()
    var written = 0
    while(written < sizeInBytes)
      written += channel.write(buffer)
    buffer.reset()
    written
  }

  override def hasMagicValue(expectedMagicValue: Byte): Boolean = {
    for (messageAndOffset <- shallowIterator) {
      if (messageAndOffset.message.magic != expectedMagicValue)
        return false
    }
    true
  }

  /** default iterator that iterates over decompressed messages */
  override def iterator: Iterator[MessageAndOffset] = internalIterator()

  /** iterator over compressed messages without decompressing */
  def shallowIterator: Iterator[MessageAndOffset] = internalIterator(true)

  /** When flag isShallow is set to be true, we do a shallow iteration: just traverse the first level of messages. **/
  private def internalIterator(isShallow: Boolean = false): Iterator[MessageAndOffset] = {
    new IteratorTemplate[MessageAndOffset] {
      var topIter = buffer.slice()
      var innerIter: Iterator[MessageAndOffset] = null

      def innerDone(): Boolean = (innerIter == null || !innerIter.hasNext)

      def makeNextOuter: MessageAndOffset = {
        // if there isn't at least an offset and size, we are done
        if (topIter.remaining < 12)
          return allDone()
        val offset = topIter.getLong()
        val size = topIter.getInt()
        if(size < Message.MinHeaderSize)
          throw new InvalidMessageException("Message found with corrupt size (" + size + ") in shallow iterator")

        // we have an incomplete message
        if(topIter.remaining < size)
          return allDone()

        // read the current message and check correctness
        val message = topIter.slice()
        message.limit(size)
        topIter.position(topIter.position + size)
        val newMessage = new Message(message)
        if(isShallow) {
          new MessageAndOffset(newMessage, offset)
        } else {
          newMessage.compressionCodec match {
            case NoCompressionCodec =>
              innerIter = null
              new MessageAndOffset(newMessage, offset)
            case _ =>
              innerIter = ByteBufferMessageSet.deepIterator(new MessageAndOffset(newMessage, offset))
              if(!innerIter.hasNext)
                innerIter = null
              makeNext()
          }
        }
      }

      override def makeNext(): MessageAndOffset = {
        if(isShallow){
          makeNextOuter
        } else {
          if(innerDone())
            makeNextOuter
          else
            innerIter.next()
        }
      }

    }
  }

  /**
   * Update the offsets for this message set and do further validation on messages including:
   * 1. Messages for compacted topics must have keys
   * 2. When magic value = 1, inner messages of a compressed message set must have monotonically increasing offsets
   *    starting from 0.
   * 3. When magic value = 1, validate and maybe overwrite timestamps of messages.
   *
   * This method will convert the messages based on the following scenarios:
   * A. Magic value of a message = 0 and messageFormatVersion is above or equals to 0.10.0-DV0
   * B. Magic value of a message = 1 and messageFormatVersion is lower than 0.10.0-DV0
   *
   * If no format conversion or value overwriting is required for messages, this method will perform in-place
   * operations and avoids re-compression.
   */
  private[kafka] def validateMessagesAndAssignOffsets(offsetCounter: AtomicLong,
                                                      now: Long = System.currentTimeMillis(),
                                                      sourceCodec: CompressionCodec,
                                                      targetCodec: CompressionCodec,
                                                      compactedTopic: Boolean = false,
                                                      messageFormatVersion: ApiVersion = ApiVersion.latestVersion,
                                                      messageTimestampType: TimestampType,
                                                      messageTimestampDiffMaxMs: Long): ByteBufferMessageSet = {
    val magicValueToUse = if (messageFormatVersion.onOrAfter(KAFKA_0_10_0_DV0)) Message.MagicValue_V1 else Message.MagicValue_V0
    if (sourceCodec == NoCompressionCodec && targetCodec == NoCompressionCodec) {
      // check the magic value
      if (!hasMagicValue(magicValueToUse)) {
        // Message format conversion
        convertNonCompressedMessages(offsetCounter, compactedTopic, now, messageTimestampType, magicValueToUse)
      } else {
        // Do in-place validation, offset assignment and maybe set timestamp
        validateNonCompressedMessagesAndAssignOffsetInPlace(offsetCounter, now, compactedTopic, messageTimestampType,
          messageTimestampDiffMaxMs)
      }

    } else {
      // Deal with compressed messages
      // We only need to do re-compression in one of the followings situation:
      // 1. Source and target compression codec are different
      // 2. When magic value to use is 0 because offsets need to be overwritten
      // 3. When magic value to use is above 0, but some fields of inner messages need to be overwritten.
      // 4. Message format conversion is needed.

      // Re-compression situation 1 and 2
      var requireReCompression = sourceCodec != targetCodec || magicValueToUse == Message.MagicValue_V0

      var maxTimestamp = Message.NoTimestamp
      val expectedRelativeOffset = new AtomicLong(0)
      val validatedMessages = new ListBuffer[Message]
      this.internalIterator(isShallow = false).foreach(messageAndOffset => {
        val message = messageAndOffset.message
        validateMessageKey(message, compactedTopic)
        if (message.magic > Message.MagicValue_V0 && magicValueToUse > Message.MagicValue_V0) {
          // Re-compression situation 3
          // Validate the timestamp
          validateTimestamp(message, now, messageTimestampType, messageTimestampDiffMaxMs)
          // Check if we need to overwrite offset
          if (messageAndOffset.offset != expectedRelativeOffset.getAndIncrement)
            requireReCompression = true
          maxTimestamp = math.max(maxTimestamp, message.timestamp)
        }

        // Re-compression situation 4
        if (message.magic != magicValueToUse)
          requireReCompression = true

        validatedMessages += message.toFormatVersion(magicValueToUse)
      })

      if (requireReCompression) {
        // Re-compression required.
        val messageSetTimestampAssignor = (messages: Seq[Message]) => {
          if (magicValueToUse == Message.MagicValue_V0)
            Message.NoTimestamp
          else if (magicValueToUse > Message.MagicValue_V0 && messageTimestampType == TimestampType.CreateTime)
            maxTimestamp
          else // Log append time
            now
        }

        new ByteBufferMessageSet(compressionCodec = targetCodec,
                                 offsetCounter = offsetCounter,
                                 messageSetTimestampAssignor = messageSetTimestampAssignor,
                                 timestampType = messageTimestampType,
                                 messages = validatedMessages.toBuffer: _*)
      } else {
        // Do not do re-compression but simply update the offset, timestamp and attributes field of the wrapper message.
        buffer.putLong(0, offsetCounter.addAndGet(validatedMessages.size) - 1)

        if (magicValueToUse > Message.MagicValue_V0) {
          var crcUpdateNeeded = true
          val timestampOffset = MessageSet.LogOverhead + Message.TimestampOffset
          val attributeOffset = MessageSet.LogOverhead + Message.AttributesOffset
          val timestamp = buffer.getLong(timestampOffset)
          val attributes = buffer.get(attributeOffset)
          if (messageTimestampType == TimestampType.CreateTime) {
            if (timestamp == maxTimestamp && TimestampType.getTimestampType(attributes) == TimestampType.CreateTime)
              // We don't need to recompute crc if the timestamp is not updated.
              crcUpdateNeeded = false
            else {
              // Set timestamp type and timestamp
              buffer.putLong(timestampOffset, maxTimestamp)
              buffer.put(attributeOffset, TimestampType.setTimestampType(attributes, TimestampType.CreateTime))
            }
          } else if (messageTimestampType == TimestampType.LogAppendTime) {
            // Set timestamp type and timestamp
            buffer.putLong(timestampOffset, now)
            buffer.put(attributeOffset, TimestampType.setTimestampType(attributes, TimestampType.LogAppendTime))
          }

          if (crcUpdateNeeded) {
            // need to recompute the crc value
            buffer.position(MessageSet.LogOverhead)
            val wrapperMessage = new Message(buffer.slice())
            Utils.writeUnsignedInt(buffer, MessageSet.LogOverhead + Message.CrcOffset, wrapperMessage.computeChecksum())
          }
        }
        buffer.rewind()
        this
      }
    }
  }

  private def convertNonCompressedMessages(offsetCounter: AtomicLong,
                                           compactedTopic: Boolean,
                                           now: Long,
                                           timestampType: TimestampType,
                                           toMagicValue: Byte): ByteBufferMessageSet = {
    // Get message count, shallow iterator is in-place
    val sizeInBytesAfterConversion = shallowValidBytes + this.internalIterator(isShallow = true).foldLeft(0)(
      (sizeDiff, messageAndOffset) => sizeDiff + Message.headerSizeDiff(messageAndOffset.message.magic, toMagicValue))
    val newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion)
    var newMessagePosition = 0
    this.internalIterator(isShallow = true).foreach {messageAndOffset =>
      val message = messageAndOffset.message
      validateMessageKey(message, compactedTopic)
      newBuffer.position(newMessagePosition)
      // write offset.
      newBuffer.putLong(offsetCounter.getAndIncrement)
      // Write new message size
      val newMessageSize = message.size + Message.headerSizeDiff(message.magic, toMagicValue)
      newBuffer.putInt(newMessageSize)
      // Create new message buffer
      val newMessageBuffer = newBuffer.slice()
      newMessageBuffer.limit(newMessageSize)
      // Convert message
      message.convertToBuffer(toMagicValue, newMessageBuffer, now, timestampType)

      newMessagePosition += MessageSet.LogOverhead + newMessageSize
    }
    newBuffer.rewind()
    new ByteBufferMessageSet(newBuffer)
  }

  private def validateNonCompressedMessagesAndAssignOffsetInPlace(offsetCounter: AtomicLong,
                                                                  now: Long,
                                                                  compactedTopic: Boolean,
                                                                  timestampType: TimestampType,
                                                                  timestampDiffMaxMs: Long): ByteBufferMessageSet = {
    // do in-place validation and offset assignment
    var messagePosition = 0
    buffer.mark()
    while (messagePosition < sizeInBytes - MessageSet.LogOverhead) {
      buffer.position(messagePosition)
      buffer.putLong(offsetCounter.getAndIncrement())
      val messageSize = buffer.getInt()
      val messageBuffer = buffer.slice()
      messageBuffer.limit(messageSize)
      val message = new Message(messageBuffer)
      validateMessageKey(message, compactedTopic)
      if (message.magic > Message.MagicValue_V0) {
        validateTimestamp(message, now, timestampType, timestampDiffMaxMs)
        val crcUpdateNeeded = {
          if (timestampType == TimestampType.LogAppendTime) {
            message.buffer.putLong(Message.TimestampOffset, now)
            message.buffer.put(Message.AttributesOffset, TimestampType.setTimestampType(message.attributes, TimestampType.LogAppendTime))
            true
          } else if (timestampType == TimestampType.CreateTime &&
              TimestampType.getTimestampType(message.attributes) != TimestampType.CreateTime) {
            message.buffer.put(Message.AttributesOffset, TimestampType.setTimestampType(message.attributes, TimestampType.CreateTime))
            true
          } else
            false
        }
        // We have to update crc after updating the timestamp or timestamp type.
        if (crcUpdateNeeded)
          Utils.writeUnsignedInt(message.buffer, Message.CrcOffset, message.computeChecksum())
      }
      messagePosition += MessageSet.LogOverhead + messageSize
    }
    buffer.reset()
    this
  }

  private def validateMessageKey(message: Message, compactedTopic: Boolean) {
    if (compactedTopic && !message.hasKey)
      throw new InvalidMessageException("Compacted topic cannot accept message without key.")
  }

  /**
   * This method validates the timestamps of a message.
   * If the message is using create time, this method checks if it is with acceptable range.
   */
  private def validateTimestamp(message: Message,
                                now: Long,
                                timestampType: TimestampType,
                                timestampDiffMaxMs: Long) {
    if (timestampType == TimestampType.CreateTime && math.abs(message.timestamp - now) > timestampDiffMaxMs)
      throw new InvalidMessageException(s"Timestamp ${message.timestamp} of message is out of range. " +
        s"The timestamp should be within [${now - timestampDiffMaxMs}, ${now + timestampDiffMaxMs}")
  }

  /**
   * The total number of bytes in this message set, including any partial trailing messages
   */
  def sizeInBytes: Int = buffer.limit

  /**
   * The total number of bytes in this message set not including any partial, trailing messages
   */
  def validBytes: Int = shallowValidBytes

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
