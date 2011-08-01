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

package kafka.message

import scala.collection.mutable
import org.apache.log4j.Logger
import kafka.common.{InvalidMessageSizeException, ErrorMapping}
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import kafka.utils.IteratorTemplate

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
class ByteBufferMessageSet(private val buffer: ByteBuffer,
                           private val initialOffset: Long = 0L,
                           private val errorCode: Int = ErrorMapping.NoError) extends MessageSet {
  private val logger = Logger.getLogger(getClass())  
  private var validByteCount = -1L
  private var shallowValidByteCount = -1L
  private var deepValidByteCount = -1L

  def this(compressionCodec: CompressionCodec, messages: Message*) {
    this(
      compressionCodec match {
        case NoCompressionCodec =>
          val buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
          for (message <- messages) {
            message.serializeTo(buffer)
          }
          buffer.rewind
          buffer
        case _ =>
          val message = CompressionUtils.compress(messages, compressionCodec)
          val buffer = ByteBuffer.allocate(message.serializedSize)
          message.serializeTo(buffer)
          buffer.rewind
          buffer
      }, 0L, ErrorMapping.NoError)
  }

  def this(messages: Message*) {
    this(NoCompressionCodec, messages: _*)
  }

  def getInitialOffset = initialOffset

  def getBuffer = buffer

  def getErrorCode = errorCode

  def serialized(): ByteBuffer = buffer

  def validBytes: Long = deepValidBytes
  
  def shallowValidBytes: Long = {
    if(shallowValidByteCount < 0) {
      val iter = deepIterator
      while(iter.hasNext) {
        val messageAndOffset = iter.next
        shallowValidByteCount = messageAndOffset.offset
      }
    }
    shallowValidByteCount - initialOffset
  }
  
  def deepValidBytes: Long = {
    if (deepValidByteCount < 0) {
      val iter = deepIterator
      while (iter.hasNext)
        iter.next
    }
    deepValidByteCount
  }

  /** Write the messages in this set to the given channel */
  def writeTo(channel: WritableByteChannel, offset: Long, size: Long): Long =
    channel.write(buffer.duplicate)
  
  override def iterator: Iterator[MessageAndOffset] = deepIterator

  private def deepIterator(): Iterator[MessageAndOffset] = {
    ErrorMapping.maybeThrowException(errorCode)
    new IteratorTemplate[MessageAndOffset] {
      var topIter = buffer.slice()
      var currValidBytes = initialOffset
      var innerIter:Iterator[MessageAndOffset] = null
      var lastMessageSize = 0L

      def innerDone():Boolean = (innerIter==null || !innerIter.hasNext)

      def makeNextOuter: MessageAndOffset = {
        if (topIter.remaining < 4) {
          deepValidByteCount = currValidBytes
          return allDone()
        }
        val size = topIter.getInt()
        lastMessageSize = size

        if(logger.isTraceEnabled) {
          logger.trace("Remaining bytes in iterator = " + topIter.remaining)
          logger.trace("size of data = " + size)
        }
        if(size < 0 || topIter.remaining < size) {
          deepValidByteCount = currValidBytes
          if (currValidBytes == 0 || size < 0)
            throw new InvalidMessageSizeException("invalid message size: %d only received bytes: %d " +
              " at %d possible causes (1) a single message larger than the fetch size; (2) log corruption "
                .format(size, topIter.remaining, currValidBytes))
          return allDone()
        }
        val message = topIter.slice()
        message.limit(size)
        topIter.position(topIter.position + size)
        val newMessage = new Message(message)
        newMessage.compressionCodec match {
          case NoCompressionCodec =>
            if(logger.isDebugEnabled)
              logger.debug("Message is uncompressed. Valid byte count = %d".format(currValidBytes))
            innerIter = null
            currValidBytes += 4 + size
            new MessageAndOffset(newMessage, currValidBytes)
          case _ =>
            if(logger.isDebugEnabled)
              logger.debug("Message is compressed. Valid byte count = %d".format(currValidBytes))
            innerIter = CompressionUtils.decompress(newMessage).deepIterator
            makeNext()
        }
      }

      override def makeNext(): MessageAndOffset = {
        if(logger.isDebugEnabled)
          logger.debug("makeNext() in deepIterator: innerDone = " + innerDone)
        innerDone match {
          case true => makeNextOuter
          case false => {
            val messageAndOffset = innerIter.next
            if(!innerIter.hasNext)
              currValidBytes += 4 + lastMessageSize
            new MessageAndOffset(messageAndOffset.message, currValidBytes)
          }
        }
      }
    }
  }

  def sizeInBytes: Long = buffer.limit
  
  override def toString: String = {
    val builder = new StringBuilder()
    builder.append("ByteBufferMessageSet(")
    for(message <- this) {
      builder.append(message)
      builder.append(", ")
    }
    builder.append(")")
    builder.toString
  }

  override def equals(other: Any): Boolean = {
    other match {
      case that: ByteBufferMessageSet =>
        (that canEqual this) && errorCode == that.errorCode && buffer.equals(that.buffer) && initialOffset == that.initialOffset
      case _ => false
    }
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ByteBufferMessageSet]

  override def hashCode: Int = 31 + (17 * errorCode) + buffer.hashCode + initialOffset.hashCode
}
