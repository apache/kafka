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

import scala.reflect.BeanProperty
import kafka.utils.Logging
import java.nio.ByteBuffer
import java.nio.channels._
import java.io.{InputStream, ByteArrayOutputStream, DataOutputStream}
import java.util.concurrent.atomic.AtomicLong
import kafka.utils.IteratorTemplate

object ByteBufferMessageSet {
  
  private def create(offsetCounter: AtomicLong, compressionCodec: CompressionCodec, messages: Message*): ByteBuffer = {
    if(messages.size == 0) {
      MessageSet.Empty.buffer
    } else if(compressionCodec == NoCompressionCodec) {
      val buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
      for(message <- messages)
        writeMessage(buffer, message, offsetCounter.getAndIncrement)
      buffer.rewind()
      buffer
    } else {
      val byteArrayStream = new ByteArrayOutputStream(MessageSet.messageSetSize(messages))
      val output = new DataOutputStream(CompressionFactory(compressionCodec, byteArrayStream))
      var offset = -1L
      try {
        for(message <- messages) {
          offset = offsetCounter.getAndIncrement
          output.writeLong(offset)
          output.writeInt(message.size)
          output.write(message.buffer.array, message.buffer.arrayOffset, message.buffer.limit)
        }
      } finally {
        output.close()
      }
      val bytes = byteArrayStream.toByteArray
      val message = new Message(bytes, compressionCodec)
      val buffer = ByteBuffer.allocate(message.size + MessageSet.LogOverhead)
      writeMessage(buffer, message, offset)
      buffer.rewind()
      buffer
    }
  }
  
  def decompress(message: Message): ByteBufferMessageSet = {
    val outputStream: ByteArrayOutputStream = new ByteArrayOutputStream
    val inputStream: InputStream = new ByteBufferBackedInputStream(message.payload)
    val intermediateBuffer = new Array[Byte](1024)
    val compressed = CompressionFactory(message.compressionCodec, inputStream)
    try {
      Stream.continually(compressed.read(intermediateBuffer)).takeWhile(_ > 0).foreach { dataRead =>
        outputStream.write(intermediateBuffer, 0, dataRead)
      }
    } finally {
      compressed.close()
    }
    val outputBuffer = ByteBuffer.allocate(outputStream.size)
    outputBuffer.put(outputStream.toByteArray)
    outputBuffer.rewind
    new ByteBufferMessageSet(outputBuffer)
  }
    
  private def writeMessage(buffer: ByteBuffer, message: Message, offset: Long) {
    buffer.putLong(offset)
    buffer.putInt(message.size)
    buffer.put(message.buffer)
    message.buffer.rewind()
  }
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
class ByteBufferMessageSet(@BeanProperty val buffer: ByteBuffer) extends MessageSet with Logging {
  private var shallowValidByteCount = -1

  def this(compressionCodec: CompressionCodec, messages: Message*) {
    this(ByteBufferMessageSet.create(new AtomicLong(0), compressionCodec, messages:_*))
  }
  
  def this(compressionCodec: CompressionCodec, offsetCounter: AtomicLong, messages: Message*) {
    this(ByteBufferMessageSet.create(offsetCounter, compressionCodec, messages:_*))
  }

  def this(messages: Message*) {
    this(NoCompressionCodec, new AtomicLong(0), messages: _*)
  }

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

  /** default iterator that iterates over decompressed messages */
  override def iterator: Iterator[MessageAndOffset] = internalIterator()

  /** iterator over compressed messages without decompressing */
  def shallowIterator: Iterator[MessageAndOffset] = internalIterator(true)

  /** When flag isShallow is set to be true, we do a shallow iteration: just traverse the first level of messages. **/
  private def internalIterator(isShallow: Boolean = false): Iterator[MessageAndOffset] = {
    new IteratorTemplate[MessageAndOffset] {
      var topIter = buffer.slice()
      var innerIter: Iterator[MessageAndOffset] = null

      def innerDone():Boolean = (innerIter == null || !innerIter.hasNext)

      def makeNextOuter: MessageAndOffset = {
        // if there isn't at least an offset and size, we are done
        if (topIter.remaining < 12)
          return allDone()
        val offset = topIter.getLong()
        val size = topIter.getInt()
        if(size < Message.MinHeaderSize)
          throw new InvalidMessageException("Message found with corrupt size (" + size + ")")
        
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
              innerIter = ByteBufferMessageSet.decompress(newMessage).internalIterator()
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
            innerIter.next
        }
      }
      
    }
  }
  
  /**
   * Update the offsets for this message set. This method attempts to do an in-place conversion
   * if there is no compression, but otherwise recopies the messages
   */
  private[kafka] def assignOffsets(offsetCounter: AtomicLong, codec: CompressionCodec): ByteBufferMessageSet = {
    if(codec == NoCompressionCodec) {
      // do an in-place conversion
      var position = 0
      buffer.mark()
      while(position < sizeInBytes - MessageSet.LogOverhead) {
        buffer.position(position)
        buffer.putLong(offsetCounter.getAndIncrement())
        position += MessageSet.LogOverhead + buffer.getInt()
      }
      buffer.reset()
      this
    } else {
      // messages are compressed, crack open the messageset and recompress with correct offset
      val messages = this.internalIterator(isShallow = false).map(_.message)
      new ByteBufferMessageSet(compressionCodec = codec, offsetCounter = offsetCounter, messages = messages.toBuffer:_*)
    }
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
