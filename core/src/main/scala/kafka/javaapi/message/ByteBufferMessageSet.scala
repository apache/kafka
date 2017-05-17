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
package kafka.javaapi.message

import java.nio.ByteBuffer

import kafka.common.LongRef
import kafka.message._

import scala.collection.JavaConverters._

class ByteBufferMessageSet(val buffer: ByteBuffer) extends MessageSet {
  private val underlying: kafka.message.ByteBufferMessageSet = new kafka.message.ByteBufferMessageSet(buffer)
  
  def this(compressionCodec: CompressionCodec, messages: java.util.List[Message]) {
    this(new kafka.message.ByteBufferMessageSet(compressionCodec, new LongRef(0), messages.asScala: _*).buffer)
  }

  def this(messages: java.util.List[Message]) {
    this(NoCompressionCodec, messages)
  }

  def validBytes: Int = underlying.validBytes

  def getBuffer = buffer

  override def iterator: java.util.Iterator[MessageAndOffset] = new java.util.Iterator[MessageAndOffset] {
    val underlyingIterator = underlying.iterator
    override def hasNext(): Boolean = {
      underlyingIterator.hasNext
    }

    override def next(): MessageAndOffset = {
      underlyingIterator.next
    }

    override def remove = throw new UnsupportedOperationException("remove API on MessageSet is not supported")
  }

  override def toString: String = underlying.toString

  def sizeInBytes: Int = underlying.sizeInBytes

  override def equals(other: Any): Boolean = {
    other match {
      case that: ByteBufferMessageSet => buffer.equals(that.buffer)
      case _ => false
    }
  }


  override def hashCode: Int = buffer.hashCode
}
