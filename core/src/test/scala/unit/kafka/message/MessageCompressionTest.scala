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

import java.io.ByteArrayOutputStream
import scala.collection._
import org.scalatest.junit.JUnitSuite
import org.junit._
import junit.framework.Assert._

class MessageCompressionTest extends JUnitSuite {
  
  @Test
  def testSimpleCompressDecompress() {
    val codecs = mutable.ArrayBuffer[CompressionCodec](GZIPCompressionCodec)
    if(isSnappyAvailable)
      codecs += SnappyCompressionCodec
    if(isLZ4Available)
      codecs += LZ4CompressionCodec
    for(codec <- codecs)
      testSimpleCompressDecompress(codec)
  }
  
  def testSimpleCompressDecompress(compressionCodec: CompressionCodec) {
    val messages = List[Message](new Message("hi there".getBytes), new Message("I am fine".getBytes), new Message("I am not so well today".getBytes))
    val messageSet = new ByteBufferMessageSet(compressionCodec = compressionCodec, messages = messages:_*)
    assertEquals(compressionCodec, messageSet.shallowIterator.next.message.compressionCodec)
    val decompressed = messageSet.iterator.map(_.message).toList
    assertEquals(messages, decompressed)
  }

  @Test
  def testComplexCompressDecompress() {
    val messages = List(new Message("hi there".getBytes), new Message("I am fine".getBytes), new Message("I am not so well today".getBytes))
    val message = new ByteBufferMessageSet(compressionCodec = DefaultCompressionCodec, messages = messages.slice(0, 2):_*)
    val complexMessages = List(message.shallowIterator.next.message):::messages.slice(2,3)
    val complexMessage = new ByteBufferMessageSet(compressionCodec = DefaultCompressionCodec, messages = complexMessages:_*)
    val decompressedMessages = complexMessage.iterator.map(_.message).toList
    assertEquals(messages, decompressedMessages)
  }
  
  def isSnappyAvailable(): Boolean = {
    try {
      val snappy = new org.xerial.snappy.SnappyOutputStream(new ByteArrayOutputStream())
      true
    } catch {
      case e: UnsatisfiedLinkError => false
      case e: org.xerial.snappy.SnappyError => false
    }
  }

  def isLZ4Available(): Boolean = {
    try {
      val lz4 = new net.jpountz.lz4.LZ4BlockOutputStream(new ByteArrayOutputStream())
      true
    } catch {
      case e: UnsatisfiedLinkError => false
    }
  }
}
