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

import org.apache.kafka.common.record._

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import scala.collection._
import org.scalatest.junit.JUnitSuite
import org.junit._
import org.junit.Assert._

class MessageCompressionTest extends JUnitSuite {

  @Test
  def testLZ4FramingV0() {
    val output = CompressionFactory(LZ4CompressionCodec, Message.MagicValue_V0, new ByteArrayOutputStream())
    assertTrue(output.asInstanceOf[KafkaLZ4BlockOutputStream].useBrokenFlagDescriptorChecksum())

    val input = CompressionFactory(LZ4CompressionCodec, Message.MagicValue_V0, new ByteArrayInputStream(Array[Byte](0x04, 0x22, 0x4D, 0x18, 0x60, 0x40, 0x1A)))
    assertTrue(input.asInstanceOf[KafkaLZ4BlockInputStream].ignoreFlagDescriptorChecksum())
  }

  @Test
  def testLZ4FramingV1() {
    val output = CompressionFactory(LZ4CompressionCodec, Message.MagicValue_V1, new ByteArrayOutputStream())
    assertFalse(output.asInstanceOf[KafkaLZ4BlockOutputStream].useBrokenFlagDescriptorChecksum())

    val input = CompressionFactory(LZ4CompressionCodec, Message.MagicValue_V1, new ByteArrayInputStream(Array[Byte](0x04, 0x22, 0x4D, 0x18, 0x60, 0x40, -126)))
    assertFalse(input.asInstanceOf[KafkaLZ4BlockInputStream].ignoreFlagDescriptorChecksum())
  }

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

  //  A quick test to ensure any growth or increase in compression size is known when upgrading libraries
  @Test
  def testCompressSize() {
    val bytes1k: Array[Byte] = (0 until 1000).map(_.toByte).toArray
    val bytes2k: Array[Byte] = (1000 until 2000).map(_.toByte).toArray
    val bytes3k: Array[Byte] = (3000 until 4000).map(_.toByte).toArray
    val messages: List[Message] = List(new Message(bytes1k, Message.NoTimestamp, Message.MagicValue_V1),
                                       new Message(bytes2k, Message.NoTimestamp, Message.MagicValue_V1),
                                       new Message(bytes3k, Message.NoTimestamp, Message.MagicValue_V1))

    testCompressSize(GZIPCompressionCodec, messages, 396)

    if(isSnappyAvailable)
      testCompressSize(SnappyCompressionCodec, messages, 1063)

    if(isLZ4Available)
      testCompressSize(LZ4CompressionCodec, messages, 387)
  }

  def testSimpleCompressDecompress(compressionCodec: CompressionCodec) {
    val messages = List[Message](new Message("hi there".getBytes), new Message("I am fine".getBytes), new Message("I am not so well today".getBytes))
    val messageSet = new ByteBufferMessageSet(compressionCodec = compressionCodec, messages = messages:_*)
    assertEquals(compressionCodec, messageSet.shallowIterator.next().message.compressionCodec)
    val decompressed = messageSet.iterator.map(_.message).toList
    assertEquals(messages, decompressed)
  }

  def testCompressSize(compressionCodec: CompressionCodec, messages: List[Message], expectedSize: Int) {
    val messageSet = new ByteBufferMessageSet(compressionCodec = compressionCodec, messages = messages:_*)
    assertEquals(s"$compressionCodec size has changed.", expectedSize, messageSet.sizeInBytes)
  }

  def isSnappyAvailable: Boolean = {
    try {
      new org.xerial.snappy.SnappyOutputStream(new ByteArrayOutputStream())
      true
    } catch {
      case _: UnsatisfiedLinkError | _: org.xerial.snappy.SnappyError => false
    }
  }

  def isLZ4Available: Boolean = {
    try {
      new net.jpountz.lz4.LZ4BlockOutputStream(new ByteArrayOutputStream())
      true
    } catch {
      case _: UnsatisfiedLinkError => false
    }
  }
}
