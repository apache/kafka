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

import java.io.{InputStream, ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util.Random
import org.apache.kafka.common.record.TimestampType
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class MessageWriterTest extends JUnitSuite {

  private val rnd = new Random()

  private def mkRandomArray(size: Int): Array[Byte] = {
    (0 until size).map(_ => rnd.nextInt(10).toByte).toArray
  }

  private def mkMessageWithWriter(key: Array[Byte] = null, bytes: Array[Byte], codec: CompressionCodec): Message = {
    val writer = new MessageWriter(100)
    writer.write(key = key, codec = codec, timestamp = Message.NoTimestamp, timestampType = TimestampType.CREATE_TIME, magicValue = Message.MagicValue_V1) { output =>
      val out = if (codec == NoCompressionCodec) output else CompressionFactory(codec, Message.MagicValue_V1, output)
      try {
        val p = rnd.nextInt(bytes.length)
        out.write(bytes, 0, p)
        out.write(bytes, p, bytes.length - p)
      } finally {
        out.close()
      }
    }
    val bb = ByteBuffer.allocate(writer.size)
    writer.writeTo(bb)
    bb.rewind()
    new Message(bb)
  }

  private def compress(bytes: Array[Byte], codec: CompressionCodec): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val out = CompressionFactory(codec, Message.MagicValue_V1, baos)
    out.write(bytes)
    out.close()
    baos.toByteArray
  }

  private def decompress(compressed: Array[Byte], codec: CompressionCodec): Array[Byte] = {
    toArray(CompressionFactory(codec, Message.MagicValue_V1, new ByteArrayInputStream(compressed)))
  }

  private def toArray(in: InputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val buf = new Array[Byte](100)
    var amount = in.read(buf)
    while (amount >= 0) {
      out.write(buf, 0, amount)
      amount = in.read(buf)
    }
    out.toByteArray
  }

  private def toArray(bb: ByteBuffer): Array[Byte] = {
    val arr = new Array[Byte](bb.limit())
    bb.get(arr)
    bb.rewind()
    arr
  }

  @Test
  def testBufferingOutputStream(): Unit = {
    val out = new BufferingOutputStream(50)
    out.write(0)
    out.write(1)
    out.write(2)
    val r = out.reserve(100)
    out.write((103 until 200).map(_.toByte).toArray)
    r.write((3 until 103).map(_.toByte).toArray)

    val buf = ByteBuffer.allocate(out.size)
    out.writeTo(buf)
    buf.rewind()

    assertEquals((0 until 200).map(_.toByte), buf.array.toSeq)
  }

  @Test
  def testWithNoCompressionAttribute(): Unit = {
    val bytes = mkRandomArray(4096)
    val actual = mkMessageWithWriter(bytes = bytes, codec = NoCompressionCodec)
    val expected = new Message(bytes, Message.NoTimestamp, NoCompressionCodec, Message.MagicValue_V1)
    assertEquals(expected.buffer, actual.buffer)
  }

  @Test
  def testWithCompressionAttribute(): Unit = {
    val bytes = mkRandomArray(4096)
    val actual = mkMessageWithWriter(bytes = bytes, codec = SnappyCompressionCodec)
    val expected = new Message(compress(bytes, SnappyCompressionCodec), Message.NoTimestamp, SnappyCompressionCodec, Message.MagicValue_V1)

    assertEquals(
      decompress(toArray(expected.payload), SnappyCompressionCodec).toSeq,
      decompress(toArray(actual.payload), SnappyCompressionCodec).toSeq
    )
  }

  @Test
  def testWithKey(): Unit = {
    val key = mkRandomArray(123)
    val bytes = mkRandomArray(4096)
    val actual = mkMessageWithWriter(bytes = bytes, key = key, codec = NoCompressionCodec)
    val expected = new Message(bytes = bytes, key = key, timestamp = Message.NoTimestamp, codec = NoCompressionCodec, magicValue = Message.MagicValue_V1)

    assertEquals(expected.buffer, actual.buffer)
  }

}
