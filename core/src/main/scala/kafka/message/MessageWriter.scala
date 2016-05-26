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

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.Crc32

class MessageWriter(segmentSize: Int) extends BufferingOutputStream(segmentSize) {

  import Message._

  def write(key: Array[Byte] = null,
            codec: CompressionCodec,
            timestamp: Long,
            timestampType: TimestampType,
            magicValue: Byte)(writePayload: OutputStream => Unit): Unit = {
    withCrc32Prefix {
      // write magic value
      write(magicValue)
      // write attributes
      var attributes: Byte = 0
      if (codec.codec > 0)
        attributes = (attributes | (CompressionCodeMask & codec.codec)).toByte
      if (magicValue > MagicValue_V0)
        attributes = timestampType.updateAttributes(attributes)
      write(attributes)
      // Write timestamp
      if (magicValue > MagicValue_V0)
        writeLong(timestamp)
      // write the key
      if (key == null) {
        writeInt(-1)
      } else {
        writeInt(key.length)
        write(key, 0, key.length)
      }
      // write the payload with length prefix
      withLengthPrefix {
        writePayload(this)
      }
    }
  }

  private def writeInt(value: Int): Unit = {
    write(value >>> 24)
    write(value >>> 16)
    write(value >>> 8)
    write(value)
  }

  private def writeInt(out: ReservedOutput, value: Int): Unit = {
    out.write(value >>> 24)
    out.write(value >>> 16)
    out.write(value >>> 8)
    out.write(value)
  }

  private def writeLong(value: Long): Unit = {
    write((value >>> 56).toInt)
    write((value >>> 48).toInt)
    write((value >>> 40).toInt)
    write((value >>> 32).toInt)
    write((value >>> 24).toInt)
    write((value >>> 16).toInt)
    write((value >>> 8).toInt)
    write(value.toInt)
  }

  private def withCrc32Prefix(writeData: => Unit): Unit = {
    // get a writer for CRC value
    val crcWriter = reserve(CrcLength)
    // save current position
    var seg = currentSegment
    val offset = currentSegment.written
    // write data
    writeData
    // compute CRC32
    val crc = new Crc32()
    if (offset < seg.written) crc.update(seg.bytes, offset, seg.written - offset)
    seg = seg.next
    while (seg != null) {
      if (seg.written > 0) crc.update(seg.bytes, 0, seg.written)
      seg = seg.next
    }
    // write CRC32
    writeInt(crcWriter, crc.getValue().toInt)
  }

  private def withLengthPrefix(writeData: => Unit): Unit = {
    // get a writer for length value
    val lengthWriter = reserve(ValueSizeLength)
    // save current size
    val oldSize = size
    // write data
    writeData
    // write length value
    writeInt(lengthWriter, size - oldSize)
  }

}

/*
 * OutputStream that buffers incoming data in segmented byte arrays
 * This does not copy data when expanding its capacity
 * It has a ability to
 * - write data directly to ByteBuffer
 * - copy data from an input stream to interval segmented arrays directly
 * - hold a place holder for an unknown value that can be filled in later
 */
class BufferingOutputStream(segmentSize: Int) extends OutputStream {

  protected final class Segment(size: Int) {
    val bytes = new Array[Byte](size)
    var written = 0
    var next: Segment = null
    def freeSpace: Int = bytes.length - written
  }

  protected class ReservedOutput(seg: Segment, offset: Int, length: Int) extends OutputStream {
    private[this] var cur = seg
    private[this] var off = offset
    private[this] var len = length

    override def write(value: Int) = {
      if (len <= 0) throw new IndexOutOfBoundsException()
      if (cur.bytes.length <= off) {
        cur = cur.next
        off = 0
      }
      cur.bytes(off) = value.toByte
      off += 1
      len -= 1
    }
  }

  protected var currentSegment = new Segment(segmentSize)
  private[this] val headSegment = currentSegment
  private[this] var filled = 0

  def size(): Int = filled + currentSegment.written

  override def write(b: Int): Unit = {
    if (currentSegment.freeSpace <= 0) addSegment()
    currentSegment.bytes(currentSegment.written) = b.toByte
    currentSegment.written += 1
  }

  override def write(b: Array[Byte], off: Int, len: Int) {
    if (off >= 0 && off <= b.length && len >= 0 && off + len <= b.length) {
      var remaining = len
      var offset = off
      while (remaining > 0) {
        if (currentSegment.freeSpace <= 0) addSegment()

        val amount = math.min(currentSegment.freeSpace, remaining)
        System.arraycopy(b, offset, currentSegment.bytes, currentSegment.written, amount)
        currentSegment.written += amount
        offset += amount
        remaining -= amount
      }
    } else {
      throw new IndexOutOfBoundsException()
    }
  }

  def write(in: InputStream): Unit = {
    var amount = 0
    while (amount >= 0) {
      currentSegment.written += amount
      if (currentSegment.freeSpace <= 0) addSegment()
      amount = in.read(currentSegment.bytes, currentSegment.written, currentSegment.freeSpace)
    }
  }

  private def addSegment() = {
    filled += currentSegment.written
    val newSeg = new Segment(segmentSize)
    currentSegment.next = newSeg
    currentSegment = newSeg
  }

  private def skip(len: Int): Unit = {
    if (len >= 0) {
      var remaining = len
      while (remaining > 0) {
        if (currentSegment.freeSpace <= 0) addSegment()

        val amount = math.min(currentSegment.freeSpace, remaining)
        currentSegment.written += amount
        remaining -= amount
      }
    } else {
      throw new IndexOutOfBoundsException()
    }
  }

  def reserve(len: Int): ReservedOutput = {
    val out = new ReservedOutput(currentSegment, currentSegment.written, len)
    skip(len)
    out
  }

  def writeTo(buffer: ByteBuffer): Unit = {
    var seg = headSegment
    while (seg != null) {
      buffer.put(seg.bytes, 0, seg.written)
      seg = seg.next
    }
  }

}
