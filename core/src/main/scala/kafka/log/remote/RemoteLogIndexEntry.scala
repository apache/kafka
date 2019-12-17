/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log.remote

import java.io.InputStream
import java.nio.{BufferUnderflowException, ByteBuffer}
import java.nio.channels.FileChannel
import java.util
import java.util.zip.CRC32

import kafka.common.KafkaException
import org.apache.kafka.common.utils.Utils
import kafka.utils.Logging

/**
 * Entry representation in a remote log index
 *
 * @param magic          magic version of protocol
 * @param crc            checksum value of the entry
 * @param firstOffset    offset value of the first record for this entry stored at respective { @link #rdi}
 * @param lastOffset     offset value of the last record for this entry stored at respective { @link #rdi}
 * @param firstTimeStamp timestamp value of the first record for this entry stored at respective { @link #rdi}
 * @param lastTimeStamp  timestamp value of the last record for this entry stored at respective { @link #rdi}
 * @param dataLength     length of the data stored in remote tier at rdi.
 * @param rdi            bytes value of rdi.
 */
case class RemoteLogIndexEntry(magic: Short, crc: Int, firstOffset: Long, lastOffset: Long,
                               firstTimeStamp: Long, lastTimeStamp: Long, dataLength: Int,
                               rdi: Array[Byte]) extends Logging {
  /**
   * @return bytes length of this entry value
   */
  def entryLength: Short = {
    (4 // crc - int
      + 8 // firstOffset - long
      + 8 // lastOffset - long
      + 8 // firstTimestamp - long
      + 8 // lastTimestamp - long
      + 4 // dataLength - int
      + 2 // rdiLength - short
      + rdi.length).asInstanceOf[Short]
  }

  def totalLength: Short = (entryLength + 4).asInstanceOf[Short]

  override def equals(any: Any): Boolean = {
    any match {
      case that: RemoteLogIndexEntry if this.magic == that.magic && this.crc== that.crc &&
        this.firstOffset == that.firstOffset && this.lastOffset == that.lastOffset &&
        this.firstTimeStamp == that.firstTimeStamp && this.lastTimeStamp == that.lastTimeStamp &&
        this.dataLength == that.dataLength &&
        util.Arrays.equals(this.rdi, that.rdi) => true
      case _ => false
    }
  }

  override def hashCode(): Int = {
    crc
  }

  def asBuffer: ByteBuffer = {
    val buffer = ByteBuffer.allocate(entryLength + 2 + 2)
    buffer.putShort(magic)
    buffer.putShort(entryLength)
    buffer.putInt(crc)
    buffer.putLong(firstOffset)
    buffer.putLong(lastOffset)
    buffer.putLong(firstTimeStamp)
    buffer.putLong(lastTimeStamp)
    buffer.putInt(dataLength)
    buffer.putShort(rdi.length.asInstanceOf[Short])
    buffer.put(rdi)
    buffer.flip()
    buffer
  }

}

object RemoteLogIndexEntry extends Logging {
  def apply (firstOffset: Long, lastOffset: Long, firstTimeStamp: Long, lastTimeStamp: Long,
             dataLength: Int, rdi: Array[Byte]): RemoteLogIndexEntry = {

    val length = (8 // firstOffset - long
        + 8 // lastOffset - long
        + 8 // firstTimestamp - long
        + 8 // lastTimestamp - long
        + 4 // dataLength - int
        + 2 // rdiLength - short
        + rdi.length)

    val buffer = ByteBuffer.allocate(length)
    buffer.putLong(firstOffset)
    buffer.putLong(lastOffset)
    buffer.putLong(firstTimeStamp)
    buffer.putLong(lastTimeStamp)
    buffer.putInt(dataLength)
    buffer.putShort(rdi.length.asInstanceOf[Short])
    buffer.put(rdi)
    buffer.flip()

    val crc32 = new CRC32
    crc32.update(buffer)
    val crc = crc32.getValue.asInstanceOf[Int]

    val entry = RemoteLogIndexEntry(0, crc, firstOffset, lastOffset, firstTimeStamp, lastTimeStamp, dataLength, rdi)
    entry
  }

  def readAll(is: InputStream): Seq[RemoteLogIndexEntry] = {
    var index = Seq.empty[RemoteLogIndexEntry]

    var done = false
    while (!done) {
      parseEntry(is) match {
        case Some(entry) =>
          index = index :+ entry
        case _ =>
          done = true
      }
    }

    index
  }

  private def parseEntry(is: InputStream): Option[RemoteLogIndexEntry] = {
    val magicAndEntryLengthBuffer = ByteBuffer.allocate(java.lang.Short.BYTES + java.lang.Short.BYTES)
    Utils.readFully(is, magicAndEntryLengthBuffer)
    if (magicAndEntryLengthBuffer.hasRemaining) {
      None
    } else {
      magicAndEntryLengthBuffer.flip()
      val magic = magicAndEntryLengthBuffer.getShort()
      magic match {
        case 0 =>
          val entryLength = magicAndEntryLengthBuffer.getShort()
          val valueBuffer = ByteBuffer.allocate(entryLength)
          Utils.readFully(is, valueBuffer)
          valueBuffer.flip()

          try {
            val crc = valueBuffer.getInt()
            val firstOffset = valueBuffer.getLong()
            val lastOffset = valueBuffer.getLong()
            val firstTimestamp = valueBuffer.getLong()
            val lastTimestamp = valueBuffer.getLong()
            val dataLength = valueBuffer.getInt()
            val rdiLength = valueBuffer.getShort()
            val rdi = Array.ofDim[Byte](rdiLength)
            valueBuffer.get(rdi)
            Some(RemoteLogIndexEntry(magic, crc, firstOffset, lastOffset, firstTimestamp, lastTimestamp, dataLength, rdi))
          } catch {
            case e: BufferUnderflowException =>
              // TODO decide how to deal with incomplete records
              throw new KafkaException(e)
          }
        case _ =>
          // TODO: Throw Custom Exceptions?
          throw new RuntimeException("magic version " + magic + " is not supported")
      }
    }
  }

  def parseEntry(ch: FileChannel, position: Long): Option[RemoteLogIndexEntry] = {
    val magicBuffer = ByteBuffer.allocate(2)
    // The last 8 bytes of remote log index file is the "last offset" rather than a regular index entry
    if (position + 8 >= ch.size)
      return None
    val readCt = ch.read(magicBuffer, position)
    if (readCt > 0) {
      magicBuffer.flip()
      val magic = magicBuffer.getShort
      magic match {
        case 0 =>
          val valBuffer = ByteBuffer.allocate(4)
          val nextPos = position + 2
          ch.read(valBuffer, nextPos)
          valBuffer.flip()
          val length = valBuffer.getShort

          val valueBuffer = ByteBuffer.allocate(length)
          ch.read(valueBuffer, nextPos + 2)
          valueBuffer.flip()

          val crc = valueBuffer.getInt
          val firstOffset = valueBuffer.getLong
          val lastOffset = valueBuffer.getLong
          val firstTimestamp = valueBuffer.getLong
          val lastTimestamp = valueBuffer.getLong
          val dataLength = valueBuffer.getInt
          val rdiLength = valueBuffer.getShort
          val rdiBuffer = ByteBuffer.allocate(rdiLength)
          valueBuffer.get(rdiBuffer.array())
          Some(RemoteLogIndexEntry(magic, crc, firstOffset, lastOffset, firstTimestamp, lastTimestamp, dataLength,
            rdiBuffer.array()))
        case _ =>
          // TODO: Throw Custom Exceptions?
          throw new RuntimeException("magic version " + magic + " is not supported")
      }
    } else {
      debug(s"Reached limit of the file for channel:$ch for position: $position")
      None
    }
  }

  def readAll(ch: FileChannel): Seq[RemoteLogIndexEntry] = {
    var index = Seq[RemoteLogIndexEntry]()

    var pos = 0L;
    while (pos < ch.size()) {
      val entryOption = parseEntry(ch, pos.toInt)
      val entry = entryOption.getOrElse(throw new KafkaException(s"Entry could not be found at position: $pos"))
      index = index :+ entry
      pos += entry.entryLength + 4
    }

    index
  }
}