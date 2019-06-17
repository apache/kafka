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

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util
import java.util.zip.CRC32

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
                               rdi: Array[Byte]) {
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

object RemoteLogIndexEntry {
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

  def parseEntry(ch: FileChannel, position: Long): RemoteLogIndexEntry = {
    val magicBuffer = ByteBuffer.allocate(2)
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
          RemoteLogIndexEntry(magic, crc, firstOffset, lastOffset, firstTimestamp, lastTimestamp, dataLength,
            rdiBuffer.array())
        case _ =>
          // TODO: Throw Custom Exceptions?
          throw new RuntimeException("magic version " + magic + " is not supported")
      }
    } else {
      // TODO: log
      println("Reached limit of the file")
      null
    }
  }

  def readAll(ch: FileChannel): Seq[RemoteLogIndexEntry] = {
    var index = Seq[RemoteLogIndexEntry]()

    var pos = 0L;
    while (pos < ch.size()) {
      val entry = parseEntry(ch, pos)
      index = index :+ entry
      pos += entry.entryLength + 4;
    }

    index
  }
}