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
import java.util

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
 * @param rdiLength      length of bytes to be read to construct rdi.
 * @param rdi            bytes value of rdi.
 */
case class RemoteLogIndexEntry(magic: Short, crc: Int, firstOffset: Long, lastOffset: Long,
                               firstTimeStamp: Long, lastTimeStamp: Long, dataLength: Int, rdiLength: Short,
                               rdi: Array[Byte]) {

  /**
   * @return bytes length of this entry value
   */
  def entryLength: Int = {
    (4 // crc - int
      + 8 // firstOffset - long
      + 8 // lasstOffset - long
      + 8 // firstTimestamp - long
      + 8 // lastTimestamp - long
      + 4 // dataLength - int
      + 2 // rdiLength type
      + rdiLength)
  }

  override def equals(any: Any): Boolean = {
    any match {
      case that: RemoteLogIndexEntry if this.magic == that.magic && this.crc== that.crc &&
        this.firstOffset == that.firstOffset && this.lastOffset == that.lastOffset &&
        this.firstTimeStamp == that.firstTimeStamp && this.lastTimeStamp == that.lastTimeStamp &&
        this.dataLength == that.dataLength && this.rdiLength == that.rdiLength &&
        util.Arrays.equals(this.rdi, that.rdi) => true
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val state = Seq(magic, crc, firstOffset, lastOffset, firstTimeStamp, lastTimeStamp, dataLength, rdiLength, rdi)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  def asBuffer: ByteBuffer = {
    val buffer = ByteBuffer.allocate(entryLength + 2 + 4)
    buffer.putShort(magic)
    buffer.putInt(entryLength)
    buffer.putInt(crc)
    buffer.putLong(firstOffset)
    buffer.putLong(lastOffset)
    buffer.putLong(firstTimeStamp)
    buffer.putLong(lastTimeStamp)
    buffer.putInt(dataLength)
    buffer.putShort(rdiLength)
    buffer.put(rdi)
    buffer.flip()
    buffer
  }

}
