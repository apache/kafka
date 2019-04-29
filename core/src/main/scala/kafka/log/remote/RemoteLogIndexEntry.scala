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

/**
 * Entry representation in a remote log index
 *
 * @param magic          magic version of protocol
 * @param length         bytes length of this entry value
 * @param crc            checksum value of the entry
 * @param firstOffset    offset value of first record for this entry stored at respective { @link #rdi}
 * @param lastOffset     offset value of last record for this entry stored at respective { @link #rdi}
 * @param firstTimeStamp timestamp value of first record for this entry stored at respective { @link #rdi}
 * @param lastTimeStamp  timestamp value of last record for this entry stored at respective { @link #rdi}
 * @param dataLength     length of the data stored in remote tier at rdi.
 * @param rdiLength      length of bytes to be read to construct rdi.
 * @param rdi            bytes value of rdi.
 */
case class RemoteLogIndexEntry(magic: Short, length: Int, crc: Int, firstOffset: Long, lastOffset: Long,
                               firstTimeStamp: Long, lastTimeStamp: Long, dataLength: Int, rdiLength: Short,
                               rdi: Array[Byte]) {
  def asBuffer: ByteBuffer = {
    val buffer = ByteBuffer.allocate(2 + length)
    buffer.putShort(magic)
    buffer.putInt(crc)
    buffer.putLong(firstOffset)
    buffer.putLong(lastOffset)
    buffer.putLong(firstTimeStamp)
    buffer.putLong(lastTimeStamp)
    buffer.putInt(dataLength)
    buffer.putShort(rdiLength)
    buffer.put(rdi)
    buffer
  }
}
