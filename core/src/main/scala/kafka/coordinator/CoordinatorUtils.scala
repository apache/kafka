/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.coordinator

import java.nio.ByteBuffer

import kafka.log.Log
import kafka.utils.Logging
import org.apache.kafka.common.record.{FileRecords, MemoryRecords}
import org.apache.kafka.common.requests.IsolationLevel

object CoordinatorUtils extends Logging {

  /**
   * Read records from `log` and return them as `MemoryRecords` using `cachedBuffer` if possible.
   *
   * A new buffer will be allocated if `cachedBuffer` is smaller than `minLoadBufferSize` or the first record batch.
   * As such, callers should replace their cached buffer with the returned ByteBuffer to minimise buffer allocations.
   */
  def readRecords(log: Log, startOffset: Long, cachedBuffer: ByteBuffer,
                  minLoadBufferSize: Int): (MemoryRecords, ByteBuffer) = {
    val fetchDataInfo = log.read(startOffset, minLoadBufferSize, maxOffset = None, minOneMessage = true,
      isolationLevel = IsolationLevel.READ_UNCOMMITTED)
    var buffer = cachedBuffer
    val memRecords = fetchDataInfo.records match {
      case records: MemoryRecords => records
      case fileRecords: FileRecords =>
        val sizeInBytes = fileRecords.sizeInBytes
        val bytesNeeded = Math.max(minLoadBufferSize, sizeInBytes)

        // minOneMessage = true in the above log.read means that the buffer may need to be grown to ensure progress can
        // be made
        if (buffer.capacity < bytesNeeded)
          buffer = ByteBuffer.allocate(bytesNeeded)
        else
          buffer.clear()

        fileRecords.readInto(buffer, 0)
        MemoryRecords.readableRecords(buffer)
    }
    (memRecords, buffer)
  }

}
