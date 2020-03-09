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

package kafka.log

import java.io.File

import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.common.utils.Time

object LogUtils {
  /**
    *  Create a segment with the given base offset
    */
  def createSegment(offset: Long,
                    logDir: File,
                    indexIntervalBytes: Int = 10,
                    time: Time = Time.SYSTEM): LogSegment = {
    val segDir = new File(logDir, String.valueOf(offset))
    segDir.mkdirs()
    val statusFile = new File(segDir, SegmentFile.STATUS.getName)
    SegmentStatusHandler.setStatus(statusFile, SegmentStatus.HOT)

    val ms = FileRecords.open(new File(segDir, SegmentFile.LOG.getName))
    val idx = LazyIndex.forOffset(new File(segDir, SegmentFile.OFFSET_INDEX.getName), offset, maxIndexSize = 1000)
    val timeIdx = LazyIndex.forTime(new File(segDir, SegmentFile.TIME_INDEX.getName), offset, maxIndexSize = 1500)
    val txnIndex = new TransactionIndex(offset, new File(segDir, SegmentFile.TXN_INDEX.getName))
    new LogSegment(ms, idx, timeIdx, txnIndex, offset, indexIntervalBytes, 0, time, statusFile)
  }
}
