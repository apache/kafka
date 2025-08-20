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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.utils.Time;

import java.io.File;
import java.io.IOException;

public class LogTestUtils {
    public static LogSegment createSegment(long offset, File logDir, int indexIntervalBytes, Time time) throws IOException {
        // Create instances of the required components
        FileRecords ms = FileRecords.open(LogFileUtils.logFile(logDir, offset));
        LazyIndex<OffsetIndex> idx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(logDir, offset), offset, 1000);
        LazyIndex<TimeIndex> timeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(logDir, offset), offset, 1500);
        TransactionIndex txnIndex = new TransactionIndex(offset, LogFileUtils.transactionIndexFile(logDir, offset, ""));

        // Create and return the LogSegment instance
        return new LogSegment(ms, idx, timeIdx, txnIndex, offset, indexIntervalBytes, 0, time);
    }
}
