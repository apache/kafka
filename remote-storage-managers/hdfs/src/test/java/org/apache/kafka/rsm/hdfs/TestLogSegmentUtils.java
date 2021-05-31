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
package org.apache.kafka.rsm.hdfs;

import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.test.TestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

public class TestLogSegmentUtils {

    public static final String LOG_FILE_NAME = "log";
    public static final String OFFSET_INDEX_FILE_NAME = "index";
    public static final String TIME_INDEX_FILE_NAME = "time";
    public static final String TXN_INDEX_FILE_NAME = "txn";
    public static final String PRODUCER_SNAPSHOT_FILE_NAME = "snapshot";

    public static LogSegmentData createLogSegmentData(File logDir,
                                                      int startOffset,
                                                      int segSize,
                                                      boolean withOptionalFiles) throws IOException {
        String prefix = String.format("%020d", startOffset);
        Path segment = new File(logDir, prefix + "." + LOG_FILE_NAME).toPath();
        int optimalSize = 1024 * 1024;
        if (segSize < optimalSize) {
            Files.write(segment, TestUtils.randomBytes(segSize));
        } else {
            Files.createFile(segment);
            int segSizeRemainingToFill = segSize;
            while (segSizeRemainingToFill > 0) {
                int bufferLength = Math.min(segSizeRemainingToFill, optimalSize);
                Files.write(segment, TestUtils.randomBytes(bufferLength), StandardOpenOption.APPEND);
                segSizeRemainingToFill -= bufferLength;
            }
        }

        Path offsetIndex = new File(logDir, prefix + "." + OFFSET_INDEX_FILE_NAME).toPath();
        Files.write(offsetIndex, TestUtils.randomBytes(10));

        Path timeIndex = new File(logDir, prefix + "." + TIME_INDEX_FILE_NAME).toPath();
        Files.write(timeIndex, TestUtils.randomBytes(10));

        ByteBuffer leaderEpochIndex = ByteBuffer.wrap(TestUtils.randomBytes(10));
        leaderEpochIndex.rewind();

        Path producerSnapshotIndex = new File(logDir, prefix + "." + PRODUCER_SNAPSHOT_FILE_NAME).toPath();
        Files.write(producerSnapshotIndex, TestUtils.randomBytes(10));

        Path txnIndex = null;
        if (withOptionalFiles) {
            txnIndex = new File(logDir, prefix + "." + TXN_INDEX_FILE_NAME).toPath();
            Files.write(txnIndex, TestUtils.randomBytes(10));
        }
        return new LogSegmentData(segment, offsetIndex, timeIndex, Optional.ofNullable(txnIndex),
                producerSnapshotIndex, leaderEpochIndex);
    }
}
