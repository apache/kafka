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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogSegmentDataHeaderTest {

    private File logDir;

    @BeforeEach
    public void before() {
        logDir = TestUtils.tempDirectory();
    }

    @AfterEach
    public void tearDown() throws Exception {
        Utils.delete(logDir);
    }

    @Test
    void getDataPosition() throws IOException {
        LogSegmentDataHeader dataHeader = createLogSegmentHeader(true);
        int startPos = LogSegmentDataHeader.LENGTH;
        int idxFileSize = 10;
        for (LogSegmentDataHeader.FileType fileType : LogSegmentDataHeader.FileType.values()) {
            LogSegmentDataHeader.DataPosition dataPosition = dataHeader.getDataPosition(fileType);
            int length = fileType == LogSegmentDataHeader.FileType.SEGMENT ? Integer.MAX_VALUE : idxFileSize;
            assertEquals(new LogSegmentDataHeader.DataPosition(startPos, length), dataPosition);
            startPos += length;
        }
    }

    @Test
    void testSerde() throws IOException {
        LogSegmentDataHeader expected = createLogSegmentHeader(true);
        byte[] serializedBytes = LogSegmentDataHeader.serialize(expected);
        LogSegmentDataHeader actual = LogSegmentDataHeader.deserialize(ByteBuffer.wrap(serializedBytes));
        assertEquals(expected, actual);
    }

    @Test
    void testCreateLogSegmentDataHeaderWithoutOptionalFiles() throws IOException {
        createLogSegmentHeader(false);
    }

    @Test
    void testCreateLogSegmentDataHeader() throws IOException {
        createLogSegmentHeader(true);
    }

    private LogSegmentDataHeader createLogSegmentHeader(boolean withOptional) throws IOException {
        LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, 1000, withOptional);
        LogSegmentDataHeader dataHeader = LogSegmentDataHeader.create(segmentData);
        assertEquals(LogSegmentDataHeader.CURRENT_VERSION, dataHeader.version());
        assertEquals(6, dataHeader.filePositions().size());
        return dataHeader;
    }
}