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

package org.apache.kafka.common.network;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileChannelSendTest {

    private FileChannel fileChannel;
    private byte[][] values = new byte[][] {Utils.utf8("foo"), Utils.utf8("bar")};

    @BeforeEach
    public void setup() throws IOException {
        this.fileChannel = createReadableFileChannel(values);
    }

    @Test
    public void testToSend() throws IOException {
        Send send = new FileChannelSend(fileChannel, 0, Math.toIntExact(fileChannel.size()));
        ByteBuffer readBuffer = TestUtils.toBuffer(send);
        Iterator<Record> records = MemoryRecords.readableRecords(readBuffer)
                .records().iterator();
        for (byte[] value : values) {
            assertTrue(records.hasNext());
            assertEquals(records.next().value(), ByteBuffer.wrap(value));
        }
    }

    private FileChannel createReadableFileChannel(byte[][] values) throws IOException {
        FileRecords fileRecords = FileRecords.open(tempFile());

        for (byte[] value : values) {
            fileRecords.append(MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord(value)));
        }
        fileRecords.close();

        return FileChannel.open(fileRecords.file().toPath());
    }

}
