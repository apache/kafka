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
package org.apache.kafka.common.record;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnalignedFileRecordsTest {

    private byte[][] values = new byte[][] {
            "foo".getBytes(),
            "bar".getBytes()
    };
    private FileRecords fileRecords;

    @BeforeEach
    public void setup() throws IOException {
        this.fileRecords = createFileRecords(values);
    }

    @AfterEach
    public void cleanup() throws IOException {
        this.fileRecords.close();
    }

    @Test
    public void testWriteTo() throws IOException {

        org.apache.kafka.common.requests.ByteBufferChannel channel = new org.apache.kafka.common.requests.ByteBufferChannel(fileRecords.sizeInBytes());
        int size = fileRecords.sizeInBytes();

        UnalignedFileRecords records1 = fileRecords.sliceUnaligned(0, size / 2);
        UnalignedFileRecords records2 = fileRecords.sliceUnaligned(size / 2, size - size / 2);

        records1.writeTo(channel, 0, records1.sizeInBytes());
        records2.writeTo(channel, 0, records2.sizeInBytes());

        channel.close();
        Iterator<Record> records = MemoryRecords.readableRecords(channel.buffer()).records().iterator();
        for (byte[] value : values) {
            assertTrue(records.hasNext());
            assertEquals(records.next().value(), ByteBuffer.wrap(value));
        }
    }

    private FileRecords createFileRecords(byte[][] values) throws IOException {
        FileRecords fileRecords = FileRecords.open(tempFile());

        for (byte[] value : values) {
            fileRecords.append(MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord(value)));
        }

        return fileRecords;
    }
}
