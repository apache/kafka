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

import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FileLogInputStreamTest {

    @Test
    public void testWriteTo() throws IOException {
        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            fileRecords.append(MemoryRecords.withRecords(CompressionType.NONE, new KafkaRecord("foo".getBytes())));
            fileRecords.flush();

            FileLogInputStream logInputStream = new FileLogInputStream(fileRecords.channel(), Integer.MAX_VALUE, 0,
                    fileRecords.sizeInBytes());

            FileLogInputStream.FileChannelRecordBatch entry = logInputStream.nextBatch();
            assertNotNull(entry);

            ByteBuffer buffer = ByteBuffer.allocate(128);
            entry.writeTo(buffer);
            buffer.flip();

            MemoryRecords memRecords = MemoryRecords.readableRecords(buffer);
            List<Record> records = Utils.toList(memRecords.records().iterator());
            assertEquals(1, records.size());
        }
    }
}