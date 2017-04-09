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
import static org.junit.Assert.assertTrue;

public class FileLogInputStreamTest {

    @Test
    public void testWriteTo() throws IOException {
        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            fileRecords.append(MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("foo".getBytes()),
                    new SimpleRecord("bar".getBytes())));
            fileRecords.flush();

            FileLogInputStream logInputStream = new FileLogInputStream(fileRecords.channel(), Integer.MAX_VALUE, 0,
                    fileRecords.sizeInBytes());

            FileLogInputStream.FileChannelRecordBatch batch = logInputStream.nextBatch();
            assertNotNull(batch);
            assertEquals(RecordBatch.MAGIC_VALUE_V2, batch.magic());

            ByteBuffer buffer = ByteBuffer.allocate(128);
            batch.writeTo(buffer);
            buffer.flip();

            MemoryRecords memRecords = MemoryRecords.readableRecords(buffer);
            List<Record> records = Utils.toList(memRecords.records().iterator());
            assertEquals(2, records.size());
            Record record0 = records.get(0);
            assertTrue(record0.hasMagic(RecordBatch.MAGIC_VALUE_V2));
            assertEquals("foo", Utils.utf8(record0.value(), record0.valueSize()));
            Record record1 = records.get(1);
            assertTrue(record1.hasMagic(RecordBatch.MAGIC_VALUE_V2));
            assertEquals("bar", Utils.utf8(record1.value(), record1.valueSize()));
        }
    }
}
