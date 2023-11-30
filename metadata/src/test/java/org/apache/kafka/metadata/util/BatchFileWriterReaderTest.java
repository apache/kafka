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

package org.apache.kafka.metadata.util;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.metadata.util.BatchFileReader.BatchAndType;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final public class BatchFileWriterReaderTest {
    @Test
    public void testHeaderFooter() throws Exception {
        File tempFile = TestUtils.tempFile();
        Path tempPath = tempFile.toPath();
        // Delete the file because BatchFileWriter doesn't expect the file to exist
        tempFile.delete();

        try (BatchFileWriter writer = BatchFileWriter.open(tempPath)) {
            ApiMessageAndVersion message = new ApiMessageAndVersion(
                new TopicRecord()
                    .setName("bar")
                    .setTopicId(Uuid.fromString("cxBT72dK4si8Ied1iP4wBA")),
                (short) 0
            );

            writer.append(message);
        }

        try (BatchFileReader reader = new BatchFileReader.Builder()
                .setPath(tempPath.toString()).build()) {
            // Check the SnapshotHeaderRecord
            long currentOffset = 0;
            assertTrue(reader.hasNext());
            BatchAndType batchAndType = reader.next();
            assertTrue(batchAndType.isControl());
            Batch<ApiMessageAndVersion> batch = batchAndType.batch();
            assertEquals(currentOffset, batch.baseOffset());
            assertEquals(currentOffset, batch.lastOffset());
            List<ApiMessageAndVersion> records = batch.records();
            assertEquals(1, records.size());
            ApiMessageAndVersion apiMessageAndVersion = records.get(0);
            assertEquals(0, apiMessageAndVersion.version());

            SnapshotHeaderRecord headerRecord = (SnapshotHeaderRecord) apiMessageAndVersion.message();
            assertEquals(0, headerRecord.version());
            assertEquals(0, headerRecord.lastContainedLogTimestamp());

            // Check the TopicRecord
            currentOffset++;
            assertTrue(reader.hasNext());
            batchAndType = reader.next();
            assertFalse(batchAndType.isControl());
            batch = batchAndType.batch();
            assertEquals(currentOffset, batch.baseOffset());
            assertEquals(currentOffset, batch.lastOffset());
            records = batch.records();
            assertEquals(1, records.size());
            apiMessageAndVersion = records.get(0);
            assertEquals(0, apiMessageAndVersion.version());

            TopicRecord topicRecord = (TopicRecord) apiMessageAndVersion.message();
            assertEquals("bar", topicRecord.name());
            assertEquals(Uuid.fromString("cxBT72dK4si8Ied1iP4wBA"), topicRecord.topicId());

            // Check the SnapshotFooterRecord
            currentOffset++;
            assertTrue(reader.hasNext());
            batchAndType = reader.next();
            assertTrue(batchAndType.isControl());
            batch = batchAndType.batch();
            assertEquals(currentOffset, batch.baseOffset());
            assertEquals(currentOffset, batch.lastOffset());
            records = batch.records();
            assertEquals(1, records.size());
            apiMessageAndVersion = records.get(0);
            assertEquals(0, apiMessageAndVersion.version());

            SnapshotFooterRecord footerRecord = (SnapshotFooterRecord) apiMessageAndVersion.message();
            assertEquals(0, footerRecord.version());
        }
    }
}
