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
package org.apache.kafka.server.share.fetch;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.test.TestUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Helper functions for writing share fetch unit tests.
 */
public class ShareFetchTestUtils {

    /**
     * Create an ordered map of TopicIdPartition to partition max bytes.
     *
     * @param partitionMaxBytes The maximum number of bytes that can be fetched for each partition.
     * @param topicIdPartitions The topic partitions to create the map for.
     * @return The ordered map of TopicIdPartition to partition max bytes.
     */
    public static LinkedHashMap<TopicIdPartition, Integer> orderedMap(int partitionMaxBytes, TopicIdPartition... topicIdPartitions) {
        LinkedHashMap<TopicIdPartition, Integer> map = new LinkedHashMap<>();
        for (TopicIdPartition tp : topicIdPartitions) {
            map.put(tp, partitionMaxBytes);
        }
        return map;
    }

    /**
     * Validate that the rotated map is equal to the original map with the keys rotated by the given position.
     *
     * @param original The original map.
     * @param result The rotated map.
     * @param rotationAt The position to rotate the keys at.
     */
    public static void validateRotatedMapEquals(
        LinkedHashMap<TopicIdPartition, Integer> original,
        LinkedHashMap<TopicIdPartition, Integer> result,
        int rotationAt
    ) {
        Set<TopicIdPartition> originalKeys = original.keySet();
        Set<TopicIdPartition> resultKeys = result.keySet();

        TopicIdPartition[] originalKeysArray = new TopicIdPartition[originalKeys.size()];
        int i = 0;
        for (TopicIdPartition key : originalKeys) {
            if (i < rotationAt) {
                originalKeysArray[originalKeys.size() - rotationAt + i] = key;
            } else {
                originalKeysArray[i - rotationAt] = key;
            }
            i++;
        }
        assertArrayEquals(originalKeysArray, resultKeys.toArray());
        for (TopicIdPartition key : originalKeys) {
            assertEquals(original.get(key), result.get(key));
        }
    }

    /**
     * Create a file records with the given offset values, the number of records from each given start
     * offset.
     *
     * @param recordsPerOffset The offset values and the number of records to create from given offset.
     * @return The file records.
     * @throws IOException If the file records cannot be created.
     */
    public static FileRecords createFileRecords(Map<Long, Integer> recordsPerOffset) throws IOException {
        FileRecords fileRecords = FileRecords.open(tempFile());
        for (Entry<Long, Integer> entry : recordsPerOffset.entrySet()) {
            try (MemoryRecordsBuilder records = memoryRecordsBuilder(entry.getValue(), entry.getKey())) {
                fileRecords.append(records.build());
            }
        }
        return fileRecords;
    }

    /**
     * Create a memory records builder with the given number of records and start offset.
     *
     * @param numOfRecords The number of records to create.
     * @param startOffset The start offset of the records.
     * @return The memory records builder.
     */
    public static MemoryRecordsBuilder memoryRecordsBuilder(int numOfRecords, long startOffset) {
        return memoryRecordsBuilder(ByteBuffer.allocate(1024), numOfRecords, startOffset);
    }

    /**
     * Create a memory records builder with the number of records and start offset, in the given buffer.
     *
     * @param buffer The buffer to write the records to.
     * @param numOfRecords The number of records to create.
     * @param startOffset The start offset of the records.
     * @return The memory records builder.
     */
    public static MemoryRecordsBuilder memoryRecordsBuilder(ByteBuffer buffer, int numOfRecords, long startOffset) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Compression.NONE,
            TimestampType.CREATE_TIME, startOffset, 2);
        for (int i = 0; i < numOfRecords; i++) {
            builder.appendWithOffset(startOffset + i, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        }
        return builder;
    }

    /**
     * Create a share acquired records from the given acquired records.
     *
     * @param acquiredRecords The acquired records to create the share acquired records from.
     * @return The share acquired records.
     */
    public static ShareAcquiredRecords createShareAcquiredRecords(AcquiredRecords acquiredRecords) {
        return new ShareAcquiredRecords(
            List.of(acquiredRecords), (int) (acquiredRecords.lastOffset() - acquiredRecords.firstOffset() + 1)
        );
    }
}
