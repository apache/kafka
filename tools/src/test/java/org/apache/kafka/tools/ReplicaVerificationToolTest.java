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
package org.apache.kafka.tools;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReplicaVerificationToolTest {
    @Test
    void testReplicaBufferVerifyChecksum() {
        StringBuilder sb = new StringBuilder();
        final Map<TopicPartition, Integer> expectedReplicasPerTopicAndPartition = new HashMap<TopicPartition, Integer>() {{
                put(new TopicPartition("a", 0), 3);
                put(new TopicPartition("a", 1), 3);
                put(new TopicPartition("b", 0), 2);
            }};

        ReplicaVerificationTool.ReplicaBuffer replicaBuffer =
            new ReplicaVerificationTool.ReplicaBuffer(expectedReplicasPerTopicAndPartition, Collections.emptyMap(), 2, 0);
        expectedReplicasPerTopicAndPartition.forEach((tp, numReplicas) -> {
            IntStream.range(0, numReplicas).forEach(replicaId -> {
                SimpleRecord[] records = IntStream.rangeClosed(0, 5)
                    .mapToObj(index -> new SimpleRecord(("key " + index).getBytes(), ("value " + index).getBytes()))
                    .toArray(SimpleRecord[]::new);

                long initialOffset = 4L;
                MemoryRecords memoryRecords = MemoryRecords.withRecords(initialOffset, CompressionType.NONE, records);
                FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData()
                    .setPartitionIndex(tp.partition())
                    .setHighWatermark(20)
                    .setLastStableOffset(20)
                    .setLogStartOffset(0)
                    .setRecords(memoryRecords);

                replicaBuffer.addFetchedData(tp, replicaId, partitionData);
            });
        });

        replicaBuffer.verifyCheckSum(line -> sb.append(format("%s%n", line)));
        String output = sb.toString().trim();

        // if you change this assertion, you should verify that the replica_verification_test.py system test still passes
        assertTrue(output.endsWith(": max lag is 10 for partition a-1 at offset 10 among 3 partitions"),
            format("Max lag information should be in output: %s", output));
    }
}
