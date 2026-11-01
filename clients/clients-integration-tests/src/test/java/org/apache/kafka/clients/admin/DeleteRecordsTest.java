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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.ClientsTestUtils.sendRecords;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ClusterTestDefaults(types = {Type.KRAFT})
public class DeleteRecordsTest {
    private static final String TOPIC = "topic";
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);
    private static final int NUM_RECORDS = 10;

    @ClusterTest
    public void testDeleteRecordsWithOffsetLargerThanHighWatermark(ClusterInstance cluster) throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1);
        sendRecords(cluster, TOPIC_PARTITION, NUM_RECORDS);
        try (Admin admin = cluster.admin()) {
            assertFutureThrows(OffsetOutOfRangeException.class,
                deleteRecords(admin, NUM_RECORDS + 1));
            assertEquals(0L, earliestOffset(admin));
        }
    }

    @ClusterTest
    public void testDeleteRecordsWithOffsetSmallerThanMinusOne(ClusterInstance cluster) throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1);
        sendRecords(cluster, TOPIC_PARTITION, NUM_RECORDS);
        try (Admin admin = cluster.admin()) {
            assertFutureThrows(OffsetOutOfRangeException.class,
                deleteRecords(admin, -2L));
            assertEquals(0L, earliestOffset(admin));
        }
    }

    @ClusterTest
    public void testDeleteRecordsToHighWatermark(ClusterInstance cluster) throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1);
        sendRecords(cluster, TOPIC_PARTITION, NUM_RECORDS);
        try (Admin admin = cluster.admin()) {
            assertEquals(NUM_RECORDS, deleteRecords(admin, -1L).get().lowWatermark());
        }
    }

    private static KafkaFuture<DeletedRecords> deleteRecords(Admin admin, long offset) {
        return admin.deleteRecords(Map.of(TOPIC_PARTITION, RecordsToDelete.beforeOffset(offset)))
            .lowWatermarks().get(TOPIC_PARTITION);
    }

    private static long earliestOffset(Admin admin) throws ExecutionException, InterruptedException {
        return admin.listOffsets(Map.of(TOPIC_PARTITION, OffsetSpec.earliest()))
            .partitionResult(TOPIC_PARTITION).get().offset();
    }
}
