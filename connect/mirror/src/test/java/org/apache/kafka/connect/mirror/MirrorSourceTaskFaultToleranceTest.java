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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Simulates a real OffsetOutOfRangeException during poll() and checks the right exception (or none) comes out. */
class MirrorSourceTaskFaultToleranceTest {

    private static final String TOPIC = "commit-log";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    @Test
    void sameTopicIdThrowsDataLossException() {
        Uuid sameId = Uuid.randomUuid();
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer();
        Admin admin = mockAdminReturning(sameId);

        MirrorSourceTask task = new MirrorSourceTask(consumer, null, "primary", null, null, admin);
        task.seedKnownTopicId(TOPIC, sameId);
        scheduleOffsetOutOfRange(consumer);

        assertThrows(DataLossException.class, task::poll);
    }

    @Test
    void differentTopicIdInFailFastModeThrowsTopicResetException() {
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer();
        Admin admin = mockAdminReturning(Uuid.randomUuid());

        MirrorSourceTask task = new MirrorSourceTask(consumer, null, "primary", null, null, admin);
        task.seedKnownTopicId(TOPIC, Uuid.randomUuid()); // different from the mocked current ID
        task.setResetBehaviorForTesting("fail-fast");
        scheduleOffsetOutOfRange(consumer);

        assertThrows(TopicResetException.class, task::poll);
    }

    @Test
    void differentTopicIdInSelfHealModeDoesNotThrow() {
        MockConsumer<byte[], byte[]> consumer = newAssignedConsumer();
        Admin admin = mockAdminReturning(Uuid.randomUuid());

        MirrorSourceTask task = new MirrorSourceTask(consumer, null, "primary", null, null, admin);
        task.seedKnownTopicId(TOPIC, Uuid.randomUuid());
        task.setResetBehaviorForTesting("self-heal");
        scheduleOffsetOutOfRange(consumer);

        assertDoesNotThrow(task::poll);
    }

    private static MockConsumer<byte[], byte[]> newAssignedConsumer() {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("none");
        consumer.assign(Collections.singletonList(TP));
        consumer.updateBeginningOffsets(Collections.singletonMap(TP, 0L));
        return consumer;
    }

    private static void scheduleOffsetOutOfRange(MockConsumer<byte[], byte[]> consumer) {
        Map<TopicPartition, Long> outOfRange = Collections.singletonMap(TP, 100L);
        consumer.schedulePollTask(() -> {
            throw new OffsetOutOfRangeException(outOfRange);
        });
    }

    private static Admin mockAdminReturning(Uuid currentTopicId) {
        Admin admin = mock(Admin.class);
        TopicDescription description = mock(TopicDescription.class);
        when(description.topicId()).thenReturn(currentTopicId);
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        when(result.topicNameValues()).thenReturn(
                Collections.singletonMap(TOPIC, KafkaFuture.completedFuture(description)));
        when(admin.describeTopics(anyCollection())).thenReturn(result);
        return admin;
    }
}