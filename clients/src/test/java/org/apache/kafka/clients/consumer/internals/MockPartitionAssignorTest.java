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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MockPartitionAssignorTest {
    private final static String TOPIC = "topic";
    private TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    private TopicPartition tp1 = new TopicPartition(TOPIC, 1);

    @Test
    public void testConsumersClaimingOverlappedPartitions() {
        MockPartitionAssignor assignor = new MockPartitionAssignor(Collections.emptyList());
        Map<String, List<TopicPartition>> result = new HashMap<>();
        assignor.prepare(result);

        Map<String, ConsumerPartitionAssignor.Subscription> consumerSubscriptions = new HashMap<>();
        consumerSubscriptions.put("consumer1", new ConsumerPartitionAssignor.Subscription(Collections.singletonList(TOPIC), null, Collections.singletonList(tp0), 1));
        consumerSubscriptions.put("consumer2", new ConsumerPartitionAssignor.Subscription(Collections.singletonList(TOPIC), null, Collections.singletonList(tp1), 1));
        // no overlapped partitions claimed by consumers
        assertDoesNotThrow(() -> assignor.validateSubscription(consumerSubscriptions));

        consumerSubscriptions.put("consumer3", new ConsumerPartitionAssignor.Subscription(Collections.singletonList(TOPIC), null, Collections.singletonList(tp0), 1));
        // overlapped partition tp0 is claimed by consumer1 and consumer3
        assertThrows(IllegalStateException.class, () -> assignor.validateSubscription(consumerSubscriptions));
    }
}
