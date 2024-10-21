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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class UtilsTest {
    @Test
    public void testHashSubscriptionMetadata() {
        Uuid topic1 = Uuid.randomUuid();

        TopicMetadata topicMetadata1WithAscendingOrder = new TopicMetadata(
            topic1,
            "topic1",
            1,
            Map.of(
                0, Set.of("rack00", "rack01", "rack02"),
                1, Set.of("rack10", "rack11", "rack12"),
                2, Set.of("rack20", "rack21", "rack22")));
        TopicMetadata topicMetadata1WithDescendingOrder = new TopicMetadata(
            topic1,
            "topic1",
            1,
            Map.of(
                2, Set.of("rack22", "rack21", "rack20"),
                1, Set.of("rack12", "rack11", "rack10"),
                0, Set.of("rack02", "rack01", "rack00")));

        // test different rack order
        assertEquals(
            Utils.hashSubscriptionMetadata(Map.of("topic1", topicMetadata1WithAscendingOrder)),
            Utils.hashSubscriptionMetadata(Map.of("topic1", topicMetadata1WithDescendingOrder)));

        // test different topic order
        Uuid topic2 = Uuid.randomUuid();
        TopicMetadata topicMetadata2 = new TopicMetadata(topic2, "topic2", 1);
        assertEquals(
            Utils.hashSubscriptionMetadata(Map.of("topic1", topicMetadata1WithAscendingOrder, "topic2", topicMetadata2)),
            Utils.hashSubscriptionMetadata(Map.of("topic2", topicMetadata2, "topic1", topicMetadata1WithDescendingOrder)));

        // test different topic
        assertNotEquals(
            Utils.hashSubscriptionMetadata(Map.of("topic1", topicMetadata1WithAscendingOrder)),
            Utils.hashSubscriptionMetadata(Map.of("topic2", topicMetadata2)));

        // test different uuid
        assertNotEquals(
            Utils.hashSubscriptionMetadata(Map.of("topic2", topicMetadata2)),
            Utils.hashSubscriptionMetadata(Map.of("topic2", new TopicMetadata(Uuid.randomUuid(), "topic2", 1))));

        // test different topic name
        assertNotEquals(
            Utils.hashSubscriptionMetadata(Map.of("topic2", topicMetadata2)),
            Utils.hashSubscriptionMetadata(Map.of("topic2", new TopicMetadata(topic2, "topic2foo", 1))));

        // test different topic number numPartitions
        assertNotEquals(
            Utils.hashSubscriptionMetadata(Map.of("topic2", topicMetadata2)),
            Utils.hashSubscriptionMetadata(Map.of("topic2", new TopicMetadata(topic2, "topic2", 2))));

        // test different topic number partitionRacks
        assertNotEquals(
            Utils.hashSubscriptionMetadata(Map.of("topic2", topicMetadata2)),
            Utils.hashSubscriptionMetadata(Map.of("topic2", new TopicMetadata(topic2, "topic2", 1, Map.of(0, Set.of("rack0"))))));
    }
}
