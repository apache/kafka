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
package org.apache.kafka.test;

import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics;
import org.apache.kafka.common.requests.OffsetFetchRequest;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MoreAssertions {

    public static void assertRequestEquals(OffsetFetchRequest expectedRequest, OffsetFetchRequest actualRequest) {
        if (expectedRequest.version() > 9 || actualRequest.version() > 9) {
            throw new AssertionError("A new version of OffsetFetchRequest has been detected. Please " +
                "review the equality contract enforced here and add/remove fields accordingly.");
        }

        OffsetFetchRequestData expected = expectedRequest.data();
        OffsetFetchRequestData actual = actualRequest.data();

        assertEquals(expected.groupId(), actual.groupId());
        assertEquals(expected.requireStable(), actual.requireStable());

        BiConsumer<OffsetFetchRequestTopic, OffsetFetchRequestTopic> topicEvaluator = (t1, t2) -> {
            assertEquals(t1.name(), t2.name());
            assertEquals(new HashSet<>(t1.partitionIndexes()), new HashSet<>(t2.partitionIndexes()));
        };

        BiConsumer<OffsetFetchRequestTopics, OffsetFetchRequestTopics> topicsEvaluator = (t1, t2) -> {
            assertEquals(t1.name(), t2.name());
            assertEquals(t1.topicId(), t2.topicId());
            assertEquals(new HashSet<>(t1.partitionIndexes()), new HashSet<>(t2.partitionIndexes()));
        };

        BiConsumer<OffsetFetchRequestGroup, OffsetFetchRequestGroup> groupEvaluator = (g1, g2) -> {
            assertEquals(g1.groupId(), g2.groupId());
            evaluate(g1.topics(), g2.topics(), g -> new TopicNameAndId(g.name(), g.topicId()), topicsEvaluator);
        };

        evaluate(expected.groups(), actual.groups(), OffsetFetchRequestGroup::groupId, groupEvaluator);
        evaluate(expected.topics(), actual.topics(), OffsetFetchRequestTopic::name, topicEvaluator);
    }

    public static <T, K> void evaluate(
            Collection<T> expected,
            Collection<T> actual,
            Function<T, K> classifier,
            BiConsumer<T, T> evaluator) {

        if (expected == null || actual == null) {
            assertTrue(expected == null && actual == null);
        }

        Map<K, T> lhs = expected.stream().collect(toMap(classifier, identity()));
        Map<K, T> rhs = actual.stream().collect(toMap(classifier, identity()));

        lhs.forEach((k, v) -> {
            T w = rhs.get(k);
            assertNotNull(w);
            evaluator.accept(v, w);
        });
    }
}
