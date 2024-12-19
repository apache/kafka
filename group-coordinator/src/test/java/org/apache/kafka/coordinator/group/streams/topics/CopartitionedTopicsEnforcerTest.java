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
package org.apache.kafka.coordinator.group.streams.topics;

import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse.Status;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CopartitionedTopicsEnforcerTest {

    private static final LogContext LOG_CONTEXT = new LogContext();

    private static OptionalInt emptyTopicPartitionProvider(String topic) {
        return OptionalInt.empty();
    }

    private static OptionalInt firstSecondTopicConsistent(String topic) {
        if (topic.equals("first") || topic.equals("second")) {
            return OptionalInt.of(2);
        }
        return OptionalInt.empty();
    }

    private static OptionalInt firstSecondTopicInconsistent(String topic) {
        if (topic.equals("first")) {
            return OptionalInt.of(2);
        }
        if (topic.equals("second")) {
            return OptionalInt.of(1);
        }
        return OptionalInt.empty();
    }

    @Test
    public void shouldThrowTopicConfigurationExceptionIfNoPartitionsFoundForCoPartitionedTopic() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::emptyTopicPartitionProvider);
        TopicConfigurationException ex =  assertThrows(TopicConfigurationException.class, () -> validator.enforce(Collections.singleton("topic"),
            Collections.emptyMap(), Collections.emptySet()));
        assertEquals(Status.MISSING_SOURCE_TOPICS, ex.status());
        assertEquals("Following topics are missing: [topic]", ex.getMessage());
    }

    @Test
    public void shouldThrowTopicConfigurationExceptionIfPartitionCountsForCoPartitionedTopicsDontMatch() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicInconsistent);
        TopicConfigurationException ex = assertThrows(TopicConfigurationException.class, () -> validator.enforce(Set.of("first", "second"),
            Collections.emptyMap(), Collections.emptySet()));
        assertEquals(Status.INCORRECTLY_PARTITIONED_TOPICS, ex.status());
        assertEquals("Following topics do not have the same number of partitions: " +
            "[{first=2, second=1}]", ex.getMessage());
    }


    @Test
    public void shouldEnforceCopartitioningOnRepartitionTopics() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicConsistent);
        final String repartitionTopic = "repartitioned";

        Map<String, Integer> result =
            validator.enforce(Set.of("first", "second", repartitionTopic),
                Map.of(
                    repartitionTopic, 10
                ),
                Collections.emptySet()
            );

        assertEquals(Map.of(repartitionTopic, 2), result);
    }


    @Test
    public void shouldSetNumPartitionsToMaximumPartitionsWhenAllTopicsAreRepartitionTopics() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::emptyTopicPartitionProvider);
        final String one = "one";
        final String two = "two";
        final String three = "three";

        Map<String, Integer> result = validator.enforce(Set.of(
                one,
                two,
                three
            ),
            Map.of(
                one, 1,
                two, 15,
                three, 5
            ),
            Collections.emptySet()
        );

        assertEquals(Map.of(
            one, 15,
            two, 15,
            three, 15
        ), result);
    }

    @Test
    public void shouldThrowAnExceptionIfTopicInfosWithEnforcedNumOfPartitionsHaveDifferentNumOfPartitions() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicConsistent);
        final String topic1 = "repartitioned-1";
        final String topic2 = "repartitioned-2";

        final TopicConfigurationException ex = assertThrows(
            TopicConfigurationException.class,
            () -> validator.enforce(Set.of(topic1, topic2),
                Utils.mkMap(
                    Utils.mkEntry(topic1, 10),
                    Utils.mkEntry(topic2, 5)
                ),
                Set.of(topic1, topic2)
            )
        );

        final TreeMap<String, Integer> sorted = new TreeMap<>(
            Utils.mkMap(Utils.mkEntry(topic1, 10),
                Utils.mkEntry(topic2, 5))
        );
        assertEquals(Status.INCORRECTLY_PARTITIONED_TOPICS, ex.status());
        assertEquals(String.format(
            "Following topics do not have the same number of partitions: " +
                "[%s]", sorted), ex.getMessage());
    }

    @Test
    public void shouldNotThrowAnExceptionWhenTopicInfosWithEnforcedNumOfPartitionsAreValid() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicConsistent);
        final String topic1 = "repartitioned-1";
        final String topic2 = "repartitioned-2";

        final Map<String, Integer> enforced = validator.enforce(Set.of(topic1, topic2),
            Utils.mkMap(
                Utils.mkEntry(topic1, 10),
                Utils.mkEntry(topic2, 10)
            ),
            Set.of(topic1, topic2)
        );

        assertEquals(Map.of(
            topic1, 10,
            topic2, 10
        ), enforced);
    }

    @Test
    public void shouldThrowAnExceptionWhenNumberOfPartitionsOfNonRepartitionTopicAndRepartitionTopicWithEnforcedNumOfPartitionsDoNotMatch() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicConsistent);
        final String topic1 = "repartitioned-1";

        final TopicConfigurationException ex = assertThrows(
            TopicConfigurationException.class,
            () -> validator.enforce(Set.of(topic1, "second"),
                Utils.mkMap(Utils.mkEntry(topic1, 10)),
                Set.of(topic1))
        );

        assertEquals(Status.INCORRECTLY_PARTITIONED_TOPICS, ex.status());
        assertEquals(String.format("Number of partitions [%s] " +
                "of repartition topic [%s] " +
                "doesn't match number of partitions [%s] of the source topic.",
            10, topic1, 2), ex.getMessage());
    }

    @Test
    public void shouldNotThrowAnExceptionWhenNumberOfPartitionsOfNonRepartitionTopicAndRepartitionTopicWithEnforcedNumOfPartitionsMatch() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicConsistent);
        final String topic1 = "repartitioned-1";

        final Map<String, Integer> enforced = validator.enforce(Set.of(topic1, "second"),
            Utils.mkMap(Utils.mkEntry(topic1, 2)),
            Set.of(topic1));

        assertEquals(Map.of(
            topic1, 2
        ), enforced);
    }

    @Test
    public void shouldDeductNumberOfPartitionsFromRepartitionTopicWithEnforcedNumberOfPartitions() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicConsistent);
        final String topic1 = "repartitioned-1";
        final String topic2 = "repartitioned-2";
        final String topic3 = "repartitioned-3";

        final Map<String, Integer> enforced = validator.enforce(Set.of(topic1, topic2),
            Utils.mkMap(
                Utils.mkEntry(topic1, 2),
                Utils.mkEntry(topic2, 5),
                Utils.mkEntry(topic3, 2)
            ),
            Set.of(
                topic1, topic3
            )
        );

        assertEquals(Map.of(
            topic1, 2,
            topic2, 2,
            topic3, 2
        ), enforced);
    }

}