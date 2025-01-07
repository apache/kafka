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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CopartitionedTopicsEnforcerTest {

    private static final LogContext LOG_CONTEXT = new LogContext();
    private static final String REPARTITION_TOPIC_1 = "repartitioned-1";
    private static final String REPARTITION_TOPIC_2 = "repartitioned-2";
    private static final String REPARTITION_TOPIC_3 = "repartitioned-3";
    private static final String SOURCE_TOPIC_1 = "source-1";
    private static final String SOURCE_TOPIC_2 = "source-2";

    private static Function<String, OptionalInt> topicPartitionProvider(Map<String, Integer> topicPartitionCounts) {
        return topic -> {
            Integer a = topicPartitionCounts.get(topic);
            return a == null ? OptionalInt.empty() : OptionalInt.of(a);
        };
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfNoPartitionsFoundForCoPartitionedTopic() {
        final Map<String, Integer> topicPartitionCounts = Collections.emptyMap();
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            enforcer.enforce(
                Set.of(SOURCE_TOPIC_1),
                Set.of(),
                Set.of()
            ));
        assertEquals(String.format("Number of partitions is not set for topic: %s", SOURCE_TOPIC_1), ex.getMessage());
    }

    @Test
    public void shouldThrowTopicConfigurationExceptionIfPartitionCountsForCoPartitionedTopicsDontMatch() {
        final Map<String, Integer> topicPartitionCounts = Map.of(SOURCE_TOPIC_1, 2, SOURCE_TOPIC_2, 1);
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final TopicConfigurationException ex = assertThrows(TopicConfigurationException.class, () ->
            enforcer.enforce(
                Set.of(SOURCE_TOPIC_1, SOURCE_TOPIC_2),
                Set.of(),
                Set.of()
            )
        );
        assertEquals(Status.INCORRECTLY_PARTITIONED_TOPICS, ex.status());
        assertEquals(String.format("Following topics do not have the same number of partitions: " +
            "[{%s=2, %s=1}]", SOURCE_TOPIC_1, SOURCE_TOPIC_2), ex.getMessage());
    }

    @Test
    public void shouldEnforceCopartitioningOnRepartitionTopics() {
        final Map<String, Integer> topicPartitionCounts = Map.of(
            SOURCE_TOPIC_1, 2,
            SOURCE_TOPIC_2, 2,
            REPARTITION_TOPIC_1, 10
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final Map<String, Integer> result =
            enforcer.enforce(
                Set.of(SOURCE_TOPIC_1, SOURCE_TOPIC_2, REPARTITION_TOPIC_1),
                Set.of(),
                Set.of(REPARTITION_TOPIC_1)
            );

        assertEquals(Map.of(REPARTITION_TOPIC_1, 2), result);
    }

    @Test
    public void shouldSetNumPartitionsToMaximumPartitionsWhenAllTopicsAreRepartitionTopics() {
        final Map<String, Integer> topicPartitionCounts = Map.of(
            REPARTITION_TOPIC_1, 1,
            REPARTITION_TOPIC_2, 15,
            REPARTITION_TOPIC_3, 5
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final Map<String, Integer> result = enforcer.enforce(
            Set.of(REPARTITION_TOPIC_1, REPARTITION_TOPIC_2, REPARTITION_TOPIC_3),
            Set.of(),
            Set.of(REPARTITION_TOPIC_1, REPARTITION_TOPIC_2, REPARTITION_TOPIC_3)
        );

        assertEquals(Map.of(
            REPARTITION_TOPIC_1, 15,
            REPARTITION_TOPIC_2, 15,
            REPARTITION_TOPIC_3, 15
        ), result);
    }

    @Test
    public void shouldThrowAnExceptionIfTopicInfosWithEnforcedNumOfPartitionsHaveDifferentNumOfPartitions() {
        final Map<String, Integer> topicPartitionCounts = Map.of(
            REPARTITION_TOPIC_1, 10,
            REPARTITION_TOPIC_2, 5
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final TopicConfigurationException ex = assertThrows(
            TopicConfigurationException.class,
            () -> enforcer.enforce(
                Set.of(REPARTITION_TOPIC_1, REPARTITION_TOPIC_2),
                Set.of(REPARTITION_TOPIC_1, REPARTITION_TOPIC_2),
                Set.of()
            )
        );

        final TreeMap<String, Integer> sorted = new TreeMap<>(
            Map.of(REPARTITION_TOPIC_1, 10, REPARTITION_TOPIC_2, 5)
        );
        assertEquals(Status.INCORRECTLY_PARTITIONED_TOPICS, ex.status());
        assertEquals(String.format(
            "Following topics do not have the same number of partitions: " +
                "[%s]", sorted), ex.getMessage());
    }

    @Test
    public void shouldReturnThePartitionCountsUnchangedWhenTopicInfosWithEnforcedNumOfPartitionsAreValid() {
        final Map<String, Integer> topicPartitionCounts = Map.of(
            REPARTITION_TOPIC_1, 10,
            REPARTITION_TOPIC_2, 10
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final Map<String, Integer> enforced = enforcer.enforce(
            Set.of(REPARTITION_TOPIC_1, REPARTITION_TOPIC_2),
            Set.of(),
            Set.of(REPARTITION_TOPIC_1, REPARTITION_TOPIC_2)
        );

        assertEquals(Map.of(
            REPARTITION_TOPIC_1, 10,
            REPARTITION_TOPIC_2, 10
        ), enforced);
    }

    @Test
    public void shouldThrowAnExceptionWhenNumberOfPartitionsOfNonRepartitionTopicAndRepartitionTopicWithEnforcedNumOfPartitionsDoNotMatch() {
        final Map<String, Integer> topicPartitionCounts = Map.of(
            REPARTITION_TOPIC_1, 10,
            SOURCE_TOPIC_1, 2
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final TopicConfigurationException ex = assertThrows(
            TopicConfigurationException.class,
            () -> enforcer.enforce(
                Set.of(REPARTITION_TOPIC_1, SOURCE_TOPIC_1),
                Set.of(REPARTITION_TOPIC_1),
                Set.of())
        );

        assertEquals(Status.INCORRECTLY_PARTITIONED_TOPICS, ex.status());
        assertEquals(String.format("Number of partitions [%s] " +
                "of repartition topic [%s] " +
                "doesn't match number of partitions [%s] of the source topic.",
            10, REPARTITION_TOPIC_1, 2), ex.getMessage());
    }

    @Test
    public void shouldReturnThePartitionCountsUnchangedWhenNumberOfPartitionsOfNonRepartitionTopicAndRepartitionTopicWithEnforcedNumOfPartitionsMatch() {
        final Map<String, Integer> topicPartitionCounts = Map.of(
            REPARTITION_TOPIC_1, 2,
            SOURCE_TOPIC_1, 2
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final Map<String, Integer> enforced = enforcer.enforce(
            Set.of(REPARTITION_TOPIC_1, SOURCE_TOPIC_1),
            Set.of(),
            Set.of(REPARTITION_TOPIC_1)
        );

        assertEquals(Map.of(
            REPARTITION_TOPIC_1, 2
        ), enforced);
    }

    @Test
    public void shouldDeductNumberOfPartitionsFromRepartitionTopicWithEnforcedNumberOfPartitions() {
        final Map<String, Integer> topicPartitionCounts = Map.of(
            REPARTITION_TOPIC_1, 2,
            REPARTITION_TOPIC_2, 5,
            REPARTITION_TOPIC_3, 2
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final Map<String, Integer> enforced = enforcer.enforce(
            Set.of(REPARTITION_TOPIC_1, REPARTITION_TOPIC_2, REPARTITION_TOPIC_3),
            Set.of(REPARTITION_TOPIC_1, REPARTITION_TOPIC_3),
            Set.of(REPARTITION_TOPIC_2)
        );

        assertEquals(Map.of(
            REPARTITION_TOPIC_1, 2,
            REPARTITION_TOPIC_2, 2,
            REPARTITION_TOPIC_3, 2
        ), enforced);
    }

}