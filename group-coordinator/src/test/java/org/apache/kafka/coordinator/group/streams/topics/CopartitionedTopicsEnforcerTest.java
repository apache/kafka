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
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CopartitionedTopicsEnforcerTest {

    private static final LogContext LOG_CONTEXT = new LogContext();

    private static Function<String, OptionalInt> topicPartitionProvider(Map<String, Integer> topicPartitionCounts) {
        return topic -> {
            Integer a = topicPartitionCounts.get(topic);
            return a == null ? OptionalInt.empty() : OptionalInt.of(a);
        };
    }

    @Test
    public void shouldThrowTopicConfigurationExceptionIfNoPartitionsFoundForCoPartitionedTopic() {
        final String topic = "topic";
        final Map<String, Integer> topicPartitionCounts = Collections.emptyMap();
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final TopicConfigurationException ex = assertThrows(TopicConfigurationException.class, () ->
            enforcer.enforce(
                Set.of(topic),
                Set.of(),
                Set.of()
            ));
        assertEquals(Status.MISSING_SOURCE_TOPICS, ex.status());
        assertEquals("Following topics are missing: [topic]", ex.getMessage());
    }

    @Test
    public void shouldThrowTopicConfigurationExceptionIfPartitionCountsForCoPartitionedTopicsDontMatch() {
        final String firstSourceTopic = "first";
        final String secondSourceTopic = "second";
        final Map<String, Integer> topicPartitionCounts = Map.of(firstSourceTopic, 2, secondSourceTopic, 1);
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final TopicConfigurationException ex = assertThrows(TopicConfigurationException.class, () ->
            enforcer.enforce(
                Set.of(firstSourceTopic, secondSourceTopic),
                Set.of(),
                Set.of()
            )
        );
        assertEquals(Status.INCORRECTLY_PARTITIONED_TOPICS, ex.status());
        assertEquals("Following topics do not have the same number of partitions: " +
            "[{first=2, second=1}]", ex.getMessage());
    }


    @Test
    public void shouldEnforceCopartitioningOnRepartitionTopics() {
        final String firstSourceTopic = "first";
        final String secondSourceTopic = "second";
        final String repartitionTopic = "repartitioned";
        final Map<String, Integer> topicPartitionCounts = Map.of(
            firstSourceTopic, 2,
            secondSourceTopic, 2,
            repartitionTopic, 10
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final Map<String, Integer> result =
            enforcer.enforce(
                Set.of(firstSourceTopic, secondSourceTopic, repartitionTopic),
                Set.of(),
                Set.of(repartitionTopic)
            );

        assertEquals(Map.of(repartitionTopic, 2), result);
    }


    @Test
    public void shouldSetNumPartitionsToMaximumPartitionsWhenAllTopicsAreRepartitionTopics() {
        final String repartitionTopic1 = "repartitionTopic1";
        final String repartitionTopic2 = "repartitionTopic2";
        final String repartitionTopic3 = "repartitionTopic3";
        final Map<String, Integer> topicPartitionCounts = Map.of(
            repartitionTopic1, 1,
            repartitionTopic2, 15,
            repartitionTopic3, 5
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final Map<String, Integer> result = enforcer.enforce(
            Set.of(repartitionTopic1, repartitionTopic2, repartitionTopic3),
            Set.of(),
            Set.of(repartitionTopic1, repartitionTopic2, repartitionTopic3)
        );

        assertEquals(Map.of(
            repartitionTopic1, 15,
            repartitionTopic2, 15,
            repartitionTopic3, 15
        ), result);
    }

    @Test
    public void shouldThrowAnExceptionIfTopicInfosWithEnforcedNumOfPartitionsHaveDifferentNumOfPartitions() {
        final String repartitionTopic1 = "repartitioned-1";
        final String repartitionTopic2 = "repartitioned-2";
        final Map<String, Integer> topicPartitionCounts = Map.of(
            repartitionTopic1, 10,
            repartitionTopic2, 5
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final TopicConfigurationException ex = assertThrows(
            TopicConfigurationException.class,
            () -> enforcer.enforce(
                Set.of(repartitionTopic1, repartitionTopic2),
                Set.of(repartitionTopic1, repartitionTopic2),
                Set.of()
            )
        );

        final TreeMap<String, Integer> sorted = new TreeMap<>(
            Utils.mkMap(Utils.mkEntry(repartitionTopic1, 10),
                Utils.mkEntry(repartitionTopic2, 5))
        );
        assertEquals(Status.INCORRECTLY_PARTITIONED_TOPICS, ex.status());
        assertEquals(String.format(
            "Following topics do not have the same number of partitions: " +
                "[%s]", sorted), ex.getMessage());
    }

    @Test
    public void shouldNotThrowAnExceptionWhenTopicInfosWithEnforcedNumOfPartitionsAreValid() {
        final String repartitionTopic1 = "repartitioned-1";
        final String repartitionTopic2 = "repartitioned-2";
        final Map<String, Integer> topicPartitionCounts = Map.of(
            repartitionTopic1, 10,
            repartitionTopic2, 10
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final Map<String, Integer> enforced = enforcer.enforce(
            Set.of(repartitionTopic1, repartitionTopic2),
            Set.of(),
            Set.of(repartitionTopic1, repartitionTopic2)
        );

        assertEquals(Map.of(
            repartitionTopic1, 10,
            repartitionTopic2, 10
        ), enforced);
    }

    @Test
    public void shouldThrowAnExceptionWhenNumberOfPartitionsOfNonRepartitionTopicAndRepartitionTopicWithEnforcedNumOfPartitionsDoNotMatch() {
        final String repartitionTopic1 = "repartitioned-1";
        final String firstSourceTopic = "first";
        final Map<String, Integer> topicPartitionCounts = Map.of(
            repartitionTopic1, 10,
            firstSourceTopic, 2
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final TopicConfigurationException ex = assertThrows(
            TopicConfigurationException.class,
            () -> enforcer.enforce(
                Set.of(repartitionTopic1, firstSourceTopic),
                Set.of(repartitionTopic1),
                Set.of())
        );

        assertEquals(Status.INCORRECTLY_PARTITIONED_TOPICS, ex.status());
        assertEquals(String.format("Number of partitions [%s] " +
                "of repartition topic [%s] " +
                "doesn't match number of partitions [%s] of the source topic.",
            10, repartitionTopic1, 2), ex.getMessage());
    }

    @Test
    public void shouldNotThrowAnExceptionWhenNumberOfPartitionsOfNonRepartitionTopicAndRepartitionTopicWithEnforcedNumOfPartitionsMatch() {
        final String repartitionTopic1 = "repartitioned-1";
        final String firstSourceTopic = "first";
        final Map<String, Integer> topicPartitionCounts = Map.of(
            repartitionTopic1, 2,
            firstSourceTopic, 2
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final Map<String, Integer> enforced = enforcer.enforce(
            Set.of(repartitionTopic1, firstSourceTopic),
            Set.of(),
            Set.of(repartitionTopic1)
        );

        assertEquals(Map.of(
            repartitionTopic1, 2
        ), enforced);
    }

    @Test
    public void shouldDeductNumberOfPartitionsFromRepartitionTopicWithEnforcedNumberOfPartitions() {
        final String repartitionTopic1 = "repartitioned-1";
        final String repartitionTopic2 = "repartitioned-2";
        final String repartitionTopic3 = "repartitioned-3";
        final Map<String, Integer> topicPartitionCounts = Map.of(
            repartitionTopic1, 2,
            repartitionTopic2, 5,
            repartitionTopic3, 2
        );
        final CopartitionedTopicsEnforcer enforcer =
            new CopartitionedTopicsEnforcer(LOG_CONTEXT, topicPartitionProvider(topicPartitionCounts));

        final Map<String, Integer> enforced = enforcer.enforce(
            Set.of(repartitionTopic1, repartitionTopic2, repartitionTopic3),
            Set.of(repartitionTopic1, repartitionTopic3),
            Set.of(repartitionTopic2)
        );

        assertEquals(Map.of(
            repartitionTopic1, 2,
            repartitionTopic2, 2,
            repartitionTopic3, 2
        ), enforced);
    }

}