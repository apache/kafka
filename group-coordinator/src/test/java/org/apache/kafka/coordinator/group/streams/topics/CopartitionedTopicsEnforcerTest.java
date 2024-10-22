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

import org.apache.kafka.common.errors.StreamsInconsistentInternalTopicsException;
import org.apache.kafka.common.errors.StreamsInvalidTopologyException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class CopartitionedTopicsEnforcerTest {

    private static final LogContext LOG_CONTEXT = new LogContext();

    private static Integer emptyTopicPartitionProvider(String topic) {
        return null;
    }

    private static Integer firstSecondTopicConsistent(String topic) {
        if (topic.equals("first") || topic.equals("second")) {
            return 2;
        }
        return null;
    }

    private static Integer firstSecondTopicInconsistent(String topic) {
        if (topic.equals("first")) {
            return 2;
        }
        if (topic.equals("second")) {
            return 1;
        }
        return null;
    }

    @Test
    public void shouldThrowStreamsInconsistentInternalTopicsExceptionIfNoPartitionsFoundForCoPartitionedTopic() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::emptyTopicPartitionProvider);
        assertThrows(StreamsInvalidTopologyException.class, () -> validator.enforce(Collections.singleton("topic"),
            Collections.emptyMap()));
    }

    @Test
    public void shouldThrowStreamsInconsistentInternalTopicsExceptionIfPartitionCountsForCoPartitionedTopicsDontMatch() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicInconsistent);
        assertThrows(StreamsInconsistentInternalTopicsException.class, () -> validator.enforce(Set.of("first", "second"),
            Collections.emptyMap()));
    }


    @Test
    public void shouldEnforceCopartitioningOnRepartitionTopics() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicConsistent);
        final ConfiguredInternalTopic config = createTopicConfig("repartitioned", 10);

        validator.enforce(Set.of("first", "second", config.name()),
            Collections.singletonMap(config.name(), config));

        assertEquals(Optional.of(2), config.numberOfPartitions());
    }


    @Test
    public void shouldSetNumPartitionsToMaximumPartitionsWhenAllTopicsAreRepartitionTopics() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::emptyTopicPartitionProvider);
        final ConfiguredInternalTopic one = createTopicConfig("one", 1);
        final ConfiguredInternalTopic two = createTopicConfig("two", 15);
        final ConfiguredInternalTopic three = createTopicConfig("three", 5);
        final Map<String, ConfiguredInternalTopic> configuredInternalTopics = new HashMap<>();

        configuredInternalTopics.put(one.name(), one);
        configuredInternalTopics.put(two.name(), two);
        configuredInternalTopics.put(three.name(), three);

        validator.enforce(Set.of(
                one.name(),
                two.name(),
                three.name()
            ),
            configuredInternalTopics
        );

        assertEquals(Optional.of(15), one.numberOfPartitions());
        assertEquals(Optional.of(15), two.numberOfPartitions());
        assertEquals(Optional.of(15), three.numberOfPartitions());
    }

    @Test
    public void shouldThrowAnExceptionIfConfiguredInternalTopicsWithEnforcedNumOfPartitionsHaveDifferentNumOfPartitions() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicConsistent);
        final ConfiguredInternalTopic topic1 = createConfiguredInternalTopicWithEnforcedNumberOfPartitions("repartitioned-1", 10);
        final ConfiguredInternalTopic topic2 = createConfiguredInternalTopicWithEnforcedNumberOfPartitions("repartitioned-2", 5);

        final StreamsInconsistentInternalTopicsException ex = assertThrows(
            StreamsInconsistentInternalTopicsException.class,
            () -> validator.enforce(Set.of(topic1.name(), topic2.name()),
                Utils.mkMap(
                    Utils.mkEntry(topic1.name(), topic1),
                    Utils.mkEntry(topic2.name(), topic2)
                )
            )
        );

        final TreeMap<String, Integer> sorted = new TreeMap<>(
            Utils.mkMap(Utils.mkEntry(topic1.name(), topic1.numberOfPartitions().get()),
                Utils.mkEntry(topic2.name(), topic2.numberOfPartitions().get()))
        );

        assertEquals(String.format(
            "Following topics do not have the same number of partitions: " +
                "[%s]", sorted), ex.getMessage());
    }

    @Test
    public void shouldNotThrowAnExceptionWhenConfiguredInternalTopicsWithEnforcedNumOfPartitionsAreValid() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicConsistent);
        final ConfiguredInternalTopic topic1 = createConfiguredInternalTopicWithEnforcedNumberOfPartitions("repartitioned-1", 10);
        final ConfiguredInternalTopic topic2 = createConfiguredInternalTopicWithEnforcedNumberOfPartitions("repartitioned-2", 10);

        validator.enforce(Set.of(topic1.name(), topic2.name()),
            Utils.mkMap(
                Utils.mkEntry(topic1.name(), topic1),
                Utils.mkEntry(topic2.name(), topic2)
            )
        );

        assertEquals(Optional.of(10), topic1.numberOfPartitions());
        assertEquals(Optional.of(10), topic2.numberOfPartitions());
    }

    @Test
    public void shouldThrowAnExceptionWhenNumberOfPartitionsOfNonRepartitionTopicAndRepartitionTopicWithEnforcedNumOfPartitionsDoNotMatch() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicConsistent);
        final ConfiguredInternalTopic topic1 = createConfiguredInternalTopicWithEnforcedNumberOfPartitions("repartitioned-1", 10);

        final StreamsInconsistentInternalTopicsException ex = assertThrows(
            StreamsInconsistentInternalTopicsException.class,
            () -> validator.enforce(Set.of(topic1.name(), "second"),
                Utils.mkMap(Utils.mkEntry(topic1.name(), topic1)))
        );

        assertEquals(String.format("Number of partitions [%s] " +
                "of repartition topic [%s] " +
                "doesn't match number of partitions [%s] of the source topic.",
            topic1.numberOfPartitions().get(), topic1.name(), 2), ex.getMessage());
    }

    @Test
    public void shouldNotThrowAnExceptionWhenNumberOfPartitionsOfNonRepartitionTopicAndRepartitionTopicWithEnforcedNumOfPartitionsMatch() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicConsistent);
        final ConfiguredInternalTopic topic1 = createConfiguredInternalTopicWithEnforcedNumberOfPartitions("repartitioned-1", 2);

        validator.enforce(Set.of(topic1.name(), "second"),
            Utils.mkMap(Utils.mkEntry(topic1.name(), topic1)));

        assertEquals(Optional.of(2), topic1.numberOfPartitions());
    }

    @Test
    public void shouldDeductNumberOfPartitionsFromRepartitionTopicWithEnforcedNumberOfPartitions() {
        final CopartitionedTopicsEnforcer validator = new CopartitionedTopicsEnforcer(LOG_CONTEXT,
            CopartitionedTopicsEnforcerTest::firstSecondTopicConsistent);
        final ConfiguredInternalTopic topic1 = createConfiguredInternalTopicWithEnforcedNumberOfPartitions("repartitioned-1", 2);
        final ConfiguredInternalTopic topic2 = createTopicConfig("repartitioned-2", 5);
        final ConfiguredInternalTopic topic3 = createConfiguredInternalTopicWithEnforcedNumberOfPartitions("repartitioned-3", 2);

        validator.enforce(Set.of(topic1.name(), topic2.name()),
            Utils.mkMap(
                Utils.mkEntry(topic1.name(), topic1),
                Utils.mkEntry(topic2.name(), topic2),
                Utils.mkEntry(topic3.name(), topic3)
            )
        );

        assertEquals(topic1.numberOfPartitions(), topic2.numberOfPartitions());
        assertEquals(topic2.numberOfPartitions(), topic3.numberOfPartitions());
    }

    private ConfiguredInternalTopic createTopicConfig(final String repartitionTopic,
                                                      final int partitions) {
        final ConfiguredInternalTopic config =
            new ConfiguredInternalTopic(repartitionTopic, Collections.emptyMap());

        config.setNumberOfPartitions(partitions);
        return config;
    }

    private ConfiguredInternalTopic createConfiguredInternalTopicWithEnforcedNumberOfPartitions(final String repartitionTopic,
                                                                                                final int partitions) {
        return new ConfiguredInternalTopic(repartitionTopic,
            Collections.emptyMap(),
            Optional.of(partitions),
            Optional.empty());
    }

}