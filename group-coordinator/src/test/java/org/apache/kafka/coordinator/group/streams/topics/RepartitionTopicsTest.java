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

import org.apache.kafka.common.errors.StreamsInvalidTopologyException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicInfo;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RepartitionTopicsTest {

    private static final LogContext LOG_CONTEXT = new LogContext();
    private static final String SOURCE_TOPIC_NAME1 = "source1";
    private static final String SOURCE_TOPIC_NAME2 = "source2";
    private static final TopicInfo REPARTITION_TOPIC1 = new TopicInfo().setName("repartition1").setPartitions(4);
    private static final TopicInfo REPARTITION_TOPIC2 = new TopicInfo().setName("repartition2").setPartitions(2);
    private static final TopicInfo REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT = new TopicInfo().setName("repartitionWithoutPartitionCount");

    private static OptionalInt sourceTopicPartitionCounts(final String topicName) {
        return SOURCE_TOPIC_NAME1.equals(topicName) || SOURCE_TOPIC_NAME2.equals(topicName) ? OptionalInt.of(3) : OptionalInt.empty();
    }

    @Test
    public void shouldSetupRepartitionTopics() {
        final Subtopology subtopology1 = new Subtopology()
            .setSubtopologyId("subtopology1")
            .setSourceTopics(List.of(SOURCE_TOPIC_NAME1, SOURCE_TOPIC_NAME2))
            .setRepartitionSinkTopics(List.of(REPARTITION_TOPIC1.name()));
        final Subtopology subtopology2 = new Subtopology()
            .setSubtopologyId("subtopology2")
            .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC1));
        final List<Subtopology> subtopologies = List.of(subtopology1, subtopology2);
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologies,
            RepartitionTopicsTest::sourceTopicPartitionCounts
        );

        final Map<String, Integer> setup = repartitionTopics.setup();

        assertEquals(
            Map.of(REPARTITION_TOPIC1.name(), REPARTITION_TOPIC1.partitions()),
            setup
        );
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfMissingSourceTopics() {
        final Subtopology subtopology1 = new Subtopology()
            .setSubtopologyId("subtopology1")
            .setSourceTopics(List.of(SOURCE_TOPIC_NAME1, SOURCE_TOPIC_NAME2))
            .setRepartitionSinkTopics(List.of(REPARTITION_TOPIC1.name()));
        final Subtopology subtopology2 = new Subtopology()
            .setSubtopologyId("subtopology2")
            .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC1));
        final Function<String, OptionalInt> topicPartitionCountProvider =
            s -> Objects.equals(s, SOURCE_TOPIC_NAME1) ? OptionalInt.empty() : sourceTopicPartitionCounts(s);
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            List.of(subtopology1, subtopology2),
            topicPartitionCountProvider
        );

        final IllegalStateException exception = assertThrows(IllegalStateException.class,
            repartitionTopics::setup);

        assertEquals("Missing source topics: source1", exception.getMessage());
    }

    @Test
    public void shouldThrowStreamsInvalidTopologyExceptionIfPartitionCountCannotBeComputedForAllRepartitionTopicsDueToLoops() {
        final Subtopology subtopology1 = new Subtopology()
            .setSubtopologyId("subtopology1")
            .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT))
            .setRepartitionSinkTopics(List.of(REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT.name()));
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            List.of(subtopology1),
            RepartitionTopicsTest::sourceTopicPartitionCounts
        );

        final StreamsInvalidTopologyException exception = assertThrows(StreamsInvalidTopologyException.class, repartitionTopics::setup);

        assertEquals(
            "Failed to compute number of partitions for all repartition topics. There may be loops in the topology that cannot be resolved.",
            exception.getMessage()
        );
    }

    @Test
    public void shouldThrowStreamsInvalidTopologyExceptionIfPartitionCountCannotBeComputedForAllRepartitionTopicsDueToMissingSinks() {
        final Subtopology subtopology1 = new Subtopology()
            .setSubtopologyId("subtopology1")
            .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT));
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            List.of(subtopology1),
            RepartitionTopicsTest::sourceTopicPartitionCounts
        );

        final StreamsInvalidTopologyException exception = assertThrows(StreamsInvalidTopologyException.class, repartitionTopics::setup);

        assertEquals(
            "Failed to compute number of partitions for all repartition topics, because a repartition source topic is never used as a sink topic.",
            exception.getMessage()
        );
    }

    @Test
    public void shouldSetRepartitionTopicPartitionCountFromUpstreamExternalSourceTopic() {
        final Subtopology subtopology = new Subtopology()
            .setSubtopologyId("subtopology0")
            .setSourceTopics(List.of(SOURCE_TOPIC_NAME1))
            .setRepartitionSinkTopics(List.of(REPARTITION_TOPIC1.name(), REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT.name()))
            .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC2));
        final Subtopology subtopologyWithoutPartitionCount = new Subtopology()
            .setSubtopologyId("subtopologyWithoutPartitionCount")
            .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC1, REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT));
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            List.of(subtopology, subtopologyWithoutPartitionCount),
            RepartitionTopicsTest::sourceTopicPartitionCounts
        );

        final Map<String, Integer> setup = repartitionTopics.setup();

        assertEquals(Map.of(
            REPARTITION_TOPIC1.name(), REPARTITION_TOPIC1.partitions(),
            REPARTITION_TOPIC2.name(), REPARTITION_TOPIC2.partitions(),
            REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT.name(), sourceTopicPartitionCounts(SOURCE_TOPIC_NAME1).getAsInt()
        ), setup);
    }

    @Test
    public void shouldSetRepartitionTopicPartitionCountFromUpstreamInternalRepartitionSourceTopic() {
        final Subtopology subtopology = new Subtopology()
            .setSubtopologyId("subtopology0")
            .setSourceTopics(List.of(SOURCE_TOPIC_NAME1))
            .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC1))
            .setRepartitionSinkTopics(List.of(REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT.name()));
        final Subtopology subtopologyWithoutPartitionCount = new Subtopology()
            .setSubtopologyId("subtopologyWithoutPartitionCount")
            .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT))
            .setRepartitionSinkTopics(List.of(REPARTITION_TOPIC1.name()));
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            List.of(subtopology, subtopologyWithoutPartitionCount),
            RepartitionTopicsTest::sourceTopicPartitionCounts
        );

        final Map<String, Integer> setup = repartitionTopics.setup();

        assertEquals(
            Map.of(
                REPARTITION_TOPIC1.name(), REPARTITION_TOPIC1.partitions(),
                REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT.name(), REPARTITION_TOPIC1.partitions()
            ),
            setup
        );
    }

    @Test
    public void shouldNotSetupRepartitionTopicsWhenTopologyDoesNotContainAnyRepartitionTopics() {
        final Subtopology subtopology = new Subtopology()
            .setSubtopologyId("subtopology0")
            .setSourceTopics(List.of(SOURCE_TOPIC_NAME1));
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            List.of(subtopology),
            RepartitionTopicsTest::sourceTopicPartitionCounts
        );

        final Map<String, Integer> setup = repartitionTopics.setup();

        assertEquals(Collections.emptyMap(), setup);
    }

}