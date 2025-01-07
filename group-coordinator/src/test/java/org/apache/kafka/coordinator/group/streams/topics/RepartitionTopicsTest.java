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
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicConfig;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicInfo;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RepartitionTopicsTest {

    private static final LogContext LOG_CONTEXT = new LogContext();
    private static final String SOURCE_TOPIC_NAME1 = "source1";
    private static final String SOURCE_TOPIC_NAME2 = "source2";
    private static final String SINK_TOPIC_NAME1 = "sink1";
    private static final String SINK_TOPIC_NAME2 = "sink2";
    private static final String REPARTITION_TOPIC_NAME1 = "repartition1";
    private static final String REPARTITION_TOPIC_NAME2 = "repartition2";
    private static final String REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT = "repartitionWithoutPartitionCount";
    private static final TopicConfig TOPIC_CONFIG1 = new TopicConfig().setKey("config1").setValue("val1");
    private static final TopicConfig TOPIC_CONFIG2 = new TopicConfig().setKey("config2").setValue("val2");
    private static final TopicConfig TOPIC_CONFIG5 = new TopicConfig().setKey("config5").setValue("val5");
    private static final TopicInfo REPARTITION_TOPIC_INFO1 = new TopicInfo()
        .setName(REPARTITION_TOPIC_NAME1)
        .setPartitions(4)
        .setTopicConfigs(List.of(TOPIC_CONFIG1));
    private static final Subtopology SUBTOPOLOGY2 = new Subtopology()
        .setSubtopologyId("SUBTOPOLOGY2")
        .setSourceTopics(Collections.singletonList(REPARTITION_TOPIC_NAME1))
        .setRepartitionSinkTopics(Collections.singletonList(SINK_TOPIC_NAME1))
        .setRepartitionSourceTopics(List.of(REPARTITION_TOPIC_INFO1))
        .setStateChangelogTopics(Collections.emptyList());
    private static final TopicInfo REPARTITION_TOPIC_INFO2 = new TopicInfo()
        .setName(REPARTITION_TOPIC_NAME2)
        .setPartitions(2)
        .setTopicConfigs(List.of(TOPIC_CONFIG2));
    private static final Subtopology SUBTOPOLOGY1 = new Subtopology()
        .setSubtopologyId("SUBTOPOLOGY1")
        .setSourceTopics(List.of(SOURCE_TOPIC_NAME1, SOURCE_TOPIC_NAME2))
        .setRepartitionSinkTopics(Collections.singletonList(REPARTITION_TOPIC_NAME1))
        .setRepartitionSourceTopics(List.of(
            REPARTITION_TOPIC_INFO1,
            REPARTITION_TOPIC_INFO2
        ))
        .setStateChangelogTopics(Collections.emptyList());
    private static final TopicInfo REPARTITION_TOPIC_INFO_WITHOUT_PARTITION_COUNT = new TopicInfo()
        .setName(REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT)
        .setTopicConfigs(List.of(TOPIC_CONFIG5));
    private static final Subtopology SUBTOPOLOGY_WITHOUT_PARTITION_COUNT = new Subtopology()
        .setSubtopologyId("SUBTOPOLOGY_WITHOUT_PARTITION_COUNT")
        .setSourceTopics(List.of(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT))
        .setRepartitionSinkTopics(Collections.singletonList(SINK_TOPIC_NAME2))
        .setRepartitionSourceTopics(List.of(
            REPARTITION_TOPIC_INFO1,
            REPARTITION_TOPIC_INFO_WITHOUT_PARTITION_COUNT
        ))
        .setStateChangelogTopics(Collections.emptyList());
    private static final Set<String> TOPICS = Set.of(
        SOURCE_TOPIC_NAME1,
        SOURCE_TOPIC_NAME2,
        SINK_TOPIC_NAME1,
        SINK_TOPIC_NAME2,
        REPARTITION_TOPIC_NAME1,
        REPARTITION_TOPIC_NAME2
    );

    @Test
    public void shouldSetupRepartitionTopics() {
        List<Subtopology> subtopologyToSubtopology = List.of(SUBTOPOLOGY1, SUBTOPOLOGY2);
        Function<String, OptionalInt> topicPartitionCountProvider = s -> TOPICS.contains(s) ? OptionalInt.of(3) : OptionalInt.empty();
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologyToSubtopology,
            topicPartitionCountProvider
        );

        Map<String, Integer> setup = repartitionTopics.setup();

        assertEquals(
            mkMap(
                mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_INFO1.partitions()),
                mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_INFO2.partitions())
            ),
            setup
        );
    }

    @Test
    public void shouldThrowStreamsMissingSourceTopicsExceptionIfMissingSourceTopics() {
        List<Subtopology> subtopologyToSubtopology = List.of(SUBTOPOLOGY1, SUBTOPOLOGY2);
        Function<String, OptionalInt> topicPartitionCountProvider =
            s -> Objects.equals(s, SOURCE_TOPIC_NAME1) ? OptionalInt.empty() :
                (TOPICS.contains(s) ? OptionalInt.of(3) : OptionalInt.empty());
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologyToSubtopology,
            topicPartitionCountProvider
        );

        final TopicConfigurationException exception = assertThrows(TopicConfigurationException.class,
            repartitionTopics::setup);

        assertNotNull(exception);
        assertEquals(Status.MISSING_SOURCE_TOPICS, exception.status());
        assertEquals("Missing source topics: source1", exception.getMessage());
    }

    @Test
    public void shouldThrowStreamsMissingSourceTopicsExceptionIfPartitionCountCannotBeComputedForAllRepartitionTopics() {
        List<Subtopology> subtopologyToSubtopology = List.of(SUBTOPOLOGY1, SUBTOPOLOGY_WITHOUT_PARTITION_COUNT);
        Function<String, OptionalInt> topicPartitionCountProvider = s -> TOPICS.contains(s) ? OptionalInt.of(3) : OptionalInt.empty();
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologyToSubtopology,
            topicPartitionCountProvider
        );

        TopicConfigurationException exception = assertThrows(TopicConfigurationException.class, repartitionTopics::setup);

        assertEquals(Status.MISSING_SOURCE_TOPICS, exception.status());
        assertEquals(
            "Failed to compute number of partitions for all repartition topics, make sure all user input topics are created "
                + "and all pattern subscriptions match at least one topic in the cluster",
            exception.getMessage()
        );
    }

    @Test
    public void shouldSetRepartitionTopicPartitionCountFromUpstreamExternalSourceTopic() {
        final Subtopology subtopology = new Subtopology()
            .setSubtopologyId("SUBTOPOLOGY0")
            .setSourceTopics(List.of(SOURCE_TOPIC_NAME1, REPARTITION_TOPIC_NAME2))
            .setRepartitionSinkTopics(List.of(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT))
            .setRepartitionSourceTopics(List.of(
                REPARTITION_TOPIC_INFO1,
                REPARTITION_TOPIC_INFO2,
                REPARTITION_TOPIC_INFO_WITHOUT_PARTITION_COUNT
            ))
            .setStateChangelogTopics(Collections.emptyList());
        List<Subtopology> subtopologyToSubtopology = List.of(
            subtopology,
            SUBTOPOLOGY_WITHOUT_PARTITION_COUNT
        );
        Function<String, OptionalInt> topicPartitionCountProvider = s -> TOPICS.contains(s) ? OptionalInt.of(3) : OptionalInt.empty();
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologyToSubtopology,
            topicPartitionCountProvider
        );

        Map<String, Integer> setup = repartitionTopics.setup();

        assertEquals(mkMap(
            mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_INFO1.partitions()),
            mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_INFO2.partitions()),
            mkEntry(REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT, 3)
        ), setup);
    }

    @Test
    public void shouldSetRepartitionTopicPartitionCountFromUpstreamInternalRepartitionSourceTopic() {
        final Subtopology subtopology = new Subtopology()
            .setSubtopologyId("SUBTOPOLOGY0")
            .setSourceTopics(List.of(SOURCE_TOPIC_NAME1, REPARTITION_TOPIC_NAME1))
            .setRepartitionSinkTopics(List.of(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT))
            .setRepartitionSourceTopics(List.of(
                REPARTITION_TOPIC_INFO1,
                REPARTITION_TOPIC_INFO2,
                REPARTITION_TOPIC_INFO_WITHOUT_PARTITION_COUNT
            ))
            .setStateChangelogTopics(Collections.emptyList());
        List<Subtopology> subtopologyToSubtopology = List.of(subtopology, SUBTOPOLOGY_WITHOUT_PARTITION_COUNT);
        Function<String, OptionalInt> topicPartitionCountProvider = s -> TOPICS.contains(s) ? OptionalInt.of(3) : OptionalInt.empty();
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologyToSubtopology,
            topicPartitionCountProvider
        );

        Map<String, Integer> setup = repartitionTopics.setup();

        assertEquals(
            mkMap(
                mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_INFO1.partitions()),
                mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_INFO2.partitions()),
                mkEntry(REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT, 4)
            ),
            setup
        );
    }

    @Test
    public void shouldSetRepartitionTopicPartitionCountFromUpstreamSourceTopicMultipleSubtopologies() {
        final Subtopology subtopology0 = new Subtopology()
            .setSubtopologyId("SUBTOPOLOGY0")
            .setSourceTopics(List.of(SOURCE_TOPIC_NAME1))
            .setRepartitionSinkTopics(List.of(REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT));
        final Subtopology subtopology1 = new Subtopology()
            .setSubtopologyId("SUBTOPOLOGY1")
            .setRepartitionSourceTopics(List.of(
                REPARTITION_TOPIC_INFO_WITHOUT_PARTITION_COUNT
            ));
        List<Subtopology> subtopologyToSubtopology = List.of(subtopology0, subtopology1);
        Function<String, OptionalInt> topicPartitionCountProvider = s -> Objects.equals(s, SOURCE_TOPIC_NAME1) ? OptionalInt.of(3) : OptionalInt.empty();
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologyToSubtopology,
            topicPartitionCountProvider
        );

        Map<String, Integer> setup = repartitionTopics.setup();

        assertEquals(
            mkMap(
                mkEntry(REPARTITION_TOPIC_WITHOUT_PARTITION_COUNT, 3)
            ),
            setup
        );
    }

    @Test
    public void shouldNotSetupRepartitionTopicsWhenTopologyDoesNotContainAnyRepartitionTopics() {
        final Subtopology subtopology = new Subtopology()
            .setSubtopologyId("SUBTOPOLOGY0")
            .setSourceTopics(Collections.singletonList(SOURCE_TOPIC_NAME1))
            .setRepartitionSinkTopics(Collections.singletonList(SINK_TOPIC_NAME1))
            .setRepartitionSourceTopics(Collections.emptyList())
            .setStateChangelogTopics(Collections.emptyList());
        List<Subtopology> subtopologyToSubtopology = List.of(subtopology);
        Function<String, OptionalInt> topicPartitionCountProvider = s -> TOPICS.contains(s) ? OptionalInt.of(3) : OptionalInt.empty();
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologyToSubtopology,
            topicPartitionCountProvider
        );

        Map<String, Integer> setup = repartitionTopics.setup();

        assertEquals(Collections.emptyMap(), setup);
    }

}