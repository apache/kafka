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

import org.apache.kafka.common.errors.StreamsMissingSourceTopicsException;
import org.apache.kafka.common.utils.LogContext;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
    private static final String REPARTITION_WITHOUT_PARTITION_COUNT = "repartitionWithoutPartitionCount";
    private static final Map<String, String> TOPIC_CONFIG1 = Collections.singletonMap("config1", "val1");
    private static final Map<String, String> TOPIC_CONFIG2 = Collections.singletonMap("config2", "val2");
    private static final Map<String, String> TOPIC_CONFIG5 = Collections.singletonMap("config5", "val5");
    private static final ConfiguredInternalTopic REPARTITION_TOPIC_CONFIG1 =
        new ConfiguredInternalTopic(REPARTITION_TOPIC_NAME1, TOPIC_CONFIG1, Optional.of(4), Optional.empty());
    private static final ConfiguredInternalTopic REPARTITION_TOPIC_CONFIG2 =
        new ConfiguredInternalTopic(REPARTITION_TOPIC_NAME2, TOPIC_CONFIG2, Optional.of(2), Optional.empty());
    private static final ConfiguredInternalTopic REPARTITION_TOPIC_CONFIG_WITHOUT_PARTITION_COUNT =
        new ConfiguredInternalTopic(REPARTITION_WITHOUT_PARTITION_COUNT, TOPIC_CONFIG5);
    private static final ConfiguredSubtopology TOPICS_INFO_WITHOUT_PARTITION_COUNT = new ConfiguredSubtopology(
        Collections.singleton(SINK_TOPIC_NAME2),
        Set.of(REPARTITION_TOPIC_NAME1, REPARTITION_WITHOUT_PARTITION_COUNT),
        mkMap(
            mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
            mkEntry(REPARTITION_WITHOUT_PARTITION_COUNT, REPARTITION_TOPIC_CONFIG_WITHOUT_PARTITION_COUNT)
        ),
        Collections.emptyMap()
    );
    private static final ConfiguredSubtopology TOPICS_INFO1 = new ConfiguredSubtopology(
        Collections.singleton(REPARTITION_TOPIC_NAME1),
        Set.of(SOURCE_TOPIC_NAME1, SOURCE_TOPIC_NAME2),
        mkMap(
            mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
            mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_CONFIG2)
        ),
        Collections.emptyMap()
    );
    private static final ConfiguredSubtopology TOPICS_INFO2 = new ConfiguredSubtopology(
        Collections.singleton(SINK_TOPIC_NAME1),
        Collections.singleton(REPARTITION_TOPIC_NAME1),
        mkMap(mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1)),
        Collections.emptyMap()
    );
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
        Map<String, ConfiguredSubtopology> subtopologyToTopicsInfo = mkMap(mkEntry("subtopology_0", TOPICS_INFO1), mkEntry("subtopology_1", TOPICS_INFO2));
        Function<String, Integer> topicPartitionCountProvider = s -> TOPICS.contains(s) ? 3 : null;
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologyToTopicsInfo,
            topicPartitionCountProvider
        );

        Map<String, ConfiguredInternalTopic> setup = repartitionTopics.setup();

        assertEquals(
            mkMap(
                mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
                mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_CONFIG2)
            ),
            setup
        );
    }

    @Test
    public void shouldThrowStreamsMissingSourceTopicsExceptionIfMissingSourceTopics() {
        Map<String, ConfiguredSubtopology> subtopologyToTopicsInfo = mkMap(mkEntry("subtopology_0", TOPICS_INFO1), mkEntry("subtopology_1", TOPICS_INFO2));
        Function<String, Integer> topicPartitionCountProvider = s -> Objects.equals(s, SOURCE_TOPIC_NAME1) ? null : (TOPICS.contains(s) ? 3 : null);
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologyToTopicsInfo,
            topicPartitionCountProvider
        );

        final StreamsMissingSourceTopicsException exception = assertThrows(StreamsMissingSourceTopicsException.class, repartitionTopics::setup);

        assertNotNull(exception);
        assertEquals("Missing source topics: source1", exception.getMessage());
    }

    @Test
    public void shouldThrowStreamsMissingSourceTopicsExceptionIfPartitionCountCannotBeComputedForAllRepartitionTopics() {
        Map<String, ConfiguredSubtopology> subtopologyToTopicsInfo = mkMap(
            mkEntry("subtopology_0", TOPICS_INFO1),
            mkEntry("subtopology_1", TOPICS_INFO_WITHOUT_PARTITION_COUNT)
        );
        Function<String, Integer> topicPartitionCountProvider = s -> TOPICS.contains(s) ? 3 : null;
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologyToTopicsInfo,
            topicPartitionCountProvider
        );

        StreamsMissingSourceTopicsException exception = assertThrows(StreamsMissingSourceTopicsException.class, repartitionTopics::setup);

        assertEquals(
            "Failed to compute number of partitions for all repartition topics, make sure all user input topics are created and all pattern subscriptions match at least one topic in the cluster",
            exception.getMessage()
        );
    }

    @Test
    public void shouldSetRepartitionTopicPartitionCountFromUpstreamExternalSourceTopic() {
        final ConfiguredSubtopology configuredSubtopology = new ConfiguredSubtopology(
            Set.of(REPARTITION_TOPIC_NAME1, REPARTITION_WITHOUT_PARTITION_COUNT),
            Set.of(SOURCE_TOPIC_NAME1, REPARTITION_TOPIC_NAME2),
            mkMap(
                mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
                mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_CONFIG2),
                mkEntry(REPARTITION_WITHOUT_PARTITION_COUNT, REPARTITION_TOPIC_CONFIG_WITHOUT_PARTITION_COUNT)
            ),
            Collections.emptyMap()
        );
        Map<String, ConfiguredSubtopology> subtopologyToTopicsInfo = mkMap(
            mkEntry("subtopology_0", configuredSubtopology),
            mkEntry("subtopology_1", TOPICS_INFO_WITHOUT_PARTITION_COUNT)
        );
        Function<String, Integer> topicPartitionCountProvider = s -> TOPICS.contains(s) ? 3 : null;
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologyToTopicsInfo,
            topicPartitionCountProvider
        );

        Map<String, ConfiguredInternalTopic> setup = repartitionTopics.setup();

        assertEquals(mkMap(
            mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
            mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_CONFIG2),
            mkEntry(REPARTITION_WITHOUT_PARTITION_COUNT, REPARTITION_TOPIC_CONFIG_WITHOUT_PARTITION_COUNT)
        ), setup);
    }

    @Test
    public void shouldSetRepartitionTopicPartitionCountFromUpstreamInternalRepartitionSourceTopic() {
        final ConfiguredSubtopology configuredSubtopology = new ConfiguredSubtopology(
            Set.of(REPARTITION_TOPIC_NAME2, REPARTITION_WITHOUT_PARTITION_COUNT),
            Set.of(SOURCE_TOPIC_NAME1, REPARTITION_TOPIC_NAME1),
            mkMap(
                mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
                mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_CONFIG2),
                mkEntry(REPARTITION_WITHOUT_PARTITION_COUNT, REPARTITION_TOPIC_CONFIG_WITHOUT_PARTITION_COUNT)
            ),
            Collections.emptyMap()
        );
        Map<String, ConfiguredSubtopology> subtopologyToTopicsInfo = mkMap(
            mkEntry("subtopology_0", configuredSubtopology),
            mkEntry("subtopology_1", TOPICS_INFO_WITHOUT_PARTITION_COUNT)
        );
        Function<String, Integer> topicPartitionCountProvider = s -> TOPICS.contains(s) ? 3 : null;
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologyToTopicsInfo,
            topicPartitionCountProvider
        );

        Map<String, ConfiguredInternalTopic> setup = repartitionTopics.setup();

        assertEquals(
            mkMap(
                mkEntry(REPARTITION_TOPIC_NAME1, REPARTITION_TOPIC_CONFIG1),
                mkEntry(REPARTITION_TOPIC_NAME2, REPARTITION_TOPIC_CONFIG2),
                mkEntry(REPARTITION_WITHOUT_PARTITION_COUNT, REPARTITION_TOPIC_CONFIG_WITHOUT_PARTITION_COUNT)
            ),
            setup
        );
    }

    @Test
    public void shouldNotSetupRepartitionTopicsWhenTopologyDoesNotContainAnyRepartitionTopics() {
        final ConfiguredSubtopology configuredSubtopology = new ConfiguredSubtopology(
            Collections.singleton(SINK_TOPIC_NAME1),
            Collections.singleton(SOURCE_TOPIC_NAME1),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        Map<String, ConfiguredSubtopology> subtopologyToTopicsInfo = mkMap(mkEntry("subtopology_0", configuredSubtopology));
        Function<String, Integer> topicPartitionCountProvider = s -> TOPICS.contains(s) ? 3 : null;
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            LOG_CONTEXT,
            subtopologyToTopicsInfo,
            topicPartitionCountProvider
        );

        Map<String, ConfiguredInternalTopic> setup = repartitionTopics.setup();

        assertEquals(Collections.emptyMap(), setup);
    }

}