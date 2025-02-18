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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KStreamSkipRepartitionTest {
    @Test
    void shouldAllowAggregationWithSkipRepartition() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<Object, Long> aggregatedTable = builder.stream("input-topic")
            .selectKey((key, value) -> key)
            .skipRepartition()
            .groupByKey()
            .count();

        assertNotNull(aggregatedTable, "Aggregation should still work correctly when using skipRepartition().");
    }

    @Test
    void shouldContainSinkNodeWhenNotUsingSkipRepartition() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic").selectKey((key, value) -> key).groupByKey().count();

        final TopologyDescription description = builder.build().describe();
        final boolean hasSinkTopic = description.subtopologies().stream()
            .flatMap(subtopology -> subtopology.nodes().stream())
            .anyMatch(node -> node.name().contains("KSTREAM-SINK-"));

        assertTrue(hasSinkTopic, "Topology should contain a sink node when skipRepartition() is not used.");
    }

    @Test
    void shouldNotAllowNullNamedOnSkipRepartition() {
        final KStream<String, String> stream = new StreamsBuilder().stream("input-topic");
        final NullPointerException exception =
            assertThrows(NullPointerException.class, () -> stream.skipRepartition(null));
        assertThat(exception.getMessage(), equalTo("named cannot be null"));
    }

    @Test
    void shouldNotContainSinkNodeWhenNotUsingSkipRepartition() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic").selectKey((key, value) -> key).skipRepartition().groupByKey().count();

        final TopologyDescription description = builder.build().describe();
        final boolean hasSinkTopic = description.subtopologies().stream()
            .flatMap(subtopology -> subtopology.nodes().stream())
            .anyMatch(node -> node.name().contains("KSTREAM-SINK-"));

        assertFalse(hasSinkTopic, "Topology should not contain a sink node when skipRepartition() is not used.");
    }

    @Test
    void shouldNotCreateMultipleSubtopologiesEvenWithMultipleSkipRepartitionCalls() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic")
            .selectKey((key, value) -> key)
            .skipRepartition()
            .skipRepartition()
            .groupByKey();

        final TopologyDescription description = builder.build().describe();
        assertEquals(1, description.subtopologies().size(),
            "Topology should remain in a single subtopology even when skipRepartition() is applied multiple times.");
    }
}
