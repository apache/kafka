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
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KStreamSkipRepartitionTest {
    @Test
    void shouldNotAllowNullNamedOnSkipRepartition() {
        final KStream<String, String> stream = new StreamsBuilder().stream("source");
        final NullPointerException exception =
            assertThrows(NullPointerException.class, () -> stream.skipRepartition(null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    void shouldNotContainRepartitionNodeWhenSkipRepartitionUsed() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input-topic")
            .selectKey((key, value) -> key)
            .skipRepartition()
            .groupByKey()
            .count()
            .toStream();
        final TopologyDescription description = builder.build().describe();
        final boolean hasRepartitionTopic = description.subtopologies().stream()
            .flatMap(subtopology -> subtopology.nodes().stream())
            .anyMatch(node -> node.name().contains("repartition"));

        assertFalse(hasRepartitionTopic, "Topology should not contain a repartition node when using skipRepartition()");
    }
}
