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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionStore;
import org.junit.Test;

import java.time.Duration;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@SuppressWarnings("deprecation")
public class SuppressTopologyTest {
    private static final Serde<String> STRING_SERDE = Serdes.String();

    private static final String NAMED_FINAL_TOPOLOGY = "Topologies:\n" +
        "   Sub-topology: 0\n" +
        "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
        "      --> KSTREAM-KEY-SELECT-0000000001\n" +
        "    Processor: KSTREAM-KEY-SELECT-0000000001 (stores: [])\n" +
        "      --> counts-repartition-filter\n" +
        "      <-- KSTREAM-SOURCE-0000000000\n" +
        "    Processor: counts-repartition-filter (stores: [])\n" +
        "      --> counts-repartition-sink\n" +
        "      <-- KSTREAM-KEY-SELECT-0000000001\n" +
        "    Sink: counts-repartition-sink (topic: counts-repartition)\n" +
        "      <-- counts-repartition-filter\n" +
        "\n" +
        "  Sub-topology: 1\n" +
        "    Source: counts-repartition-source (topics: [counts-repartition])\n" +
        "      --> KSTREAM-AGGREGATE-0000000002\n" +
        "    Processor: KSTREAM-AGGREGATE-0000000002 (stores: [counts])\n" +
        "      --> myname\n" +
        "      <-- counts-repartition-source\n" +
        "    Processor: myname (stores: [myname-store])\n" +
        "      --> KTABLE-TOSTREAM-0000000006\n" +
        "      <-- KSTREAM-AGGREGATE-0000000002\n" +
        "    Processor: KTABLE-TOSTREAM-0000000006 (stores: [])\n" +
        "      --> KSTREAM-MAP-0000000007\n" +
        "      <-- myname\n" +
        "    Processor: KSTREAM-MAP-0000000007 (stores: [])\n" +
        "      --> KSTREAM-SINK-0000000008\n" +
        "      <-- KTABLE-TOSTREAM-0000000006\n" +
        "    Sink: KSTREAM-SINK-0000000008 (topic: output-suppressed)\n" +
        "      <-- KSTREAM-MAP-0000000007\n" +
        "\n";

    private static final String ANONYMOUS_FINAL_TOPOLOGY = "Topologies:\n" +
        "   Sub-topology: 0\n" +
        "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
        "      --> KSTREAM-KEY-SELECT-0000000001\n" +
        "    Processor: KSTREAM-KEY-SELECT-0000000001 (stores: [])\n" +
        "      --> counts-repartition-filter\n" +
        "      <-- KSTREAM-SOURCE-0000000000\n" +
        "    Processor: counts-repartition-filter (stores: [])\n" +
        "      --> counts-repartition-sink\n" +
        "      <-- KSTREAM-KEY-SELECT-0000000001\n" +
        "    Sink: counts-repartition-sink (topic: counts-repartition)\n" +
        "      <-- counts-repartition-filter\n" +
        "\n" +
        "  Sub-topology: 1\n" +
        "    Source: counts-repartition-source (topics: [counts-repartition])\n" +
        "      --> KSTREAM-AGGREGATE-0000000002\n" +
        "    Processor: KSTREAM-AGGREGATE-0000000002 (stores: [counts])\n" +
        "      --> KTABLE-SUPPRESS-0000000006\n" +
        "      <-- counts-repartition-source\n" +
        "    Processor: KTABLE-SUPPRESS-0000000006 (stores: [KTABLE-SUPPRESS-STATE-STORE-0000000007])\n" +
        "      --> KTABLE-TOSTREAM-0000000008\n" +
        "      <-- KSTREAM-AGGREGATE-0000000002\n" +
        "    Processor: KTABLE-TOSTREAM-0000000008 (stores: [])\n" +
        "      --> KSTREAM-MAP-0000000009\n" +
        "      <-- KTABLE-SUPPRESS-0000000006\n" +
        "    Processor: KSTREAM-MAP-0000000009 (stores: [])\n" +
        "      --> KSTREAM-SINK-0000000010\n" +
        "      <-- KTABLE-TOSTREAM-0000000008\n" +
        "    Sink: KSTREAM-SINK-0000000010 (topic: output-suppressed)\n" +
        "      <-- KSTREAM-MAP-0000000009\n" +
        "\n";

    private static final String NAMED_INTERMEDIATE_TOPOLOGY = "Topologies:\n" +
        "   Sub-topology: 0\n" +
        "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
        "      --> KSTREAM-AGGREGATE-0000000002\n" +
        "    Processor: KSTREAM-AGGREGATE-0000000002 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000001])\n" +
        "      --> asdf\n" +
        "      <-- KSTREAM-SOURCE-0000000000\n" +
        "    Processor: asdf (stores: [asdf-store])\n" +
        "      --> KTABLE-TOSTREAM-0000000003\n" +
        "      <-- KSTREAM-AGGREGATE-0000000002\n" +
        "    Processor: KTABLE-TOSTREAM-0000000003 (stores: [])\n" +
        "      --> KSTREAM-SINK-0000000004\n" +
        "      <-- asdf\n" +
        "    Sink: KSTREAM-SINK-0000000004 (topic: output)\n" +
        "      <-- KTABLE-TOSTREAM-0000000003\n" +
        "\n";

    private static final String ANONYMOUS_INTERMEDIATE_TOPOLOGY = "Topologies:\n" +
        "   Sub-topology: 0\n" +
        "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
        "      --> KSTREAM-AGGREGATE-0000000002\n" +
        "    Processor: KSTREAM-AGGREGATE-0000000002 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000001])\n" +
        "      --> KTABLE-SUPPRESS-0000000003\n" +
        "      <-- KSTREAM-SOURCE-0000000000\n" +
        "    Processor: KTABLE-SUPPRESS-0000000003 (stores: [KTABLE-SUPPRESS-STATE-STORE-0000000004])\n" +
        "      --> KTABLE-TOSTREAM-0000000005\n" +
        "      <-- KSTREAM-AGGREGATE-0000000002\n" +
        "    Processor: KTABLE-TOSTREAM-0000000005 (stores: [])\n" +
        "      --> KSTREAM-SINK-0000000006\n" +
        "      <-- KTABLE-SUPPRESS-0000000003\n" +
        "    Sink: KSTREAM-SINK-0000000006 (topic: output)\n" +
        "      <-- KTABLE-TOSTREAM-0000000005\n" +
        "\n";


    @Test
    public void shouldUseNumberingForAnonymousFinalSuppressionNode() {
        final StreamsBuilder anonymousNodeBuilder = new StreamsBuilder();
        anonymousNodeBuilder
            .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupBy((String k, String v) -> k, Grouped.with(STRING_SERDE, STRING_SERDE))
            .windowedBy(SessionWindows.with(ofMillis(5L)).grace(ofMillis(5L)))
            .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("counts").withCachingDisabled())
            .suppress(untilWindowCloses(unbounded()))
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
        final String anonymousNodeTopology = anonymousNodeBuilder.build().describe().toString();

        // without the name, the suppression node increments the topology index
        assertThat(anonymousNodeTopology, is(ANONYMOUS_FINAL_TOPOLOGY));
    }

    @Test
    public void shouldApplyNameToFinalSuppressionNode() {
        final StreamsBuilder namedNodeBuilder = new StreamsBuilder();
        namedNodeBuilder
            .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupBy((String k, String v) -> k, Grouped.with(STRING_SERDE, STRING_SERDE))
            .windowedBy(SessionWindows.with(ofMillis(5L)).grace(ofMillis(5L)))
            .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("counts").withCachingDisabled())
            .suppress(untilWindowCloses(unbounded()).withName("myname"))
            .toStream()
            .map((final Windowed<String> k, final Long v) -> new KeyValue<>(k.toString(), v))
            .to("output-suppressed", Produced.with(STRING_SERDE, Serdes.Long()));
        final String namedNodeTopology = namedNodeBuilder.build().describe().toString();

        // without the name, the suppression node does not increment the topology index
        assertThat(namedNodeTopology, is(NAMED_FINAL_TOPOLOGY));
    }

    @Test
    public void shouldUseNumberingForAnonymousSuppressionNode() {
        final StreamsBuilder anonymousNodeBuilder = new StreamsBuilder();
        anonymousNodeBuilder
            .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupByKey()
            .count()
            .suppress(untilTimeLimit(Duration.ofSeconds(1), unbounded()))
            .toStream()
            .to("output", Produced.with(STRING_SERDE, Serdes.Long()));
        final String anonymousNodeTopology = anonymousNodeBuilder.build().describe().toString();

        // without the name, the suppression node increments the topology index
        assertThat(anonymousNodeTopology, is(ANONYMOUS_INTERMEDIATE_TOPOLOGY));
    }

    @Test
    public void shouldApplyNameToSuppressionNode() {
        final StreamsBuilder namedNodeBuilder = new StreamsBuilder();
        namedNodeBuilder
            .stream("input", Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupByKey()
            .count()
            .suppress(untilTimeLimit(Duration.ofSeconds(1), unbounded()).withName("asdf"))
            .toStream()
            .to("output", Produced.with(STRING_SERDE, Serdes.Long()));
        final String namedNodeTopology = namedNodeBuilder.build().describe().toString();

        // without the name, the suppression node does not increment the topology index
        assertThat(namedNodeTopology, is(NAMED_INTERMEDIATE_TOPOLOGY));
    }
}
