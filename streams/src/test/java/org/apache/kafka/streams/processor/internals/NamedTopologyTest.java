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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyStreamsBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static java.util.Arrays.asList;

public class NamedTopologyTest {
    final KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();
    final Properties props = configProps();

    final NamedTopologyStreamsBuilder builder1 = new NamedTopologyStreamsBuilder("topology-1");
    final NamedTopologyStreamsBuilder builder2 = new NamedTopologyStreamsBuilder("topology-2");
    final NamedTopologyStreamsBuilder builder3 = new NamedTopologyStreamsBuilder("topology-3");

    KafkaStreamsNamedTopologyWrapper streams;

    @Before
    public void setup() {
        builder1.stream("input-1");
        builder2.stream("input-2");
        builder3.stream("input-3");
    }

    @After
    public void cleanup() {
        if (streams != null) {
            streams.close();
        }
    }

    private static Properties configProps() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Named-Topology-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2018");
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        return props;
    }

    @Test
    public void shouldThrowIllegalArgumentOnIllegalName() {
        assertThrows(IllegalArgumentException.class, () -> new NamedTopologyStreamsBuilder("__not-allowed__"));
    }

    @Test
    public void shouldBuildSingleNamedTopology() {
        builder1.stream("stream-1").filter((k, v) -> !k.equals(v)).to("output-1");

        streams = new KafkaStreamsNamedTopologyWrapper(builder1.buildNamedTopology(props), props, clientSupplier);
    }

    @Test
    public void shouldBuildMultipleIdenticalNamedTopologyWithRepartition() {
        builder1.stream("stream-1").selectKey((k, v) -> v).groupByKey().count().toStream().to("output-1");
        builder2.stream("stream-2").selectKey((k, v) -> v).groupByKey().count().toStream().to("output-2");
        builder3.stream("stream-3").selectKey((k, v) -> v).groupByKey().count().toStream().to("output-3");

        streams = new KafkaStreamsNamedTopologyWrapper(
            asList(
                builder1.buildNamedTopology(props),
                builder2.buildNamedTopology(props),
                builder3.buildNamedTopology(props)),
            props,
            clientSupplier
        );
    }

    @Test
    public void shouldReturnTopologyByName() {
        final NamedTopology topology1 = builder1.buildNamedTopology(props);
        final NamedTopology topology2 = builder2.buildNamedTopology(props);
        final NamedTopology topology3 = builder3.buildNamedTopology(props);
        streams = new KafkaStreamsNamedTopologyWrapper(asList(topology1, topology2, topology3), props, clientSupplier);
        assertThat(streams.getTopologyByName("topology-1").get(), equalTo(topology1));
        assertThat(streams.getTopologyByName("topology-2").get(), equalTo(topology2));
        assertThat(streams.getTopologyByName("topology-3").get(), equalTo(topology3));
    }

    @Test
    public void shouldReturnEmptyWhenLookingUpNonExistentTopologyByName() {
        streams = new KafkaStreamsNamedTopologyWrapper(builder1.buildNamedTopology(props), props, clientSupplier);
        assertThat(streams.getTopologyByName("non-existent-topology").isPresent(), equalTo(false));
    }

    @Test
    public void shouldAllowSameStoreNameToBeUsedByMultipleNamedTopologies() {
        builder1.stream("stream-1").selectKey((k, v) -> v).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store")));
        builder2.stream("stream-2").selectKey((k, v) -> v).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store")));

        streams = new KafkaStreamsNamedTopologyWrapper(asList(
                    builder1.buildNamedTopology(props),
                    builder2.buildNamedTopology(props)),
                props,
                clientSupplier
        );
    }

    @Test
    public void shouldThrowTopologyExceptionWhenMultipleNamedTopologiesCreateStreamFromSameInputTopic() {
        builder1.stream("stream");
        builder2.stream("stream");

        assertThrows(
            TopologyException.class,
            () -> streams = new KafkaStreamsNamedTopologyWrapper(
                asList(
                    builder1.buildNamedTopology(props),
                    builder2.buildNamedTopology(props)),
                props,
                clientSupplier)
        );
    }

    @Test
    public void shouldThrowTopologyExceptionWhenMultipleNamedTopologiesCreateTableFromSameInputTopic() {
        builder1.table("table");
        builder2.table("table");

        assertThrows(
            TopologyException.class,
            () -> streams = new KafkaStreamsNamedTopologyWrapper(
                asList(
                    builder1.buildNamedTopology(props),
                    builder2.buildNamedTopology(props)),
                props,
                clientSupplier)
        );
    }

    @Test
    public void shouldThrowTopologyExceptionWhenMultipleNamedTopologiesCreateStreamAndTableFromSameInputTopic() {
        builder1.stream("input");
        builder2.table("input");

        assertThrows(
            TopologyException.class,
            () -> streams = new KafkaStreamsNamedTopologyWrapper(
                asList(
                    builder1.buildNamedTopology(props),
                    builder2.buildNamedTopology(props)),
                props,
                clientSupplier)
        );
    }

    @Test
    public void shouldThrowTopologyExceptionWhenMultipleNamedTopologiesCreateStreamFromOverlappingInputTopicCollection() {
        builder1.stream("stream");
        builder2.stream(asList("unique-input", "stream"));

        assertThrows(
            TopologyException.class,
            () -> streams = new KafkaStreamsNamedTopologyWrapper(
                asList(
                    builder1.buildNamedTopology(props),
                    builder2.buildNamedTopology(props)),
                props,
                clientSupplier)
        );
    }

    @Test
    public void shouldThrowTopologyExceptionWhenMultipleNamedTopologiesCreateStreamFromSamePattern() {
        builder1.stream(Pattern.compile("some-regex"));
        builder2.stream(Pattern.compile("some-regex"));

        assertThrows(
            TopologyException.class,
            () -> streams = new KafkaStreamsNamedTopologyWrapper(
                asList(
                    builder1.buildNamedTopology(props),
                    builder2.buildNamedTopology(props)),
                props,
                clientSupplier)
        );
    }

    @Test
    public void shouldDescribeWithSingleNamedTopology() {
        builder1.stream("input").filter((k, v) -> !k.equals(v)).to("output");
        streams = new KafkaStreamsNamedTopologyWrapper(builder1.buildNamedTopology(props), props, clientSupplier);

        assertThat(
            streams.getFullTopologyDescription(),
            equalTo(
                "Topology - topology-1:\n"
                    + "   Sub-topology: 0\n"
                    + "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-1])\n"
                    + "      --> none\n"
                    + "\n"
                    + "  Sub-topology: 1\n"
                    + "    Source: KSTREAM-SOURCE-0000000001 (topics: [input])\n"
                    + "      --> KSTREAM-FILTER-0000000002\n"
                    + "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n"
                    + "      --> KSTREAM-SINK-0000000003\n"
                    + "      <-- KSTREAM-SOURCE-0000000001\n"
                    + "    Sink: KSTREAM-SINK-0000000003 (topic: output)\n"
                    + "      <-- KSTREAM-FILTER-0000000002\n\n")
        );
    }

    @Test
    public void shouldDescribeWithMultipleNamedTopologies() {
        builder1.stream("stream-1").filter((k, v) -> !k.equals(v)).to("output-1");
        builder2.stream("stream-2").filter((k, v) -> !k.equals(v)).to("output-2");
        builder3.stream("stream-3").filter((k, v) -> !k.equals(v)).to("output-3");

        streams = new KafkaStreamsNamedTopologyWrapper(
            asList(
                builder1.buildNamedTopology(props),
                builder2.buildNamedTopology(props),
                builder3.buildNamedTopology(props)),
            props,
            clientSupplier
        );

        assertThat(
            streams.getFullTopologyDescription(),
            equalTo(
                     "Topology - topology-1:\n"
                    + "   Sub-topology: 0\n"
                    + "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-1])\n"
                    + "      --> none\n"
                    + "\n"
                    + "  Sub-topology: 1\n"
                    + "    Source: KSTREAM-SOURCE-0000000001 (topics: [stream-1])\n"
                    + "      --> KSTREAM-FILTER-0000000002\n"
                    + "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n"
                    + "      --> KSTREAM-SINK-0000000003\n"
                    + "      <-- KSTREAM-SOURCE-0000000001\n"
                    + "    Sink: KSTREAM-SINK-0000000003 (topic: output-1)\n"
                    + "      <-- KSTREAM-FILTER-0000000002\n"
                    + "\n"
                    + "Topology - topology-2:\n"
                    + "   Sub-topology: 0\n"
                    + "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-2])\n"
                    + "      --> none\n"
                    + "\n"
                    + "  Sub-topology: 1\n"
                    + "    Source: KSTREAM-SOURCE-0000000001 (topics: [stream-2])\n"
                    + "      --> KSTREAM-FILTER-0000000002\n"
                    + "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n"
                    + "      --> KSTREAM-SINK-0000000003\n"
                    + "      <-- KSTREAM-SOURCE-0000000001\n"
                    + "    Sink: KSTREAM-SINK-0000000003 (topic: output-2)\n"
                    + "      <-- KSTREAM-FILTER-0000000002\n"
                    + "\n"
                    + "Topology - topology-3:\n"
                    + "   Sub-topology: 0\n"
                    + "    Source: KSTREAM-SOURCE-0000000000 (topics: [input-3])\n"
                    + "      --> none\n"
                    + "\n"
                    + "  Sub-topology: 1\n"
                    + "    Source: KSTREAM-SOURCE-0000000001 (topics: [stream-3])\n"
                    + "      --> KSTREAM-FILTER-0000000002\n"
                    + "    Processor: KSTREAM-FILTER-0000000002 (stores: [])\n"
                    + "      --> KSTREAM-SINK-0000000003\n"
                    + "      <-- KSTREAM-SOURCE-0000000001\n"
                    + "    Sink: KSTREAM-SINK-0000000003 (topic: output-3)\n"
                    + "      <-- KSTREAM-FILTER-0000000002\n\n")
        );
    }

    @Test
    public void shouldDescribeWithEmptyNamedTopology() {
        streams = new KafkaStreamsNamedTopologyWrapper(props, clientSupplier);

        assertThat(streams.getFullTopologyDescription(), equalTo(""));
    }
}
