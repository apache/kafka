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

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.errors.UnknownTopologyException;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyBuilder;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyStoreQueryParameters;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static java.util.Arrays.asList;

public class NamedTopologyTest {
    private static final String UNKNOWN_TOPOLOGY = "not-a-real-topology";
    private static final String UNKNOWN_STORE = "not-a-real-store";
    private final Properties props = configProps();

    private final KafkaStreamsNamedTopologyWrapper streams = new KafkaStreamsNamedTopologyWrapper(props);

    private final NamedTopologyBuilder builder1 = streams.newNamedTopologyBuilder("topology-1");
    private final NamedTopologyBuilder builder2 = streams.newNamedTopologyBuilder("topology-2");
    private final NamedTopologyBuilder builder3 = streams.newNamedTopologyBuilder("topology-3");

    @Before
    public void setup() {
        builder1.stream("input-1");
        builder2.stream("input-2");
        builder3.stream("input-3");
    }

    @After
    public void cleanup() {
        streams.close();
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
        assertThrows(IllegalArgumentException.class, () -> streams.newNamedTopologyBuilder("__not-allowed__"));
    }

    @Test
    public void shouldBuildSingleNamedTopology() {
        builder1.stream("stream-1").filter((k, v) -> !k.equals(v)).to("output-1");
        streams.start(builder1.build());
    }

    @Test
    public void shouldBuildMultipleIdenticalNamedTopologyWithRepartition() {
        builder1.stream("stream-1").selectKey((k, v) -> v).groupByKey().count().toStream().to("output-1");
        builder2.stream("stream-2").selectKey((k, v) -> v).groupByKey().count().toStream().to("output-2");
        builder3.stream("stream-3").selectKey((k, v) -> v).groupByKey().count().toStream().to("output-3");

        streams.start(
            asList(
                builder1.build(),
                builder2.build(),
                builder3.build())
        );
    }

    @Test
    public void shouldReturnTopologyByName() {
        final NamedTopology topology1 = builder1.build();
        final NamedTopology topology2 = builder2.build();
        final NamedTopology topology3 = builder3.build();
        streams.start(asList(topology1, topology2, topology3));
        assertThat(streams.getTopologyByName("topology-1").get(), equalTo(topology1));
        assertThat(streams.getTopologyByName("topology-2").get(), equalTo(topology2));
        assertThat(streams.getTopologyByName("topology-3").get(), equalTo(topology3));
    }

    @Test
    public void shouldReturnEmptyWhenLookingUpNonExistentTopologyByName() {
        streams.start(builder1.build());
        assertThat(streams.getTopologyByName("non-existent-topology").isPresent(), equalTo(false));
    }

    @Test
    public void shouldAllowSameStoreNameToBeUsedByMultipleNamedTopologies() {
        builder1.stream("stream-1").selectKey((k, v) -> v).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store")));
        builder2.stream("stream-2").selectKey((k, v) -> v).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store")));

        streams.start(asList(builder1.build(), builder2.build()));
    }

    @Test
    public void shouldAllowAddingAndRemovingNamedTopologyAndReturnBeforeCallingStart() throws Exception {
        builder1.stream("stream-1").selectKey((k, v) -> v).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store")));
        builder2.stream("stream-2").selectKey((k, v) -> v).groupByKey().count(Materialized.as(Stores.inMemoryKeyValueStore("store")));

        streams.addNamedTopology(builder1.build()).all().get();
        streams.addNamedTopology(builder2.build()).all().get();

        streams.removeNamedTopology("topology-2").all().get();
    }

    @Test
    public void shouldThrowTopologyExceptionWhenMultipleNamedTopologiesCreateStreamFromSameInputTopic() {
        builder1.stream("stream");
        builder2.stream("stream");

        assertThrows(
            TopologyException.class,
            () -> streams.start(asList(
                builder1.build(),
                builder2.build()))
        );
    }

    @Test
    public void shouldThrowTopologyExceptionWhenAddingNamedTopologyReadingFromSameInputTopicAfterStart() {
        builder1.stream("stream");
        builder2.stream("stream");

        streams.start();

        streams.addNamedTopology(builder1.build());

        final ExecutionException exception = assertThrows(
            ExecutionException.class,
            () -> streams.addNamedTopology(builder2.build()).all().get()
        );

        assertThat(exception.getCause().getClass(), equalTo(TopologyException.class));
    }

    @Test
    public void shouldThrowTopologyExceptionWhenAddingNamedTopologyReadingFromSameInputTopicBeforeStart() {
        builder1.stream("stream");
        builder2.stream("stream");
        
        streams.addNamedTopology(builder1.build());

        final ExecutionException exception = assertThrows(
            ExecutionException.class,
            () -> streams.addNamedTopology(builder2.build()).all().get()
        );

        assertThat(exception.getCause().getClass(), equalTo(TopologyException.class));
    }

    @Test
    public void shouldThrowTopologyExceptionWhenMultipleNamedTopologiesCreateTableFromSameInputTopic() {
        builder1.table("table");
        builder2.table("table");

        assertThrows(
            TopologyException.class,
            () -> streams.start(asList(
                builder1.build(),
                builder2.build()))
        );
    }

    @Test
    public void shouldThrowTopologyExceptionWhenMultipleNamedTopologiesCreateStreamAndTableFromSameInputTopic() {
        builder1.stream("input");
        builder2.table("input");

        assertThrows(
            TopologyException.class,
            () -> streams.start(asList(
                builder1.build(),
                builder2.build()))
        );
    }

    @Test
    public void shouldThrowTopologyExceptionWhenMultipleNamedTopologiesCreateStreamFromOverlappingInputTopicCollection() {
        builder1.stream("stream");
        builder2.stream(asList("unique-input", "stream"));

        assertThrows(
            TopologyException.class,
            () -> streams.start(asList(
                builder1.build(),
                builder2.build()))
        );
    }

    @Test
    public void shouldThrowTopologyExceptionWhenMultipleNamedTopologiesCreateStreamFromSamePattern() {
        builder1.stream(Pattern.compile("some-regex"));
        builder2.stream(Pattern.compile("some-regex"));

        assertThrows(
            TopologyException.class,
            () -> streams.start(asList(
                builder1.build(),
                builder2.build()))
        );
    }

    @Test
    public void shouldThrowUnknownTopologyExceptionForAllLocalStorePartitionLags() {
        streams.addNamedTopology(builder1.build());
        streams.start();
        assertThrows(
            UnknownTopologyException.class,
            () -> streams.allLocalStorePartitionLagsForTopology(UNKNOWN_TOPOLOGY)
        );
    }

    @Test
    public void shouldThrowUnknownTopologyExceptionForQueryMetadataForKey() {
        streams.addNamedTopology(builder1.build());
        streams.start();
        assertThrows(
            UnknownTopologyException.class,
            () -> streams.queryMetadataForKey("store", "A", new StringSerializer(), UNKNOWN_TOPOLOGY)
        );
    }

    @Test
    public void shouldThrowUnknownStateStoreExceptionForQueryMetadataForKey() {
        streams.addNamedTopology(builder1.build());
        streams.start();
        assertThrows(
            UnknownStateStoreException.class,
            () -> streams.queryMetadataForKey(UNKNOWN_STORE, "A", new StringSerializer(), "topology-1")
        );
    }

    @Test
    public void shouldThrowUnknownTopologyExceptionForStreamsMetadataForStore() {
        streams.addNamedTopology(builder1.build());
        streams.start();
        assertThrows(
            UnknownTopologyException.class,
            () -> streams.streamsMetadataForStore("store", UNKNOWN_TOPOLOGY)
        );
    }

    @Test
    public void shouldThrowUnknownStateStoreExceptionForStreamsMetadataForStore() {
        streams.addNamedTopology(builder1.build());
        streams.start();
        assertThrows(
            UnknownStateStoreException.class,
            () -> streams.streamsMetadataForStore(UNKNOWN_STORE, "topology-1")
        );
    }

    @Test
    public void shouldThrowUnknownTopologyExceptionForStore() {
        streams.addNamedTopology(builder1.build());
        streams.start();
        assertThrows(
            UnknownTopologyException.class,
            () -> streams.store(
                NamedTopologyStoreQueryParameters.fromNamedTopologyAndStoreNameAndType(
                    UNKNOWN_TOPOLOGY,
                    "store",
                    keyValueStore()
                ))
        );
    }

    @Test
    public void shouldThrowUnknownStateStoreExceptionForStore() {
        streams.addNamedTopology(builder1.build());
        streams.start();
        assertThrows(
            UnknownStateStoreException.class,
            () -> streams.store(
                NamedTopologyStoreQueryParameters.fromNamedTopologyAndStoreNameAndType(
                    "topology-1",
                    UNKNOWN_STORE,
                    keyValueStore()
                ))
        );
    }

    @Test
    public void shouldDescribeWithSingleNamedTopology() {
        builder1.stream("input").filter((k, v) -> !k.equals(v)).to("output");
        streams.start(builder1.build());

        assertThat(
            streams.getFullTopologyDescription(),
            equalTo(
                "Topology: topology-1:\n"
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

        streams.start(
            asList(
                builder1.build(),
                builder2.build(),
                builder3.build())
        );

        assertThat(
            streams.getFullTopologyDescription(),
            equalTo(
                     "Topology: topology-1:\n"
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
                    + "Topology: topology-2:\n"
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
                    + "Topology: topology-3:\n"
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
        assertThat(streams.getFullTopologyDescription(), equalTo(""));
    }
}
