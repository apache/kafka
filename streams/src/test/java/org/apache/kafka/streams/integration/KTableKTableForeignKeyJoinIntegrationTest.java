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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


@Category(IntegrationTest.class)
public class KTableKTableForeignKeyJoinIntegrationTest {

    private static final String LEFT_TABLE = "left_table";
    private static final String RIGHT_TABLE = "right_table";
    private static final String OUTPUT = "output-topic";
    private static final Properties STREAMS_CONFIG = mkProperties(mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-ktable-joinOnForeignKey"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "asdf:0000"),
        mkEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
        mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
        mkEntry(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"),
        mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100"),
        mkEntry(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE)
    ));


    @Test
    public void doJoinFromLeftThenDeleteLeftEntity() {
        for (final Boolean leftJoin : new Boolean[] {false, true}) {
            final Topology topology = getTopology(STREAMS_CONFIG, "store", leftJoin);
            try (final TopologyTestDriver driver = new TopologyTestDriver(topology, STREAMS_CONFIG)) {
                final TestInputTopic<String, Long> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new LongSerializer());
                final TestInputTopic<Integer, Float> left = driver.createInputTopic(LEFT_TABLE, new IntegerSerializer(), new FloatSerializer());
                final TestOutputTopic<Integer, String> outputTopic = driver.createOutputTopic(OUTPUT, new IntegerDeserializer(), new StringDeserializer());
                final KeyValueStore<Integer, String> store = driver.getKeyValueStore("store");

                right.pipeInput("1", 10L);
                right.pipeInput("2", 20L);

                left.pipeInput(1, 1.33f);
                left.pipeInput(2, 2.77f);

                {
                    final Map<Integer, String> expected = mkMap(
                        mkEntry(1, "value1=1.33,value2=10"),
                        mkEntry(2, "value1=2.77,value2=20")
                    );
                    assertThat(
                        "leftJoin:" + leftJoin,
                        outputTopic.readKeyValuesToMap(),
                        is(expected)
                    );
                    assertThat(
                        "leftJoin:" + leftJoin,
                        asMap(store),
                        is(expected)
                    );
                }

                //Now delete one LHS entity such that one delete is propagated down to the output.

                left.pipeInput(1, null);
                assertThat(
                    "leftJoin:" + leftJoin,
                    outputTopic.readKeyValuesToMap(),
                    is(mkMap(
                        mkEntry(1, null)
                    ))
                );
                assertThat(
                    "leftJoin:" + leftJoin,
                    asMap(store),
                    is(mkMap(
                        mkEntry(2, "value1=2.77,value2=20")
                    ))
                );
            }
        }
    }

    @Test
    public void doJoinFromRightThenDeleteRightEntity() {
        for (final Boolean leftJoin : new Boolean[] {false, true}) {
            final Topology topology = getTopology(STREAMS_CONFIG, "store", leftJoin);
            try (final TopologyTestDriver driver = new TopologyTestDriver(topology, STREAMS_CONFIG)) {
                final TestInputTopic<String, Long> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new LongSerializer());
                final TestInputTopic<Integer, Float> left = driver.createInputTopic(LEFT_TABLE, new IntegerSerializer(), new FloatSerializer());
                final TestOutputTopic<Integer, String> outputTopic = driver.createOutputTopic(OUTPUT, new IntegerDeserializer(), new StringDeserializer());
                final KeyValueStore<Integer, String> store = driver.getKeyValueStore("store");

                left.pipeInput(1, 1.33f);
                left.pipeInput(2, 1.77f);
                left.pipeInput(3, 3.77f);

                right.pipeInput("1", 10L);
                right.pipeInput("2", 20L);
                right.pipeInput("3", 30L);

                //Ensure that the joined values exist in the output
                {
                    final Map<Integer, String> expected = mkMap(
                        mkEntry(1, "value1=1.33,value2=10"),
                        mkEntry(2, "value1=1.77,value2=10"),
                        mkEntry(3, "value1=3.77,value2=30")
                    );
                    assertThat(
                        "leftJoin:" + leftJoin,
                        outputTopic.readKeyValuesToMap(),
                        is(expected)
                    );
                    assertThat(
                        "leftJoin:" + leftJoin,
                        asMap(store),
                        is(expected)
                    );
                }

                //Now delete the RHS entity such that all matching keys have deletes propagated.
                right.pipeInput("1", null);

                if (leftJoin) {
                    assertThat(
                        "leftJoin",
                        outputTopic.readKeyValuesToMap(),
                        is(mkMap(
                            mkEntry(1, "value1=1.33,value2=null"),
                            mkEntry(2, "value1=1.77,value2=null")
                        ))
                    );
                    assertThat(
                        "leftJoin",
                        asMap(store),
                        is(mkMap(
                            mkEntry(1, "value1=1.33,value2=null"),
                            mkEntry(2, "value1=1.77,value2=null"),
                            mkEntry(3, "value1=3.77,value2=30")
                        ))
                    );
                } else {
                    assertThat(
                        "innerJoin",
                        outputTopic.readKeyValuesToMap(),
                        is(mkMap(
                            mkEntry(1, null),
                            mkEntry(2, null)
                        ))
                    );
                    assertThat(
                        "innerJoin",
                        asMap(store),
                        is(mkMap(
                            mkEntry(3, "value1=3.77,value2=30")
                        ))
                    );
                }
            }
        }
    }

    @Test
    public void joinShouldProduceNullsWhenValueHasNonMatchingForeignKey() {
        for (final Boolean leftJoin : new Boolean[] {false, true}) {
            final Topology topology = getTopology(STREAMS_CONFIG, "store", leftJoin);
            try (final TopologyTestDriver driver = new TopologyTestDriver(topology, STREAMS_CONFIG)) {
                final TestInputTopic<String, Long> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new LongSerializer());
                final TestInputTopic<Integer, Float> left = driver.createInputTopic(LEFT_TABLE, new IntegerSerializer(), new FloatSerializer());
                final TestOutputTopic<Integer, String> outputTopic = driver.createOutputTopic(OUTPUT, new IntegerDeserializer(), new StringDeserializer());
                final KeyValueStore<Integer, String> store = driver.getKeyValueStore("store");

                right.pipeInput("1", 10L);

                left.pipeInput(1, 8.33f);
                // no output for a new inner join on a non-existent FK (8)
                assertThat(
                    "leftJoin:" + leftJoin,
                    outputTopic.readKeyValuesToMap(),
                    is(leftJoin ? mkMap(mkEntry(1, "value1=8.33,value2=null")) : emptyMap())
                );
                assertThat(
                    "leftJoin:" + leftJoin,
                    asMap(store),
                    is(leftJoin ? mkMap(mkEntry(1, "value1=8.33,value2=null")) : emptyMap())
                );
                // "moving" our subscription to another non-existent FK (18) results in a tombstone, since it
                // impossible to know whether the other FK exists or not (and thus whether any results have
                // previously been emitted)
                left.pipeInput(1, 18.0f);
                assertThat(
                    "leftJoin:" + leftJoin,
                    outputTopic.readKeyValuesToMap(),
                    is(mkMap(
                        mkEntry(1, leftJoin ? "value1=18.0,value2=null" : null)
                    ))
                );
                assertThat(
                    "leftJoin:" + leftJoin,
                    asMap(store),
                    is(leftJoin ? mkMap(mkEntry(1, "value1=18.0,value2=null")) : emptyMap())
                );
                // of course, moving it again to yet another non-existent FK (100) still results in a tombstone
                left.pipeInput(1, 100.0f);
                assertThat(
                    "leftJoin:" + leftJoin,
                    outputTopic.readKeyValuesToMap(),
                    is(mkMap(
                        mkEntry(1, leftJoin ? "value1=100.0,value2=null" : null)
                    ))
                );
                assertThat(
                    "leftJoin:" + leftJoin,
                    asMap(store),
                    is(leftJoin ? mkMap(mkEntry(1, "value1=100.0,value2=null")) : emptyMap())
                );
                // now, we change to a FK that exists
                left.pipeInput(1, 1.11f);
                assertThat(
                    "leftJoin:" + leftJoin,
                    outputTopic.readKeyValuesToMap(),
                    is(mkMap(
                        mkEntry(1, "value1=1.11,value2=10")
                    ))
                );
                assertThat(
                    "leftJoin:" + leftJoin,
                    asMap(store),
                    is(mkMap(mkEntry(1, "value1=1.11,value2=10")))
                );
                // but if we update it again to a non-existent one, we'll get another tombstone.
                left.pipeInput(1, 13.0f);
                assertThat(
                    "leftJoin:" + leftJoin,
                    outputTopic.readKeyValuesToMap(),
                    is(mkMap(
                        mkEntry(1, leftJoin ? "value1=13.0,value2=null" : null)
                    ))
                );
                assertThat(
                    "leftJoin:" + leftJoin,
                    asMap(store),
                    is(leftJoin ? mkMap(mkEntry(1, "value1=13.0,value2=null")) : emptyMap())
                );
            }
        }
    }

    private static Map<Integer, String> asMap(final KeyValueStore<Integer, String> store) {
        final HashMap<Integer, String> result = new HashMap<>();
        store.all().forEachRemaining(kv -> result.put(kv.key, kv.value));
        return result;
    }

    private static Topology getTopology(final Properties streamsConfig,
                                        final String queryableStoreName,
                                        final boolean leftJoin) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<Integer, Float> left = builder.table(LEFT_TABLE, Consumed.with(Serdes.Integer(), Serdes.Float()));
        final KTable<String, Long> right = builder.table(RIGHT_TABLE, Consumed.with(Serdes.String(), Serdes.Long()));

        final Materialized<Integer, String, KeyValueStore<Bytes, byte[]>> materialized =
            Materialized.<Integer, String>as(Stores.inMemoryKeyValueStore(queryableStoreName))
                .withKeySerde(Serdes.Integer())
                .withValueSerde(Serdes.String())
                .withCachingDisabled();

        final Function<Float, String> extractor = value -> Integer.toString((int) value.floatValue());
        final ValueJoiner<Float, Long, String> joiner = (value1, value2) -> "value1=" + value1 + ",value2=" + value2;

        if (leftJoin)
            left.leftJoin(right,
                          extractor,
                          joiner,
                          Named.as("customName"),
                          materialized)
                .toStream()
                .to(OUTPUT, Produced.with(Serdes.Integer(), Serdes.String()));
        else
            left.join(right,
                      extractor,
                      joiner,
                      Named.as("customName"),
                      materialized)
                .toStream()
                .to(OUTPUT, Produced.with(Serdes.Integer(), Serdes.String()));

        return builder.build(streamsConfig);
    }
}
