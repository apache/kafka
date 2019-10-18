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
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
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


@RunWith(Parameterized.class)
public class KTableKTableForeignKeyJoinIntegrationTest {

    private static final String LEFT_TABLE = "left_table";
    private static final String RIGHT_TABLE = "right_table";
    private static final String OUTPUT = "output-topic";
    private final Properties streamsConfig;
    private final boolean leftJoin;

    public KTableKTableForeignKeyJoinIntegrationTest(final boolean leftJoin, final String optimization) {
        this.leftJoin = leftJoin;
        streamsConfig = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-ktable-joinOnForeignKey"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "asdf:0000"),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimization)
        ));
    }

    @Parameterized.Parameters(name = "leftJoin={0}, optimization={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
            new Object[] {false, StreamsConfig.OPTIMIZE},
            new Object[] {false, StreamsConfig.NO_OPTIMIZATION},
            new Object[] {true, StreamsConfig.OPTIMIZE},
            new Object[] {true, StreamsConfig.NO_OPTIMIZATION}
        );
    }

    @Test
    public void doJoinFromLeftThenDeleteLeftEntity() {
        final Topology topology = getTopology(streamsConfig, "store", leftJoin);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            // Pre-populate the RHS records. This test is all about what happens when we add/remove LHS records
            right.pipeInput("rhs1", "rhsValue1");
            right.pipeInput("rhs2", "rhsValue2");
            right.pipeInput("rhs3", "rhsValue3"); // this unreferenced FK won't show up in any results

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(emptyMap())
            );
            assertThat(
                asMap(store),
                is(emptyMap())
            );

            left.pipeInput("lhs1", "lhsValue1|rhs1");
            left.pipeInput("lhs2", "lhsValue2|rhs2");

            {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                    mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)")
                );
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(expected)
                );
                assertThat(
                    asMap(store),
                    is(expected)
                );
            }

            // Add another reference to an existing FK
            left.pipeInput("lhs3", "lhsValue3|rhs1");
            {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(mkMap(
                        mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)")
                    ))
                );
                assertThat(
                    asMap(store),
                    is(mkMap(
                        mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                        mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)"),
                        mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)")
                    ))
                );
            }
            // Now delete one LHS entity such that one delete is propagated down to the output.

            left.pipeInput("lhs1", (String) null);
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(
                    mkEntry("lhs1", null)
                ))
            );
            assertThat(
                asMap(store),
                is(mkMap(
                    mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)"),
                    mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)")
                ))
            );
        }
    }

    @Test
    public void doJoinFromRightThenDeleteRightEntity() {
        final Topology topology = getTopology(streamsConfig, "store", leftJoin);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            // Pre-populate the LHS records. This test is all about what happens when we add/remove RHS records
            left.pipeInput("lhs1", "lhsValue1|rhs1");
            left.pipeInput("lhs2", "lhsValue2|rhs2");
            left.pipeInput("lhs3", "lhsValue3|rhs1");

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(leftJoin
                       ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,null)"),
                               mkEntry("lhs2", "(lhsValue2|rhs2,null)"),
                               mkEntry("lhs3", "(lhsValue3|rhs1,null)"))
                       : emptyMap()
                )
            );
            assertThat(
                asMap(store),
                is(leftJoin
                       ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,null)"),
                               mkEntry("lhs2", "(lhsValue2|rhs2,null)"),
                               mkEntry("lhs3", "(lhsValue3|rhs1,null)"))
                       : emptyMap()
                )
            );

            right.pipeInput("rhs1", "rhsValue1");

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                         mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)"))
                )
            );
            assertThat(
                asMap(store),
                is(leftJoin
                       ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                               mkEntry("lhs2", "(lhsValue2|rhs2,null)"),
                               mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)"))

                       : mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                               mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)"))
                )
            );

            right.pipeInput("rhs2", "rhsValue2");

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)")))
            );
            assertThat(
                asMap(store),
                is(mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                         mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)"),
                         mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)"))
                )
            );

            right.pipeInput("rhs3", "rhsValue3"); // this unreferenced FK won't show up in any results

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(emptyMap())
            );
            assertThat(
                asMap(store),
                is(mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                         mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)"),
                         mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)"))
                )
            );

            // Now delete the RHS entity such that all matching keys have deletes propagated.
            right.pipeInput("rhs1", (String) null);

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(mkEntry("lhs1", leftJoin ? "(lhsValue1|rhs1,null)" : null),
                         mkEntry("lhs3", leftJoin ? "(lhsValue3|rhs1,null)" : null))
                )
            );
            assertThat(
                asMap(store),
                is(leftJoin
                       ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,null)"),
                               mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)"),
                               mkEntry("lhs3", "(lhsValue3|rhs1,null)"))

                       : mkMap(mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)"))
                )
            );
        }
    }

    @Test
    public void shouldEmitTombstoneWhenDeletingNonJoiningRecords() {
        final Topology topology = getTopology(streamsConfig, "store", leftJoin);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            left.pipeInput("lhs1", "lhsValue1|rhs1");

            {
                final Map<String, String> expected =
                    leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,null)")) : emptyMap();
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(expected)
                );
                assertThat(
                    asMap(store),
                    is(expected)
                );
            }

            // Deleting a non-joining record produces an unnecessary tombstone for inner joins, because
            // it's not possible to know whether a result was previously emitted.
            // For the left join, the tombstone is necessary.
            left.pipeInput("lhs1", (String) null);
            {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(mkMap(mkEntry("lhs1", null)))
                );
                assertThat(
                    asMap(store),
                    is(emptyMap())
                );
            }

            // Deleting a non-existing record is idempotent
            left.pipeInput("lhs1", (String) null);
            {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(emptyMap())
                );
                assertThat(
                    asMap(store),
                    is(emptyMap())
                );
            }
        }
    }

    @Test
    public void shouldNotEmitTombstonesWhenDeletingNonExistingRecords() {
        final Topology topology = getTopology(streamsConfig, "store", leftJoin);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            // Deleting a record that never existed doesn't need to emit tombstones.
            left.pipeInput("lhs1", (String) null);
            {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(emptyMap())
                );
                assertThat(
                    asMap(store),
                    is(emptyMap())
                );
            }
        }
    }

    @Test
    public void joinShouldProduceNullsWhenValueHasNonMatchingForeignKey() {
        final Topology topology = getTopology(streamsConfig, "store", leftJoin);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            left.pipeInput("lhs1", "lhsValue1|rhs1");
            // no output for a new inner join on a non-existent FK
            // the left join of course emits the half-joined output
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,null)")) : emptyMap())
            );
            assertThat(
                asMap(store),
                is(leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,null)")) : emptyMap())
            );
            // "moving" our subscription to another non-existent FK results in an unnecessary tombstone for inner join,
            // since it impossible to know whether the prior FK existed or not (and thus whether any results have
            // previously been emitted)
            // The left join emits a _necessary_ update (since the lhs record has actually changed)
            left.pipeInput("lhs1", "lhsValue1|rhs2");
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(mkEntry("lhs1", leftJoin ? "(lhsValue1|rhs2,null)" : null)))
            );
            assertThat(
                asMap(store),
                is(leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs2,null)")) : emptyMap())
            );
            // of course, moving it again to yet another non-existent FK has the same effect
            left.pipeInput("lhs1", "lhsValue1|rhs3");
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(mkEntry("lhs1", leftJoin ? "(lhsValue1|rhs3,null)" : null)))
            );
            assertThat(
                asMap(store),
                is(leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs3,null)")) : emptyMap())
            );

            // Adding an RHS record now, so that we can demonstrate "moving" from a non-existent FK to an existent one
            // This RHS key was previously referenced, but it's not referenced now, so adding this record should
            // result in no changes whatsoever.
            right.pipeInput("rhs1", "rhsValue1");
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(emptyMap())
            );
            assertThat(
                asMap(store),
                is(leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs3,null)")) : emptyMap())
            );

            // now, we change to a FK that exists, and see the join completes
            left.pipeInput("lhs1", "lhsValue1|rhs1");
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)")
                ))
            );
            assertThat(
                asMap(store),
                is(mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)")
                ))
            );

            // but if we update it again to a non-existent one, we'll get a tombstone for the inner join, and the
            // left join updates appropriately.
            left.pipeInput("lhs1", "lhsValue1|rhs2");
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(
                    mkEntry("lhs1", leftJoin ? "(lhsValue1|rhs2,null)" : null)
                ))
            );
            assertThat(
                asMap(store),
                is(leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs2,null)")) : emptyMap())
            );
        }
    }

    private static Map<String, String> asMap(final KeyValueStore<String, String> store) {
        final HashMap<String, String> result = new HashMap<>();
        store.all().forEachRemaining(kv -> result.put(kv.key, kv.value));
        return result;
    }

    private static Topology getTopology(final Properties streamsConfig,
                                        final String queryableStoreName,
                                        final boolean leftJoin) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> left = builder.table(LEFT_TABLE, Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> right = builder.table(RIGHT_TABLE, Consumed.with(Serdes.String(), Serdes.String()));

        final Function<String, String> extractor = value -> value.split("\\|")[1];
        final ValueJoiner<String, String, String> joiner = (value1, value2) -> "(" + value1 + "," + value2 + ")";

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            Materialized.<String, String>as(Stores.inMemoryKeyValueStore(queryableStoreName))
                .withValueSerde(Serdes.String())
                // the cache suppresses some of the unnecessary tombstones we want to make assertions about
                .withCachingDisabled();

        final KTable<String, String> joinResult;
        if (leftJoin) {
            joinResult = left.leftJoin(
                right,
                extractor,
                joiner,
                materialized
            );
        } else {
            joinResult = left.join(
                right,
                extractor,
                joiner,
                materialized
            );
        }

        joinResult
            .toStream()
            .to(OUTPUT);

        return builder.build(streamsConfig);
    }
}
