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
import org.apache.kafka.common.utils.MockTime;
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
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.utils.UniqueTopicSerdeScope;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
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
@Category(IntegrationTest.class)
public class KTableKTableForeignKeyJoinIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);
    protected static final String LEFT_TABLE = "left_table";
    protected static final String RIGHT_TABLE = "right_table";
    protected static final String OUTPUT = "output-topic";
    private static final String REJOIN_OUTPUT = "rejoin-output-topic";

    private final MockTime time = new MockTime();

    protected final boolean leftJoin;
    protected final boolean materialized;
    private final String optimization;
    protected final boolean rejoin;
    protected final boolean leftVersioned;
    protected final boolean rightVersioned;

    protected Properties streamsConfig;
    protected long baseTimestamp;

    public KTableKTableForeignKeyJoinIntegrationTest(final boolean leftJoin,
                                                     final String optimization,
                                                     final boolean materialized,
                                                     final boolean rejoin) {
        // versioning is disabled for these tests, even though the code supports building a
        // topology with versioned tables, since KTableKTableForeignKeyVersionedJoinIntegrationTest
        // extends this test class.
        this(leftJoin, optimization, materialized, rejoin, false, false);
    }

    protected KTableKTableForeignKeyJoinIntegrationTest(final boolean leftJoin,
                                                        final String optimization,
                                                        final boolean materialized,
                                                        final boolean rejoin,
                                                        final boolean leftVersioned,
                                                        final boolean rightVersioned) {
        this.rejoin = rejoin;
        this.leftJoin = leftJoin;
        this.materialized = materialized;
        this.optimization = optimization;
        this.leftVersioned = leftVersioned;
        this.rightVersioned = rightVersioned;
    }

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() {
        streamsConfig = mkProperties(mkMap(
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimization)
        ));
        baseTimestamp = time.milliseconds();
    }

    @Parameterized.Parameters(name = "leftJoin={0}, optimization={1}, materialized={2}, rejoin={3}")
    public static Collection<Object[]> data() {
        final List<Boolean> booleans = Arrays.asList(true, false);
        final List<String> optimizations = Arrays.asList(StreamsConfig.OPTIMIZE, StreamsConfig.NO_OPTIMIZATION);
        return buildParameters(booleans, optimizations, booleans, booleans);
    }

    protected static Collection<Object[]> buildParameters(final List<?>... argOptions) {
        List<Object[]> result = new LinkedList<>();
        result.add(new Object[0]);

        for (final List<?> argOption : argOptions) {
            result = times(result, argOption);
        }

        return result;
    }

    private static List<Object[]> times(final List<Object[]> left, final List<?> right) {
        final List<Object[]> result = new LinkedList<>();
        for (final Object[] args : left) {
            for (final Object rightElem : right) {
                final Object[] resArgs = new Object[args.length + 1];
                System.arraycopy(args, 0, resArgs, 0, args.length);
                resArgs[args.length] = rightElem;
                result.add(resArgs);
            }
        }
        return result;
    }

    @Test
    public void doJoinFromLeftThenDeleteLeftEntity() {
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final TestOutputTopic<String, String> rejoinOutputTopic = rejoin ? driver.createOutputTopic(REJOIN_OUTPUT, new StringDeserializer(), new StringDeserializer()) : null;
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            // Pre-populate the RHS records. This test is all about what happens when we add/remove LHS records
            right.pipeInput("rhs1", "rhsValue1", baseTimestamp);
            right.pipeInput("rhs2", "rhsValue2", baseTimestamp + 1);
            right.pipeInput("rhs3", "rhsValue3", baseTimestamp + 2); // this unreferenced FK won't show up in any results

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(emptyMap())
            );
            if (rejoin) {
                assertThat(
                    rejoinOutputTopic.readKeyValuesToMap(),
                    is(emptyMap())
                );
            }
            if (materialized) {
                assertThat(
                    asMap(store),
                    is(emptyMap())
                );
            }

            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp + 3);
            left.pipeInput("lhs2", "lhsValue2|rhs2", baseTimestamp + 4);

            {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                    mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)")
                );
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(expected)
                );
                if (rejoin) {
                    assertThat(
                        rejoinOutputTopic.readKeyValuesToMap(),
                        is(mkMap(
                            mkEntry("lhs1", "rejoin((lhsValue1|rhs1,rhsValue1),lhsValue1|rhs1)"),
                            mkEntry("lhs2", "rejoin((lhsValue2|rhs2,rhsValue2),lhsValue2|rhs2)")
                        ))
                    );
                }
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(expected)
                    );
                }
            }

            // Add another reference to an existing FK
            left.pipeInput("lhs3", "lhsValue3|rhs1", baseTimestamp + 5);
            {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(mkMap(
                        mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)")
                    ))
                );
                if (rejoin) {
                    assertThat(
                        rejoinOutputTopic.readKeyValuesToMap(),
                        is(mkMap(
                            mkEntry("lhs3", "rejoin((lhsValue3|rhs1,rhsValue1),lhsValue3|rhs1)")
                        ))
                    );
                }
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(mkMap(
                            mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                            mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)"),
                            mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)")
                        ))
                    );
                }
            }
            // Now delete one LHS entity such that one delete is propagated down to the output.

            left.pipeInput("lhs1", (String) null, baseTimestamp + 6);
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(
                    mkEntry("lhs1", null)
                ))
            );
            if (rejoin) {
                assertThat(
                    rejoinOutputTopic.readKeyValuesToMap(),
                    is(mkMap(
                        mkEntry("lhs1", null)
                    ))
                );
            }
            if (materialized) {
                assertThat(
                    asMap(store),
                    is(mkMap(
                        mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)"),
                        mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)")
                    ))
                );
            }
        }
    }

    @Test
    public void doJoinFromRightThenDeleteRightEntity() {
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            // Pre-populate the LHS records. This test is all about what happens when we add/remove RHS records
            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp);
            left.pipeInput("lhs2", "lhsValue2|rhs2", baseTimestamp + 1);
            left.pipeInput("lhs3", "lhsValue3|rhs1", baseTimestamp + 2);

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(leftJoin
                    ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,null)"),
                    mkEntry("lhs2", "(lhsValue2|rhs2,null)"),
                    mkEntry("lhs3", "(lhsValue3|rhs1,null)"))
                    : emptyMap()
                )
            );
            if (materialized) {
                assertThat(
                    asMap(store),
                    is(leftJoin
                        ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,null)"),
                        mkEntry("lhs2", "(lhsValue2|rhs2,null)"),
                        mkEntry("lhs3", "(lhsValue3|rhs1,null)"))
                        : emptyMap()
                    )
                );
            }

            right.pipeInput("rhs1", "rhsValue1", baseTimestamp + 3);

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                    mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)"))
                )
            );
            if (materialized) {
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
            }

            right.pipeInput("rhs2", "rhsValue2", baseTimestamp + 4);

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)")))
            );
            if (materialized) {
                assertThat(
                    asMap(store),
                    is(mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                        mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)"),
                        mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)"))
                    )
                );
            }

            right.pipeInput("rhs3", "rhsValue3", baseTimestamp + 5); // this unreferenced FK won't show up in any results

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(emptyMap())
            );
            if (materialized) {
                assertThat(
                    asMap(store),
                    is(mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                        mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)"),
                        mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)"))
                    )
                );
            }

            // Now delete the RHS entity such that all matching keys have deletes propagated.
            right.pipeInput("rhs1", (String) null, baseTimestamp + 6);

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(mkEntry("lhs1", leftJoin ? "(lhsValue1|rhs1,null)" : null),
                    mkEntry("lhs3", leftJoin ? "(lhsValue3|rhs1,null)" : null))
                )
            );
            if (materialized) {
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
    }

    @Test
    public void shouldEmitTombstoneWhenDeletingNonJoiningRecords() {
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp);

            {
                final Map<String, String> expected =
                    leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,null)")) : emptyMap();
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(expected)
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(expected)
                    );
                }
            }

            // Deleting a non-joining record produces an unnecessary tombstone for inner joins, because
            // it's not possible to know whether a result was previously emitted.
            // For the left join, the tombstone is necessary.
            left.pipeInput("lhs1", (String) null, baseTimestamp + 1);
            {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(mkMap(mkEntry("lhs1", null)))
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(emptyMap())
                    );
                }
            }

            // Deleting a non-existing record is idempotent
            left.pipeInput("lhs1", (String) null, baseTimestamp + 2);
            {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(emptyMap())
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(emptyMap())
                    );
                }
            }
        }
    }

    @Test
    public void shouldNotEmitTombstonesWhenDeletingNonExistingRecords() {
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            // Deleting a record that never existed doesn't need to emit tombstones.
            left.pipeInput("lhs1", (String) null, baseTimestamp);
            {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(emptyMap())
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(emptyMap())
                    );
                }
            }
        }
    }

    @Test
    public void joinShouldProduceNullsWhenValueHasNonMatchingForeignKey() {
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp);
            // no output for a new inner join on a non-existent FK
            // the left join of course emits the half-joined output
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,null)")) : emptyMap())
            );
            if (materialized) {
                assertThat(
                    asMap(store),
                    is(leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,null)")) : emptyMap())
                );
            }
            // "moving" our subscription to another non-existent FK results in an unnecessary tombstone for inner join,
            // since it impossible to know whether the prior FK existed or not (and thus whether any results have
            // previously been emitted)
            // The left join emits a _necessary_ update (since the lhs record has actually changed)
            left.pipeInput("lhs1", "lhsValue1|rhs2", baseTimestamp + 1);
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(mkEntry("lhs1", leftJoin ? "(lhsValue1|rhs2,null)" : null)))
            );
            if (materialized) {
                assertThat(
                    asMap(store),
                    is(leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs2,null)")) : emptyMap())
                );
            }
            // of course, moving it again to yet another non-existent FK has the same effect
            left.pipeInput("lhs1", "lhsValue1|rhs3", baseTimestamp + 2);
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(mkEntry("lhs1", leftJoin ? "(lhsValue1|rhs3,null)" : null)))
            );
            if (materialized) {
                assertThat(
                    asMap(store),
                    is(leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs3,null)")) : emptyMap())
                );
            }

            // Adding an RHS record now, so that we can demonstrate "moving" from a non-existent FK to an existent one
            // This RHS key was previously referenced, but it's not referenced now, so adding this record should
            // result in no changes whatsoever.
            right.pipeInput("rhs1", "rhsValue1", baseTimestamp + 3);
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(emptyMap())
            );
            if (materialized) {
                assertThat(
                    asMap(store),
                    is(leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs3,null)")) : emptyMap())
                );
            }

            // now, we change to a FK that exists, and see the join completes
            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp + 4);
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)")
                ))
            );
            if (materialized) {
                assertThat(
                    asMap(store),
                    is(mkMap(
                        mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)")
                    ))
                );
            }

            // but if we update it again to a non-existent one, we'll get a tombstone for the inner join, and the
            // left join updates appropriately.
            left.pipeInput("lhs1", "lhsValue1|rhs2", baseTimestamp + 5);
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(
                    mkEntry("lhs1", leftJoin ? "(lhsValue1|rhs2,null)" : null)
                ))
            );
            if (materialized) {
                assertThat(
                    asMap(store),
                    is(leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs2,null)")) : emptyMap())
                );
            }
        }
    }

    @Test
    public void shouldUnsubscribeOldForeignKeyIfLeftSideIsUpdated() {
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            // Pre-populate the RHS records. This test is all about what happens when we change LHS records foreign key reference
            // then populate update on RHS
            right.pipeInput("rhs1", "rhsValue1", baseTimestamp);
            right.pipeInput("rhs2", "rhsValue2", baseTimestamp + 1);

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(emptyMap())
            );
            if (materialized) {
                assertThat(
                    asMap(store),
                    is(emptyMap())
                );
            }

            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp + 2);
            {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)")
                );
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(expected)
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(expected)
                    );
                }
            }

            // Change LHS foreign key reference
            left.pipeInput("lhs1", "lhsValue1|rhs2", baseTimestamp + 3);
            {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs2,rhsValue2)")
                );
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(expected)
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(expected)
                    );
                }
            }

            // Populate RHS update on old LHS foreign key ref
            right.pipeInput("rhs1", "rhsValue1Delta", baseTimestamp + 4);
            {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(emptyMap())
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(mkMap(
                            mkEntry("lhs1", "(lhsValue1|rhs2,rhsValue2)")
                        ))
                    );
                }
            }
        }
    }

    @Test
    public void shouldEmitRecordOnNullForeignKeyForLeftJoins() {
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, true, rejoin, leftVersioned, rightVersioned, value -> null);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp);
            {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs1,null)")
                );
                assertThat(outputTopic.readKeyValuesToMap(), is(expected));
                if (materialized) {
                    assertThat(asMap(store), is(expected));
                }
            }
        }
    }

    @Test
    public void shouldEmitRecordWhenOldAndNewFkDiffer() {
        final Function<String, String> foreignKeyExtractor = value -> {
            final String split = value.split("\\|")[1];
            if (split.equals("returnNull")) {
                //new fk
                return null;
            } else {
                //old fk
                return split;
            }
        };
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, true, rejoin, leftVersioned, rightVersioned, foreignKeyExtractor);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");
            final String subscriptionStoreName = driver.getAllStateStores().entrySet().stream()
                .filter(e -> e.getKey().contains("SUBSCRIPTION-STATE-STORE"))
                .findAny().orElseThrow(() -> new RuntimeException("couldn't find store")).getKey();
            final KeyValueStore<Bytes, ValueAndTimestamp<String>> subscriptionStore = driver.getKeyValueStore(subscriptionStoreName);
            final Bytes key = subscriptionStoreKey("lhs1", "rhs1");
            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp);
            {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs1,null)")
                );
                assertThat(outputTopic.readKeyValuesToMap(), is(expected));
                if (materialized) {
                    assertThat(asMap(store), is(expected));
                }
                Assertions.assertNotNull(subscriptionStore.get(key));
            }
            left.pipeInput("lhs1", "lhsValue1|returnNull", baseTimestamp);
            {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1|returnNull,null)")
                );
                assertThat(outputTopic.readKeyValuesToMap(), is(expected));
                if (materialized) {
                    assertThat(asMap(store), is(expected));
                }
                Assertions.assertNull(subscriptionStore.get(key));
            }
        }
    }

    private static Bytes subscriptionStoreKey(final String lhs, final String rhs) {
        final byte[] lhs1bytes = lhs.getBytes();
        final byte[] rhs1bytes = rhs.getBytes();
        final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + lhs1bytes.length + rhs1bytes.length);
        buf.putInt(rhs1bytes.length);
        buf.put(rhs1bytes);
        buf.put(lhs1bytes);
        final Bytes key = Bytes.wrap(buf.array());
        return key;
    }

    protected static Map<String, String> asMap(final KeyValueStore<String, String> store) {
        final HashMap<String, String> result = new HashMap<>();
        store.all().forEachRemaining(kv -> result.put(kv.key, kv.value));
        return result;
    }

    protected static Topology getTopology(final Properties streamsConfig,
                                          final String queryableStoreName,
                                          final boolean leftJoin,
                                          final boolean rejoin,
                                          final boolean leftVersioned,
                                          final boolean rightVersioned) {
        return getTopology(
            streamsConfig,
            queryableStoreName,
            leftJoin,
            rejoin,
            leftVersioned,
            rightVersioned,
            value -> value.split("\\|")[1]
        );
    }

    protected static Topology getTopology(final Properties streamsConfig,
                                          final String queryableStoreName,
                                          final boolean leftJoin,
                                          final boolean rejoin,
                                          final boolean leftVersioned,
                                          final boolean rightVersioned,
                                          final Function<String, String> extractor) {
        final UniqueTopicSerdeScope serdeScope = new UniqueTopicSerdeScope();
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> left;
        if (leftVersioned) {
            left = builder.table(
                LEFT_TABLE,
                Consumed.with(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true),
                    serdeScope.decorateSerde(Serdes.String(), streamsConfig, false)),
                Materialized.as(Stores.persistentVersionedKeyValueStore("left", Duration.ofMinutes(5)))
            );
        } else {
            left = builder.table(
                LEFT_TABLE,
                Consumed.with(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true),
                    serdeScope.decorateSerde(Serdes.String(), streamsConfig, false))
            );
        }

        final KTable<String, String> right;
        if (rightVersioned) {
            right = builder.table(
                RIGHT_TABLE,
                Consumed.with(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true),
                    serdeScope.decorateSerde(Serdes.String(), streamsConfig, false)),
                Materialized.as(Stores.persistentVersionedKeyValueStore("right", Duration.ofMinutes(5)))
            );
        } else {
            right = builder.table(
                RIGHT_TABLE,
                Consumed.with(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true),
                    serdeScope.decorateSerde(Serdes.String(), streamsConfig, false))
            );
        }

        final ValueJoiner<String, String, String> joiner = (value1, value2) -> "(" + value1 + "," + value2 + ")";
        final ValueJoiner<String, String, String> rejoiner = rejoin ? (value1, value2) -> "rejoin(" + value1 + "," + value2 + ")" : null;

        // the cache suppresses some of the unnecessary tombstones we want to make assertions about
        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> mainMaterialized =
            queryableStoreName == null ?
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>with(
                    null,
                    serdeScope.decorateSerde(Serdes.String(), streamsConfig, false)
                ).withCachingDisabled() :
                Materialized.<String, String>as(Stores.inMemoryKeyValueStore(queryableStoreName))
                    .withValueSerde(serdeScope.decorateSerde(Serdes.String(), streamsConfig, false))
                    .withCachingDisabled();

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> rejoinMaterialized =
            !rejoin ? null :
                queryableStoreName == null ?
                    Materialized.with(null, serdeScope.decorateSerde(Serdes.String(), streamsConfig, false)) :
                    // not actually going to query this store, but we need to force materialization here
                    // to really test this configuration
                    Materialized.<String, String>as(Stores.inMemoryKeyValueStore(queryableStoreName + "-rejoin"))
                        .withValueSerde(serdeScope.decorateSerde(Serdes.String(), streamsConfig, false))
                        // the cache suppresses some of the unnecessary tombstones we want to make assertions about
                        .withCachingDisabled();

        if (leftJoin) {
            final KTable<String, String> fkJoin =
                left.leftJoin(right, extractor, joiner, mainMaterialized);

            fkJoin.toStream()
                .to(OUTPUT);

            // also make sure the FK join is set up right for downstream operations that require materialization
            if (rejoin) {
                fkJoin.leftJoin(left, rejoiner, rejoinMaterialized)
                    .toStream()
                    .to(REJOIN_OUTPUT);
            }
        } else {
            final KTable<String, String> fkJoin = left.join(right, extractor, joiner, mainMaterialized);

            fkJoin
                .toStream()
                .to(OUTPUT);

            // also make sure the FK join is set up right for downstream operations that require materialization
            if (rejoin) {
                fkJoin.join(left, rejoiner, rejoinMaterialized)
                    .toStream()
                    .to(REJOIN_OUTPUT);
            }
        }

        return builder.build(streamsConfig);
    }
}
