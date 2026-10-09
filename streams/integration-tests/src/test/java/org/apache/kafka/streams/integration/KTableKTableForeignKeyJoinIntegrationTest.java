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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyTestDriverBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.utils.UniqueTopicSerdeScope;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Timeout(600)
public class KTableKTableForeignKeyJoinIntegrationTest {
    protected static final String LEFT_TABLE = "left_table";
    protected static final String RIGHT_TABLE = "right_table";
    protected static final String OUTPUT = "output-topic";
    private static final String REJOIN_OUTPUT = "rejoin-output-topic";

    private final MockTime time = new MockTime();

    protected long baseTimestamp;

    @BeforeEach
    public void before() {
        baseTimestamp = time.milliseconds();
    }

    private static Properties getStreamsProperties(final String optimization, final boolean withHeaders) {
        return getStreamsProperties(optimization, withHeaders, false);
    }

    private static Properties getStreamsProperties(final String optimization, final boolean withHeaders, final boolean transactional) {
        final Properties props = mkProperties(mkMap(
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                mkEntry(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimization)
        ));
        // Transactional state stores (KIP-892) are only supported under exactly-once-v2, so whenever the
        // transactional dimension is enabled we also switch the processing guarantee to exactly-once-v2.
        if (transactional) {
            props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
            props.put(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, true);
        }
        StreamsTestUtils.maybeSetDslStoreFormatHeaders(props, withHeaders);
        return props;
    }

    // versioning is disabled for these tests, even though the code supports building a
    // topology with versioned tables, since KTableKTableForeignKeyVersionedJoinIntegrationTest
    // extends this test class.
    private static Collection<Object[]> data() {
        final List<Boolean> leftJoin = Arrays.asList(true, false);
        final List<String> optimization = Arrays.asList(StreamsConfig.OPTIMIZE, StreamsConfig.NO_OPTIMIZATION);
        final List<Boolean> materialized = Arrays.asList(true, false);
        final List<Boolean> rejoin = Arrays.asList(true, false);
        final List<Boolean> leftVersioned = Collections.singletonList(false);
        final List<Boolean> rightVersioned = Collections.singletonList(false);
        final List<Boolean> withHeaders = Arrays.asList(true, false);
        return buildParameters(leftJoin, optimization, materialized, rejoin, leftVersioned, rightVersioned, withHeaders);
    }

    // optimizations and rejoin are disabled for these tests, as these tests focus on versioning.
    // see KTableKTableForeignKeyJoinIntegrationTest for test coverage for optimizations and rejoin
    private static Collection<Object[]> versionedData() {
        final List<Boolean> leftJoin = Arrays.asList(true, false);
        final List<String> optimization = Collections.singletonList(StreamsConfig.NO_OPTIMIZATION);
        final List<Boolean> materialized = Arrays.asList(true, false);
        final List<Boolean> rejoin = Collections.singletonList(false);
        final List<Boolean> leftVersioned = Arrays.asList(true, false);
        final List<Boolean> rightVersioned = Arrays.asList(true, false);
        final List<Boolean> withHeaders = Arrays.asList(true, false);
        return buildParameters(leftJoin, optimization, materialized, rejoin, leftVersioned, rightVersioned, withHeaders);
    }

    // deduplicate test cases in data and versionedData
    private static Stream<Arguments> testCases() {
        return Stream.concat(data().stream().map(Arrays::asList), versionedData().stream().map(Arrays::asList))
                .collect(Collectors.toSet())
                .stream()
                .map(a -> Arguments.of(a.toArray()));
    }

    // remove first argument: leftJoin and deduplicate test cases
    private static Stream<Arguments> testCasesWithoutLeftJoinArg() {
        return testCases().map(arguments -> Arrays.asList(Arrays.copyOfRange(arguments.get(), 1, arguments.get().length)))
                .collect(Collectors.toSet())
                .stream()
                .map(a -> Arguments.of(a.toArray()));
    }

    private static Stream<Arguments> versionedDataTestCases() {
        return versionedData().stream().map(Arguments::of);
    }

    // Extends the standard testCases() with a transactional dimension (last argument). All existing cases keep
    // transactional=false (preserving current coverage), and we add a small, representative set of
    // transactional=true cases (KIP-892 transactional state stores, which imply exactly-once-v2). To avoid
    // doubling the whole matrix, transactional=true is only added for a single materialization/config
    // combination: materialized (so the queryable store is exercised), non-optimized, non-rejoin, non-versioned,
    // and without DSL store-format headers, for both inner and left joins.
    private static Stream<Arguments> transactionalTestCases() {
        final Stream<Arguments> nonTransactional = testCases()
                .map(arguments -> extend(arguments.get(), false));
        final Stream<Arguments> transactional = Stream.of(true, false)
                .map(leftJoin -> Arguments.of(
                        leftJoin,                       // leftJoin
                        StreamsConfig.NO_OPTIMIZATION,  // optimization
                        true,                           // materialized
                        false,                          // rejoin
                        false,                          // leftVersioned
                        false,                          // rightVersioned
                        false,                          // withHeaders
                        true                            // transactional
                ));
        return Stream.concat(nonTransactional, transactional);
    }

    // Same as transactionalTestCases() but without the leftJoin argument (mirrors testCasesWithoutLeftJoinArg()).
    private static Stream<Arguments> transactionalTestCasesWithoutLeftJoinArg() {
        final Stream<Arguments> nonTransactional = testCasesWithoutLeftJoinArg()
                .map(arguments -> extend(arguments.get(), false));
        final Stream<Arguments> transactional = Stream.of(Arguments.of(
                StreamsConfig.NO_OPTIMIZATION,  // optimization
                true,                           // materialized
                false,                          // rejoin
                false,                          // leftVersioned
                false,                          // rightVersioned
                false,                          // withHeaders
                true                            // transactional
        ));
        return Stream.concat(nonTransactional, transactional);
    }

    private static Arguments extend(final Object[] args, final Object extra) {
        final Object[] extended = Arrays.copyOf(args, args.length + 1);
        extended[args.length] = extra;
        return Arguments.of(extended);
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

    @ParameterizedTest
    @MethodSource("transactionalTestCases")
    public void doJoinFromLeftThenDeleteLeftEntity(final boolean leftJoin,
                                                   final String optimization,
                                                   final boolean materialized,
                                                   final boolean rejoin,
                                                   final boolean leftVersioned,
                                                   final boolean rightVersioned,
                                                   final boolean withHeaders,
                                                   final boolean transactional) {
        final Properties streamsConfig = getStreamsProperties(optimization, withHeaders, transactional);
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(topology).withConfig(streamsConfig).build()) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final TestOutputTopic<String, String> rejoinOutputTopic = rejoin ? driver.createOutputTopic(REJOIN_OUTPUT, new StringDeserializer(), new StringDeserializer()) : null;
            final KeyValueStore<String, ValueAndTimestamp<String>> store = driver.getTimestampedKeyValueStore("store");

            // Pre-populate the RHS records. This test is all about what happens when we add/remove LHS records
            right.pipeInput("rhs1", "rhsValue1", baseTimestamp);
            right.pipeInput("rhs2", "rhsValue2", baseTimestamp + 1);
            right.pipeInput("rhs3", "rhsValue3", baseTimestamp + 2); // this unreferenced FK won't show up in any results

            assertTrue(outputTopic.readKeyValuesToList().isEmpty());
            if (rejoin) {
                assertTrue(rejoinOutputTopic.readKeyValuesToList().isEmpty());
            }
            if (materialized) {
                assertTrue(asMap(store).isEmpty());
            }

            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp + 3);
            left.pipeInput("lhs2", "lhsValue2|rhs2", baseTimestamp + 4);

            {
                final List<KeyValue<String, String>> expected = Arrays.asList(
                    KeyValue.pair("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                    KeyValue.pair("lhs2", "(lhsValue2|rhs2,rhsValue2)")
                );
                assertEquals(expected, outputTopic.readKeyValuesToList());
                if (rejoin) {
                    assertEquals(
                        List.of(
                            KeyValue.pair("lhs1", "rejoin((lhsValue1|rhs1,rhsValue1),lhsValue1|rhs1)"),
                            KeyValue.pair("lhs2", "rejoin((lhsValue2|rhs2,rhsValue2),lhsValue2|rhs2)")),
                        rejoinOutputTopic.readKeyValuesToList());
                }
                if (materialized) {
                    assertEquals(expected.stream().collect(Collectors.toMap(kv -> kv.key, kv -> kv.value)), asMap(store));
                }
            }

            // Add another reference to an existing FK
            left.pipeInput("lhs3", "lhsValue3|rhs1", baseTimestamp + 5);
            {
                assertEquals(List.of(new KeyValue<>("lhs3", "(lhsValue3|rhs1,rhsValue1)")), outputTopic.readKeyValuesToList());
                if (rejoin) {
                    assertEquals(List.of(new KeyValue<>("lhs3", "rejoin((lhsValue3|rhs1,rhsValue1),lhsValue3|rhs1)")),
                        rejoinOutputTopic.readKeyValuesToList());
                }
                if (materialized) {
                    assertEquals(
                        Map.of(
                            "lhs1", "(lhsValue1|rhs1,rhsValue1)",
                            "lhs2", "(lhsValue2|rhs2,rhsValue2)",
                            "lhs3", "(lhsValue3|rhs1,rhsValue1)"),
                        asMap(store));
                }
            }

            // Now delete one LHS entity such that one delete is propagated down to the output.

            left.pipeInput("lhs1", null, baseTimestamp + 6);
            assertEquals(List.of(new KeyValue<>("lhs1", null)), outputTopic.readKeyValuesToList());
            if (rejoin) {
                assertTrue(rejoinOutputTopic.readKeyValuesToList().contains(KeyValue.pair("lhs1", null)));
            }
            if (materialized) {
                assertEquals(
                    Map.of(
                        "lhs2", "(lhsValue2|rhs2,rhsValue2)",
                        "lhs3", "(lhsValue3|rhs1,rhsValue1)"),
                    asMap(store));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void doJoinFromLeftThenUpdateFkThenRevertBack(final boolean leftJoin,
                                                         final String optimization,
                                                         final boolean materialized,
                                                         final boolean rejoin,
                                                         final boolean leftVersioned,
                                                         final boolean rightVersioned,
                                                         final boolean withHeaders) {
        final Properties streamsConfig = getStreamsProperties(optimization, withHeaders);
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(topology).withConfig(streamsConfig).build()) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final TestOutputTopic<String, String> rejoinOutputTopic = rejoin ? driver.createOutputTopic(REJOIN_OUTPUT, new StringDeserializer(), new StringDeserializer()) : null;
            final KeyValueStore<String, ValueAndTimestamp<String>> store = driver.getTimestampedKeyValueStore("store");

            // Pre-populate the RHS records. This test is all about what happens when we add/remove LHS records
            right.pipeInput("rhs1", "rhsValue1", baseTimestamp);
            right.pipeInput("rhs2", "rhsValue2", baseTimestamp + 1);

            assertTrue(outputTopic.readKeyValuesToList().isEmpty());
            if (rejoin) {
                assertTrue(rejoinOutputTopic.readKeyValuesToList().isEmpty());
            }
            if (materialized) {
                assertTrue(asMap(store).isEmpty());
            }

            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp + 3);

            {
                final List<KeyValue<String, String>> expected = asList(
                    KeyValue.pair("lhs1", "(lhsValue1|rhs1,rhsValue1)")
                );
                assertEquals(expected, outputTopic.readKeyValuesToList());
            }

            // Add another reference to an existing FK
            left.pipeInput("lhs1", "lhsValue1|rhs2", baseTimestamp + 5);
            {
                assertEquals(List.of(new KeyValue<>("lhs1", "(lhsValue1|rhs2,rhsValue2)")), outputTopic.readKeyValuesToList());
            }

            // Now revert back the foreign key to earlier reference

            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp + 6);
            assertEquals(List.of(new KeyValue<>("lhs1", "(lhsValue1|rhs1,rhsValue1)")), outputTopic.readKeyValuesToList());
        }
    }

    @ParameterizedTest
    @MethodSource("transactionalTestCases")
    public void doJoinFromRightThenDeleteRightEntity(final boolean leftJoin,
                                                     final String optimization,
                                                     final boolean materialized,
                                                     final boolean rejoin,
                                                     final boolean leftVersioned,
                                                     final boolean rightVersioned,
                                                     final boolean withHeaders,
                                                     final boolean transactional) {
        final Properties streamsConfig = getStreamsProperties(optimization, withHeaders, transactional);
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(topology).withConfig(streamsConfig).build()) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, ValueAndTimestamp<String>> store = driver.getTimestampedKeyValueStore("store");

            // Pre-populate the LHS records. This test is all about what happens when we add/remove RHS records
            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp);
            left.pipeInput("lhs2", "lhsValue2|rhs2", baseTimestamp + 1);
            left.pipeInput("lhs3", "lhsValue3|rhs1", baseTimestamp + 2);

            assertEquals(
                leftJoin ? Map.of(
                    "lhs1", "(lhsValue1|rhs1,null)",
                    "lhs2", "(lhsValue2|rhs2,null)",
                    "lhs3", "(lhsValue3|rhs1,null)")
                    : Map.of(),
                outputTopic.readKeyValuesToMap());
            if (materialized) {
                assertEquals(
                    leftJoin ? Map.of(
                        "lhs1", "(lhsValue1|rhs1,null)",
                        "lhs2", "(lhsValue2|rhs2,null)",
                        "lhs3", "(lhsValue3|rhs1,null)")
                        : Map.of(),
                    asMap(store));
            }

            right.pipeInput("rhs1", "rhsValue1", baseTimestamp + 3);

            assertEquals(
                Map.of(
                    "lhs1", "(lhsValue1|rhs1,rhsValue1)",
                    "lhs3", "(lhsValue3|rhs1,rhsValue1)"),
                outputTopic.readKeyValuesToMap());
            if (materialized) {
                assertEquals(
                    leftJoin ? Map.of(
                        "lhs1", "(lhsValue1|rhs1,rhsValue1)",
                        "lhs2", "(lhsValue2|rhs2,null)",
                        "lhs3", "(lhsValue3|rhs1,rhsValue1)")
                        : Map.of(
                            "lhs1", "(lhsValue1|rhs1,rhsValue1)",
                            "lhs3", "(lhsValue3|rhs1,rhsValue1)"),
                    asMap(store));
            }

            right.pipeInput("rhs2", "rhsValue2", baseTimestamp + 4);

            assertEquals(Map.of("lhs2", "(lhsValue2|rhs2,rhsValue2)"), outputTopic.readKeyValuesToMap());
            if (materialized) {
                assertEquals(
                    Map.of(
                        "lhs1", "(lhsValue1|rhs1,rhsValue1)",
                        "lhs2", "(lhsValue2|rhs2,rhsValue2)",
                        "lhs3", "(lhsValue3|rhs1,rhsValue1)"),
                    asMap(store));
            }

            right.pipeInput("rhs3", "rhsValue3", baseTimestamp + 5); // this unreferenced FK won't show up in any results

            assertTrue(outputTopic.readKeyValuesToMap().isEmpty());
            if (materialized) {
                assertEquals(
                    Map.of(
                        "lhs1", "(lhsValue1|rhs1,rhsValue1)",
                        "lhs2", "(lhsValue2|rhs2,rhsValue2)",
                        "lhs3", "(lhsValue3|rhs1,rhsValue1)"),
                    asMap(store));
            }

            // Now delete the RHS entity such that all matching keys have deletes propagated.
            right.pipeInput("rhs1", null, baseTimestamp + 6);

            assertEquals(
                mkMap(
                    mkEntry("lhs1", leftJoin ? "(lhsValue1|rhs1,null)" : null),
                    mkEntry("lhs3", leftJoin ? "(lhsValue3|rhs1,null)" : null)),
                outputTopic.readKeyValuesToMap());
            if (materialized) {
                assertEquals(
                    leftJoin ? Map.of(
                        "lhs1", "(lhsValue1|rhs1,null)",
                        "lhs2", "(lhsValue2|rhs2,rhsValue2)",
                        "lhs3", "(lhsValue3|rhs1,null)")
                        : Map.of("lhs2", "(lhsValue2|rhs2,rhsValue2)"),
                    asMap(store));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void shouldEmitTombstoneWhenDeletingNonJoiningRecords(final boolean leftJoin,
                                                                 final String optimization,
                                                                 final boolean materialized,
                                                                 final boolean rejoin,
                                                                 final boolean leftVersioned,
                                                                 final boolean rightVersioned,
                                                                 final boolean withHeaders) {
        final Properties streamsConfig = getStreamsProperties(optimization, withHeaders);
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(topology).withConfig(streamsConfig).build()) {
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, ValueAndTimestamp<String>> store = driver.getTimestampedKeyValueStore("store");

            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp);

            {
                final Map<String, String> expected =
                    leftJoin ? mkMap(mkEntry("lhs1", "(lhsValue1|rhs1,null)")) : emptyMap();
                assertEquals(expected, outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertEquals(expected, asMap(store));
                }
            }

            // Deleting a non-joining record produces an unnecessary tombstone for inner joins, because
            // it's not possible to know whether a result was previously emitted.
            // For the left join, the tombstone is necessary.
            left.pipeInput("lhs1", null, baseTimestamp + 1);
            {
                assertEquals(mkMap(mkEntry("lhs1", null)), outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertTrue(asMap(store).isEmpty());
                }
            }

            // Deleting a non-existing record is idempotent
            left.pipeInput("lhs1", null, baseTimestamp + 2);
            {
                assertTrue(outputTopic.readKeyValuesToMap().isEmpty());
                if (materialized) {
                    assertTrue(asMap(store).isEmpty());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void shouldNotEmitTombstonesWhenDeletingNonExistingRecords(final boolean leftJoin,
                                                                      final String optimization,
                                                                      final boolean materialized,
                                                                      final boolean rejoin,
                                                                      final boolean leftVersioned,
                                                                      final boolean rightVersioned,
                                                                      final boolean withHeaders) {
        final Properties streamsConfig = getStreamsProperties(optimization, withHeaders);
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(topology).withConfig(streamsConfig).build()) {
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, ValueAndTimestamp<String>> store = driver.getTimestampedKeyValueStore("store");

            // Deleting a record that never existed doesn't need to emit tombstones.
            left.pipeInput("lhs1", null, baseTimestamp);
            {
                assertTrue(outputTopic.readKeyValuesToMap().isEmpty());
                if (materialized) {
                    assertTrue(asMap(store).isEmpty());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void joinShouldProduceNullsWhenValueHasNonMatchingForeignKey(final boolean leftJoin,
                                                                        final String optimization,
                                                                        final boolean materialized,
                                                                        final boolean rejoin,
                                                                        final boolean leftVersioned,
                                                                        final boolean rightVersioned,
                                                                        final boolean withHeaders) {
        final Properties streamsConfig = getStreamsProperties(optimization, withHeaders);
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(topology).withConfig(streamsConfig).build()) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, ValueAndTimestamp<String>> store = driver.getTimestampedKeyValueStore("store");

            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp);
            // no output for a new inner join on a non-existent FK
            // the left join of course emits the half-joined output
            assertEquals(leftJoin ? Map.of("lhs1", "(lhsValue1|rhs1,null)") : Map.of(), outputTopic.readKeyValuesToMap());
            if (materialized) {
                assertEquals(leftJoin ? Map.of("lhs1", "(lhsValue1|rhs1,null)") : Map.of(), asMap(store));
            }
            // "moving" our subscription to another non-existent FK results in an unnecessary tombstone for inner join,
            // since it impossible to know whether the prior FK existed or not (and thus whether any results have
            // previously been emitted)
            // The left join emits a _necessary_ update (since the lhs record has actually changed)
            left.pipeInput("lhs1", "lhsValue1|rhs2", baseTimestamp + 1);
            assertEquals(mkMap(mkEntry("lhs1", leftJoin ? "(lhsValue1|rhs2,null)" : null)), outputTopic.readKeyValuesToMap());
            if (materialized) {
                assertEquals(leftJoin ? Map.of("lhs1", "(lhsValue1|rhs2,null)") : Map.of(), asMap(store));
            }
            // of course, moving it again to yet another non-existent FK has the same effect
            left.pipeInput("lhs1", "lhsValue1|rhs3", baseTimestamp + 2);
            assertEquals(mkMap(mkEntry("lhs1", leftJoin ? "(lhsValue1|rhs3,null)" : null)), outputTopic.readKeyValuesToMap());
            if (materialized) {
                assertEquals(leftJoin ? Map.of("lhs1", "(lhsValue1|rhs3,null)") : Map.of(), asMap(store));
            }

            // Adding an RHS record now, so that we can demonstrate "moving" from a non-existent FK to an existent one
            // This RHS key was previously referenced, but it's not referenced now, so adding this record should
            // result in no changes whatsoever.
            right.pipeInput("rhs1", "rhsValue1", baseTimestamp + 3);
            assertTrue(outputTopic.readKeyValuesToMap().isEmpty());
            if (materialized) {
                assertEquals(leftJoin ? Map.of("lhs1", "(lhsValue1|rhs3,null)") : Map.of(), asMap(store));
            }

            // now, we change to a FK that exists, and see the join completes
            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp + 4);
            assertEquals(Map.of("lhs1", "(lhsValue1|rhs1,rhsValue1)"), outputTopic.readKeyValuesToMap());
            if (materialized) {
                assertEquals(Map.of("lhs1", "(lhsValue1|rhs1,rhsValue1)"), asMap(store));
            }

            // but if we update it again to a non-existent one, we'll get a tombstone for the inner join, and the
            // left join updates appropriately.
            left.pipeInput("lhs1", "lhsValue1|rhs2", baseTimestamp + 5);
            assertEquals(mkMap(mkEntry("lhs1", leftJoin ? "(lhsValue1|rhs2,null)" : null)), outputTopic.readKeyValuesToMap());
            if (materialized) {
                assertEquals(leftJoin ? Map.of("lhs1", "(lhsValue1|rhs2,null)") : Map.of(), asMap(store));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testCases")
    public void shouldUnsubscribeOldForeignKeyIfLeftSideIsUpdated(final boolean leftJoin,
                                                                  final String optimization,
                                                                  final boolean materialized,
                                                                  final boolean rejoin,
                                                                  final boolean leftVersioned,
                                                                  final boolean rightVersioned,
                                                                  final boolean withHeaders) {
        final Properties streamsConfig = getStreamsProperties(optimization, withHeaders);
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(topology).withConfig(streamsConfig).build()) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, ValueAndTimestamp<String>> store = driver.getTimestampedKeyValueStore("store");

            // Pre-populate the RHS records. This test is all about what happens when we change LHS records foreign key reference
            // then populate update on RHS
            right.pipeInput("rhs1", "rhsValue1", baseTimestamp);
            right.pipeInput("rhs2", "rhsValue2", baseTimestamp + 1);

            assertTrue(outputTopic.readKeyValuesToMap().isEmpty());
            if (materialized) {
                assertTrue(asMap(store).isEmpty());
            }

            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp + 2);
            {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)")
                );
                assertEquals(expected, outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertEquals(expected, asMap(store));
                }
            }

            // Change LHS foreign key reference
            left.pipeInput("lhs1", "lhsValue1|rhs2", baseTimestamp + 3);
            {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs2,rhsValue2)")
                );
                assertEquals(expected, outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertEquals(expected, asMap(store));
                }
            }

            // Populate RHS update on old LHS foreign key ref
            right.pipeInput("rhs1", "rhsValue1Delta", baseTimestamp + 4);
            {
                assertTrue(outputTopic.readKeyValuesToMap().isEmpty());
                if (materialized) {
                    assertEquals(Map.of("lhs1", "(lhsValue1|rhs2,rhsValue2)"), asMap(store));
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testCasesWithoutLeftJoinArg")
    public void shouldEmitRecordOnNullForeignKeyForLeftJoins(final String optimization,
                                                             final boolean materialized,
                                                             final boolean rejoin,
                                                             final boolean leftVersioned,
                                                             final boolean rightVersioned,
                                                             final boolean withHeaders) {
        final Properties streamsConfig = getStreamsProperties(optimization, withHeaders);
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, true, rejoin, leftVersioned, rightVersioned, value -> null);
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(topology).withConfig(streamsConfig).build()) {
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, ValueAndTimestamp<String>> store = driver.getTimestampedKeyValueStore("store");

            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp);
            {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs1,null)")
                );
                assertEquals(expected, outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertEquals(expected, asMap(store));
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("transactionalTestCasesWithoutLeftJoinArg")
    public void shouldEmitRecordWhenOldAndNewFkDiffer(final String optimization,
                                                      final boolean materialized,
                                                      final boolean rejoin,
                                                      final boolean leftVersioned,
                                                      final boolean rightVersioned,
                                                      final boolean withHeaders,
                                                      final boolean transactional) {
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
        final Properties streamsConfig = getStreamsProperties(optimization, withHeaders, transactional);
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, true, rejoin, leftVersioned, rightVersioned, foreignKeyExtractor);
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(topology).withConfig(streamsConfig).build()) {
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, ValueAndTimestamp<String>> store = driver.getTimestampedKeyValueStore("store");
            final String subscriptionStoreName = driver.getAllStateStores().entrySet().stream()
                .filter(e -> e.getKey().contains("SUBSCRIPTION-STATE-STORE"))
                .findAny().orElseThrow(() -> new RuntimeException("couldn't find store")).getKey();
            final KeyValueStore<Bytes, ValueAndTimestamp<String>> subscriptionStore = driver.getTimestampedKeyValueStore(subscriptionStoreName);
            final Bytes key = subscriptionStoreKey("lhs1", "rhs1");
            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp);
            {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs1,null)")
                );
                assertEquals(expected, outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertEquals(expected, asMap(store));
                }
                Assertions.assertNotNull(subscriptionStore.get(key));
            }
            left.pipeInput("lhs1", "lhsValue1|returnNull", baseTimestamp);
            {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1|returnNull,null)")
                );
                assertEquals(expected, outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertEquals(expected, asMap(store));
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

    protected static Map<String, String> asMap(final KeyValueStore<String, ValueAndTimestamp<String>> store) {
        final HashMap<String, String> result = new HashMap<>();
        try (final KeyValueIterator<String, ValueAndTimestamp<String>> it = store.all()) {
            it.forEachRemaining(kv -> result.put(kv.key, kv.value.value()));
        }
        return result;
    }

    protected static List<KeyValue<String, String>> makeList(final KeyValueStore<String, ValueAndTimestamp<String>> store) {
        final List<KeyValue<String, String>> result = new LinkedList<>();
        store.all().forEachRemaining(ele -> result.add(new KeyValue<>(ele.key, ele.value.value())));
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
            value -> {
                final String[] tokens = value.split("\\|");
                return tokens.length == 2 ? tokens[1] : null;
            }
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

    @ParameterizedTest
    @MethodSource("versionedDataTestCases")
    public void shouldIgnoreOutOfOrderRecordsIffVersioned(final boolean leftJoin,
                                                          final String optimization,
                                                          final boolean materialized,
                                                          final boolean rejoin,
                                                          final boolean leftVersioned,
                                                          final boolean rightVersioned,
                                                          final boolean withHeaders) {
        final Properties streamsConfig = getStreamsProperties(optimization, withHeaders);
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(topology).withConfig(streamsConfig).build()) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, ValueAndTimestamp<String>> store = driver.getTimestampedKeyValueStore("store");

            // RHS record
            right.pipeInput("rhs1", "rhsValue1", baseTimestamp + 4);

            assertTrue(outputTopic.readKeyValuesToMap().isEmpty());
            if (materialized) {
                assertTrue(asMap(store).isEmpty());
            }

            // LHS records with match to existing RHS record
            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp + 3);
            left.pipeInput("lhs2", "lhsValue2|rhs1", baseTimestamp + 5);
            {
                final Map<String, String> expected = mkMap(
                        mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                        mkEntry("lhs2", "(lhsValue2|rhs1,rhsValue1)")
                );
                assertEquals(expected, outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertEquals(expected, asMap(store));
                }
            }

            // replace with tombstone, to validate behavior when latest record is null
            left.pipeInput("lhs2", null, baseTimestamp + 6);
            {
                assertEquals(mkMap(mkEntry("lhs2", null)), outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertEquals(Map.of("lhs1", "(lhsValue1|rhs1,rhsValue1)"), asMap(store));
                }
            }

            // out-of-order LHS record (for existing key) does not produce a new result iff LHS is versioned
            left.pipeInput("lhs1", "lhsValue1_ooo|rhs1", baseTimestamp + 2);
            left.pipeInput("lhs2", "lhsValue2_ooo|rhs1", baseTimestamp + 2);
            if (leftVersioned) {
                assertTrue(outputTopic.readKeyValuesToMap().isEmpty());
                if (materialized) {
                    assertEquals(Map.of("lhs1", "(lhsValue1|rhs1,rhsValue1)"), asMap(store));
                }
            } else {
                final Map<String, String> expected = mkMap(
                        mkEntry("lhs1", "(lhsValue1_ooo|rhs1,rhsValue1)"),
                        mkEntry("lhs2", "(lhsValue2_ooo|rhs1,rhsValue1)")
                );
                assertEquals(expected, outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertEquals(expected, asMap(store));
                }
            }

            // out-of-order LHS tombstone (for existing key) is similarly ignored (iff LHS is versioned)
            left.pipeInput("lhs1", null, baseTimestamp + 2);
            if (leftVersioned) {
                assertTrue(outputTopic.readKeyValuesToMap().isEmpty());
                if (materialized) {
                    assertEquals(Map.of("lhs1", "(lhsValue1|rhs1,rhsValue1)"), asMap(store));
                }
            } else {
                assertEquals(mkMap(mkEntry("lhs1", null)), outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertEquals(Map.of("lhs2", "(lhsValue2_ooo|rhs1,rhsValue1)"), asMap(store));
                }
            }

            // LHS record with larger timestamp always produces a new result
            left.pipeInput("lhs1", "lhsValue1_new|rhs1", baseTimestamp + 8);
            left.pipeInput("lhs2", "lhsValue2_new|rhs1", baseTimestamp + 8);
            {
                final Map<String, String> expected = mkMap(
                        mkEntry("lhs1", "(lhsValue1_new|rhs1,rhsValue1)"),
                        mkEntry("lhs2", "(lhsValue2_new|rhs1,rhsValue1)")
                );
                assertEquals(expected, outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertEquals(expected, asMap(store));
                }
            }

            // out-of-order RHS record (for existing key) does not produce a new result iff RHS is versioned
            right.pipeInput("rhs1", "rhsValue1_ooo", baseTimestamp + 1);
            if (rightVersioned) {
                assertTrue(outputTopic.readKeyValuesToMap().isEmpty());
                if (materialized) {
                    assertEquals(
                        Map.of(
                            "lhs1", "(lhsValue1_new|rhs1,rhsValue1)",
                            "lhs2", "(lhsValue2_new|rhs1,rhsValue1)"),
                        asMap(store));
                }
            } else {
                assertEquals(
                    Map.of(
                        "lhs1", "(lhsValue1_new|rhs1,rhsValue1_ooo)",
                        "lhs2", "(lhsValue2_new|rhs1,rhsValue1_ooo)"),
                    outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertEquals(
                        Map.of(
                            "lhs1", "(lhsValue1_new|rhs1,rhsValue1_ooo)",
                            "lhs2", "(lhsValue2_new|rhs1,rhsValue1_ooo)"),
                        asMap(store));
                }
            }

            // out-of-order RHS tombstone (for existing key) is similarly ignored (iff RHS is versioned)
            right.pipeInput("rhs1", null, baseTimestamp + 1);
            if (rightVersioned) {
                assertTrue(outputTopic.readKeyValuesToMap().isEmpty());
                if (materialized) {
                    assertEquals(
                        Map.of(
                            "lhs1", "(lhsValue1_new|rhs1,rhsValue1)",
                            "lhs2", "(lhsValue2_new|rhs1,rhsValue1)"),
                        asMap(store));
                }
            } else {
                if (leftJoin) {
                    assertEquals(
                        Map.of(
                            "lhs1", "(lhsValue1_new|rhs1,null)",
                            "lhs2", "(lhsValue2_new|rhs1,null)"),
                        outputTopic.readKeyValuesToMap());
                    if (materialized) {
                        assertEquals(
                            Map.of(
                                "lhs1", "(lhsValue1_new|rhs1,null)",
                                "lhs2", "(lhsValue2_new|rhs1,null)"),
                            asMap(store));
                    }
                } else {
                    assertEquals(
                        mkMap(
                            mkEntry("lhs1", null),
                            mkEntry("lhs2", null)),
                        outputTopic.readKeyValuesToMap());
                    if (materialized) {
                        assertTrue(asMap(store).isEmpty());
                    }
                }
            }

            // RHS record with larger timestamps always produces new results
            right.pipeInput("rhs1", "rhsValue1_new", baseTimestamp + 6);
            {
                assertEquals(
                    Map.of(
                        "lhs1", "(lhsValue1_new|rhs1,rhsValue1_new)",
                        "lhs2", "(lhsValue2_new|rhs1,rhsValue1_new)"),
                    outputTopic.readKeyValuesToMap());
                if (materialized) {
                    assertEquals(
                        Map.of(
                            "lhs1", "(lhsValue1_new|rhs1,rhsValue1_new)",
                            "lhs2", "(lhsValue2_new|rhs1,rhsValue1_new)"),
                        asMap(store));
                }
            }
        }
    }
}
