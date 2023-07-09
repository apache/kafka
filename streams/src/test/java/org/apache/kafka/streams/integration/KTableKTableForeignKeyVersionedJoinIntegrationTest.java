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

import static java.util.Collections.emptyMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category(IntegrationTest.class)
public class KTableKTableForeignKeyVersionedJoinIntegrationTest extends KTableKTableForeignKeyJoinIntegrationTest {

    public KTableKTableForeignKeyVersionedJoinIntegrationTest(final boolean leftJoin,
                                                              final boolean materialized,
                                                              final boolean leftVersioned,
                                                              final boolean rightVersioned) {
        // optimizations and rejoin are disabled for these tests, as these tests focus on versioning.
        // see KTableKTableForeignKeyJoinIntegrationTest for test coverage for optimizations and rejoin
        super(leftJoin, StreamsConfig.NO_OPTIMIZATION, materialized, false, leftVersioned, rightVersioned);
    }

    @Parameterized.Parameters(name = "leftJoin={0}, materialized={1}, leftVersioned={2}, rightVersioned={3}")
    public static Collection<Object[]> data() {
        final List<Boolean> booleans = Arrays.asList(true, false);
        return buildParameters(booleans, booleans, booleans, booleans);
    }

    @Test
    public void shouldIgnoreOutOfOrderRecordsIffVersioned() {
        final Topology topology = getTopology(streamsConfig, materialized ? "store" : null, leftJoin, rejoin, leftVersioned, rightVersioned);
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> right = driver.createInputTopic(RIGHT_TABLE, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            // RHS record
            right.pipeInput("rhs1", "rhsValue1", baseTimestamp + 4);

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

            // LHS records with match to existing RHS record
            left.pipeInput("lhs1", "lhsValue1|rhs1", baseTimestamp + 3);
            left.pipeInput("lhs2", "lhsValue2|rhs1", baseTimestamp + 5);
            {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                    mkEntry("lhs2", "(lhsValue2|rhs1,rhsValue1)")
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

            // replace with tombstone, to validate behavior when latest record is null
            left.pipeInput("lhs2", null, baseTimestamp + 6);
            {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(mkMap(
                        mkEntry("lhs2", null)
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
            }

            // out-of-order LHS record (for existing key) does not produce a new result iff LHS is versioned
            left.pipeInput("lhs1", "lhsValue1_ooo|rhs1", baseTimestamp + 2);
            left.pipeInput("lhs2", "lhsValue2_ooo|rhs1", baseTimestamp + 2);
            if (leftVersioned) {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(emptyMap())
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(mkMap(
                            mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)")
                        ))
                    );
                }
            } else {
                final Map<String, String> expected = mkMap(
                    mkEntry("lhs1", "(lhsValue1_ooo|rhs1,rhsValue1)"),
                    mkEntry("lhs2", "(lhsValue2_ooo|rhs1,rhsValue1)")
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

            // out-of-order LHS tombstone (for existing key) is similarly ignored (iff LHS is versioned)
            left.pipeInput("lhs1", null, baseTimestamp + 2);
            if (leftVersioned) {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(emptyMap())
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(mkMap(
                            mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)")
                        ))
                    );
                }
            } else {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(mkMap(
                        mkEntry("lhs1", null)
                    ))
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(mkMap(
                            mkEntry("lhs2", "(lhsValue2_ooo|rhs1,rhsValue1)")
                        ))
                    );
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

            // out-of-order RHS record (for existing key) does not produce a new result iff RHS is versioned
            right.pipeInput("rhs1", "rhsValue1_ooo", baseTimestamp + 1);
            if (rightVersioned) {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(emptyMap())
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(mkMap(
                            mkEntry("lhs1", "(lhsValue1_new|rhs1,rhsValue1)"),
                            mkEntry("lhs2", "(lhsValue2_new|rhs1,rhsValue1)")
                        ))
                    );
                }
            } else {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(mkMap(
                        mkEntry("lhs1", "(lhsValue1_new|rhs1,rhsValue1_ooo)"),
                        mkEntry("lhs2", "(lhsValue2_new|rhs1,rhsValue1_ooo)")
                    ))
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(mkMap(
                            mkEntry("lhs1", "(lhsValue1_new|rhs1,rhsValue1_ooo)"),
                            mkEntry("lhs2", "(lhsValue2_new|rhs1,rhsValue1_ooo)")
                        ))
                    );
                }
            }

            // out-of-order RHS tombstone (for existing key) is similarly ignored (iff RHS is versioned)
            right.pipeInput("rhs1", null, baseTimestamp + 1);
            if (rightVersioned) {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(emptyMap())
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(mkMap(
                            mkEntry("lhs1", "(lhsValue1_new|rhs1,rhsValue1)"),
                            mkEntry("lhs2", "(lhsValue2_new|rhs1,rhsValue1)")
                        ))
                    );
                }
            } else {
                if (leftJoin) {
                    assertThat(
                        outputTopic.readKeyValuesToMap(),
                        is(mkMap(
                            mkEntry("lhs1", "(lhsValue1_new|rhs1,null)"),
                            mkEntry("lhs2", "(lhsValue2_new|rhs1,null)")
                        ))
                    );
                    if (materialized) {
                        assertThat(
                            asMap(store),
                            is(mkMap(
                                mkEntry("lhs1", "(lhsValue1_new|rhs1,null)"),
                                mkEntry("lhs2", "(lhsValue2_new|rhs1,null)")
                            ))
                        );
                    }
                } else {
                    assertThat(
                        outputTopic.readKeyValuesToMap(),
                        is(mkMap(
                            mkEntry("lhs1", null),
                            mkEntry("lhs2", null)
                        ))
                    );
                    if (materialized) {
                        assertThat(
                            asMap(store),
                            is(emptyMap())
                        );
                    }
                }
            }

            // RHS record with larger timestamps always produces new results
            right.pipeInput("rhs1", "rhsValue1_new", baseTimestamp + 6);
            {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(mkMap(
                        mkEntry("lhs1", "(lhsValue1_new|rhs1,rhsValue1_new)"),
                        mkEntry("lhs2", "(lhsValue2_new|rhs1,rhsValue1_new)")
                    ))
                );
                if (materialized) {
                    assertThat(
                        asMap(store),
                        is(mkMap(
                            mkEntry("lhs1", "(lhsValue1_new|rhs1,rhsValue1_new)"),
                            mkEntry("lhs2", "(lhsValue2_new|rhs1,rhsValue1_new)")
                        ))
                    );
                }
            }
        }
    }
}
