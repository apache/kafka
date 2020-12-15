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
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
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
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


@RunWith(Parameterized.class)
public class KTableKTableForeignKeyJoinMaterializationIntegrationTest {

    private static final String LEFT_TABLE = "left_table";
    private static final String RIGHT_TABLE = "right_table";
    private static final String OUTPUT = "output-topic";
    private final boolean materialized;
    private final boolean queryable;

    private Properties streamsConfig;

    public KTableKTableForeignKeyJoinMaterializationIntegrationTest(final boolean materialized, final boolean queryable) {
        this.materialized = materialized;
        this.queryable = queryable;
    }

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() {
        final String safeTestName = safeUniqueTestName(getClass(), testName);
        streamsConfig = mkProperties(mkMap(
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        ));
    }


    @Parameterized.Parameters(name = "materialized={0}, queryable={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
            new Object[] {false, false},
            new Object[] {true, false},
            new Object[] {true, true}
        );
    }

    @Test
    public void shouldEmitTombstoneWhenDeletingNonJoiningRecords() {
        final Topology topology = getTopology(streamsConfig, "store");
        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            final TestInputTopic<String, String> left = driver.createInputTopic(LEFT_TABLE, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(OUTPUT, new StringDeserializer(), new StringDeserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore("store");

            left.pipeInput("lhs1", "lhsValue1|rhs1");

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(emptyMap())
            );
            if (materialized && queryable) {
                assertThat(
                    asMap(store),
                    is(emptyMap())
                );
            }

            // Deleting a non-joining record produces an unnecessary tombstone for inner joins, because
            // it's not possible to know whether a result was previously emitted.
            left.pipeInput("lhs1", (String) null);
            {
                if (materialized && queryable) {
                    // in only this specific case, the record cache will actually be activated and
                    // suppress the unnecessary tombstone. This is because the cache is able to determine
                    // for sure that there has never been a previous result. (Because the "old" and "new" values
                    // are both null, and the underlying store is also missing the record in question).
                    assertThat(
                        outputTopic.readKeyValuesToMap(),
                        is(emptyMap())
                    );

                    assertThat(
                        asMap(store),
                        is(emptyMap())
                    );
                } else {
                    assertThat(
                        outputTopic.readKeyValuesToMap(),
                        is(mkMap(mkEntry("lhs1", null)))
                    );
                }
            }

            // Deleting a non-existing record is idempotent
            left.pipeInput("lhs1", (String) null);
            {
                assertThat(
                    outputTopic.readKeyValuesToMap(),
                    is(emptyMap())
                );
                if (materialized && queryable) {
                    assertThat(
                        asMap(store),
                        is(emptyMap())
                    );
                }
            }
        }
    }

    private static Map<String, String> asMap(final KeyValueStore<String, String> store) {
        final HashMap<String, String> result = new HashMap<>();
        store.all().forEachRemaining(kv -> result.put(kv.key, kv.value));
        return result;
    }

    private Topology getTopology(final Properties streamsConfig,
                                 final String queryableStoreName) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> left = builder.table(LEFT_TABLE, Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> right = builder.table(RIGHT_TABLE, Consumed.with(Serdes.String(), Serdes.String()));

        final Function<String, String> extractor = value -> value.split("\\|")[1];
        final ValueJoiner<String, String, String> joiner = (value1, value2) -> "(" + value1 + "," + value2 + ")";

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized;
        if (queryable) {
            materialized = Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(queryableStoreName).withValueSerde(Serdes.String());
        } else {
            materialized = Materialized.with(null, Serdes.String());
        }

        final KTable<String, String> joinResult;
        if (this.materialized) {
            joinResult = left.join(
                right,
                extractor,
                joiner,
                materialized
            );
        } else {
            joinResult = left.join(
                right,
                extractor,
                joiner
            );
        }

        joinResult
            .toStream()
            .to(OUTPUT);

        return builder.build(streamsConfig);
    }
}
