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
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

@Tag("integration")
@Timeout(600)
public class KTableKTableForeignKeyJoinMaterializationIntegrationTest {
    private static final String LEFT_TABLE = "left_table";
    private static final String RIGHT_TABLE = "right_table";
    private static final String OUTPUT = "output-topic";

    private Properties streamsConfig;

    @BeforeEach
    public void before() {
        streamsConfig = mkProperties(mkMap(
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        ));
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "true, true"})
    public void shouldEmitTombstoneWhenDeletingNonJoiningRecords(final boolean materialized, final boolean queryable) {
        final Topology topology = getTopology(streamsConfig, "store", materialized, queryable);
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
                                 final String queryableStoreName,
                                 final boolean materialized,
                                 final boolean queryable) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> left = builder.table(LEFT_TABLE, Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> right = builder.table(RIGHT_TABLE, Consumed.with(Serdes.String(), Serdes.String()));

        final Function<String, String> extractor = value -> value.split("\\|")[1];
        final ValueJoiner<String, String, String> joiner = (value1, value2) -> "(" + value1 + "," + value2 + ")";

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materializedStore;
        if (queryable) {
            materializedStore = Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(queryableStoreName).withValueSerde(Serdes.String());
        } else {
            materializedStore = Materialized.with(null, Serdes.String());
        }

        final KTable<String, String> joinResult;
        if (materialized) {
            joinResult = left.join(
                right,
                extractor,
                joiner,
                materializedStore
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
            .to(OUTPUT, Produced.with(null, Serdes.String()));

        return builder.build(streamsConfig);
    }
}
