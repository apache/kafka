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
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class KTableKTableForeignKeyJoinDefaultSerdeTest {
    @Test
    public void shouldWorkWithDefaultSerdes() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, String> aTable = builder.table("A");
        final KTable<String, String> bTable = builder.table("B");

        final KTable<String, String> fkJoinResult = aTable.join(
            bTable,
            value -> value.split("-")[0],
            (aVal, bVal) -> "(" + aVal + "," + bVal + ")",
            Materialized.as("asdf")
        );

        final KTable<String, String> finalJoinResult = aTable.join(
            fkJoinResult,
            (aVal, fkJoinVal) -> "(" + aVal + "," + fkJoinVal + ")"
        );

        finalJoinResult.toStream().to("output");

        validateTopologyCanProcessData(builder);
    }

    @Test
    public void shouldWorkWithDefaultAndConsumedSerdes() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, String> aTable = builder.table("A", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> bTable = builder.table("B");

        final KTable<String, String> fkJoinResult = aTable.join(
            bTable,
            value -> value.split("-")[0],
            (aVal, bVal) -> "(" + aVal + "," + bVal + ")",
            Materialized.as("asdf")
        );

        final KTable<String, String> finalJoinResult = aTable.join(
            fkJoinResult,
            (aVal, fkJoinVal) -> "(" + aVal + "," + fkJoinVal + ")"
        );

        finalJoinResult.toStream().to("output");

        validateTopologyCanProcessData(builder);
    }

    @Test
    public void shouldWorkWithDefaultAndJoinResultSerdes() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, String> aTable = builder.table("A");
        final KTable<String, String> bTable = builder.table("B");

        final KTable<String, String> fkJoinResult = aTable.join(
            bTable,
            value -> value.split("-")[0],
            (aVal, bVal) -> "(" + aVal + "," + bVal + ")",
            Materialized
                .<String, String, KeyValueStore<Bytes, byte[]>>as("asdf")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        final KTable<String, String> finalJoinResult = aTable.join(
            fkJoinResult,
            (aVal, fkJoinVal) -> "(" + aVal + "," + fkJoinVal + ")"
        );

        finalJoinResult.toStream().to("output");

        validateTopologyCanProcessData(builder);
    }

    @Test
    public void shouldWorkWithDefaultAndEquiJoinResultSerdes() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, String> aTable = builder.table("A");
        final KTable<String, String> bTable = builder.table("B");

        final KTable<String, String> fkJoinResult = aTable.join(
            bTable,
            value -> value.split("-")[0],
            (aVal, bVal) -> "(" + aVal + "," + bVal + ")",
            Materialized.as("asdf")
        );

        final KTable<String, String> finalJoinResult = aTable.join(
            fkJoinResult,
            (aVal, fkJoinVal) -> "(" + aVal + "," + fkJoinVal + ")",
            Materialized.with(Serdes.String(), Serdes.String())
        );

        finalJoinResult.toStream().to("output");

        validateTopologyCanProcessData(builder);
    }

    @Test
    public void shouldWorkWithDefaultAndProducedSerdes() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, String> aTable = builder.table("A");
        final KTable<String, String> bTable = builder.table("B");

        final KTable<String, String> fkJoinResult = aTable.join(
            bTable,
            value -> value.split("-")[0],
            (aVal, bVal) -> "(" + aVal + "," + bVal + ")",
            Materialized.as("asdf")
        );

        final KTable<String, String> finalJoinResult = aTable.join(
            fkJoinResult,
            (aVal, fkJoinVal) -> "(" + aVal + "," + fkJoinVal + ")"
        );

        finalJoinResult.toStream().to("output", Produced.with(Serdes.String(), Serdes.String()));

        validateTopologyCanProcessData(builder);
    }

    private static void validateTopologyCanProcessData(final StreamsBuilder builder) {
        final Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "dummy-" + UUID.randomUUID());
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        config.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), config)) {
            final TestInputTopic<String, String> aTopic = topologyTestDriver.createInputTopic("A", new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> bTopic = topologyTestDriver.createInputTopic("B", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> output = topologyTestDriver.createOutputTopic("output", new StringDeserializer(), new StringDeserializer());
            aTopic.pipeInput("a1", "b1-alpha");
            bTopic.pipeInput("b1", "beta");
            final Map<String, String> x = output.readKeyValuesToMap();
            assertThat(x, is(Collections.singletonMap("a1", "(b1-alpha,(b1-alpha,beta))")));
        }
    }
}
