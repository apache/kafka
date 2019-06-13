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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.MaterializedKV;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

public class WindowStoreAdaptTest {
    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
    private final Properties config = Utils.mkProperties(Utils.mkMap(
        Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase(Locale.getDefault())),
        Utils.mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
        Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus")
    ));

    @Test
    public void testWithKVStore() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder
            .stream("input", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.of(ofMillis(100)))
            .aggregate(
                () -> "init",
                (k1, v1, prev) -> prev + " (" + k1 + "," + v1 + ")",
                Materialized.<String, String, WindowStore<Bytes, byte[]>>with(Serdes.String(), Serdes.String())
            )
            .filter(
                (windowedKey, agg) -> agg.split(" ").length % 2 == 0,
                MaterializedKV.<Windowed<String>, String>as(Stores.inMemoryKeyValueStore("store"))
                    .withKeySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class))
                    .withValueSerde(Serdes.String())
            )
            .toStream()
            .foreach((k, v) -> System.out.println("filtered K: " + k + ", V: " + v));
        final Topology topology = builder.build();


        final ConsumerRecordFactory<String, String> recordFactory =
            new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", 2L));
        }
    }

    @Test
    public void testWithWindowedStore() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder
            .stream("input", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.of(ofMillis(100)))
            .aggregate(
                () -> "init",
                (k1, v1, prev) -> prev + " (" + k1 + "," + v1 + ")",
                Materialized.<String, String, WindowStore<Bytes, byte[]>>with(Serdes.String(), Serdes.String())
            )
            .filter(
                (windowedKey, agg) -> agg.split(" ").length % 2 == 0,
                MaterializedKV.<Windowed<String>, String>as(Stores.inMemoryWindowStore("store", Duration.ofDays(1), Duration.ofMillis(100), false))
                    .withKeySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class))
                    .withValueSerde(Serdes.String())
            )
            .toStream()
            .foreach((k, v) -> System.out.println("filtered K: " + k + ", V: " + v));
        final Topology topology = builder.build();


        final ConsumerRecordFactory<String, String> recordFactory =
            new ConsumerRecordFactory<>(STRING_SERIALIZER, STRING_SERIALIZER);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            driver.pipeInput(recordFactory.create("input", "k1", "v1", 0L));
            driver.pipeInput(recordFactory.create("input", "k1", "v2", 1L));
            driver.pipeInput(recordFactory.create("input", "k2", "v1", 2L));
        }
    }
}
