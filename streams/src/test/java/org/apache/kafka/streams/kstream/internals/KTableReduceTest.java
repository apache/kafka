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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.KeyValueTimestampHeaders;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.GenericInMemoryTimestampedKeyValueStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KTableReduceTest {

    /**
     * Provides both store format configurations for parameterized tests.
     */
    private static Stream<Arguments> storeFormats() {
        return Stream.of(
            Arguments.of("default"),
            Arguments.of("headers")
        );
    }

    private static Headers makeHeaders(final String key, final String value) {
        final RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(key, value.getBytes()));
        return headers;
    }

    private Properties getProps(final String storeFormat) {
        final Properties props = mkProperties(mkMap(
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("kafka-test").getAbsolutePath())));
        props.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, storeFormat);
        return props;
    }

    @Test
    public void shouldAddAndSubtract() {
        final InternalMockProcessorContext<String, Change<Set<String>>> context = new InternalMockProcessorContext<>();

        final Processor<String, Change<Set<String>>, String, Change<Set<String>>> reduceProcessor =
            new KTableReduce<String, Set<String>>(
                new MaterializedInternal<>(Materialized.as("myStore")),
                this::unionNotNullArgs,
                this::differenceNotNullArgs
            ).get();

        final TimestampedKeyValueStore<String, Set<String>> myStore =
            new GenericInMemoryTimestampedKeyValueStore<>("myStore");

        context.register(myStore, null);
        reduceProcessor.init(context);
        context.setCurrentNode(new ProcessorNode<>("reduce", reduceProcessor, singleton("myStore")));

        reduceProcessor.process(new Record<>("A", new Change<>(singleton("a"), null), 10L));
        assertEquals(ValueAndTimestamp.make(singleton("a"), 10L), myStore.get("A"));
        reduceProcessor.process(new Record<>("A", new Change<>(singleton("b"), singleton("a")), 15L));
        assertEquals(ValueAndTimestamp.make(singleton("b"), 15L), myStore.get("A"));
        reduceProcessor.process(new Record<>("A", new Change<>(null, singleton("b")), 12L));
        assertEquals(ValueAndTimestamp.make(emptySet(), 15L), myStore.get("A"));
    }

    @ParameterizedTest
    @MethodSource("storeFormats")
    public void shouldReduceAndEmitLatestValue(final String storeFormat) {
        final Properties props = getProps(storeFormat);
        final MockApiProcessorSupplier<String, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, String> table2 = table1
            .groupBy((key, value) -> new org.apache.kafka.streams.KeyValue<>(key, value), Grouped.with(Serdes.String(), Serdes.String()))
            .reduce(
                (value1, value2) -> value1 + "+" + value2,  // adder
                (value1, value2) -> value1.replace("+" + value2, ""),  // subtractor
                Materialized.as("reduce-store")
            );

        table2.toStream().process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            final Headers headersA = makeHeaders("key", "A");
            final Headers headersB = makeHeaders("key", "B");

            inputTopic.pipeInput(new TestRecord<>("A", "a", headersA, 10L));
            inputTopic.pipeInput(new TestRecord<>("B", "b", headersB, 15L));
            inputTopic.pipeInput(new TestRecord<>("A", "c", headersA, 20L));
            inputTopic.pipeInput(new TestRecord<>("B", "d", headersB, 18L));

            if (storeFormat.equals("default")) {
                assertEquals(
                    asList(
                        new KeyValueTimestamp<>("A", "a", 10L),
                        new KeyValueTimestamp<>("B", "b", 15L),
                        new KeyValueTimestamp<>("A", "a+c", 20L),  // reduced: old + new
                        new KeyValueTimestamp<>("B", "b+d", 18L)), // reduced: old + new
                    supplier.theCapturedProcessor().processed());
            } else if (storeFormat.equals("headers")) {
                assertEquals(
                    asList(
                        new KeyValueTimestampHeaders<>("A", "a", 10L, headersA),
                        new KeyValueTimestampHeaders<>("B", "b", 15L, headersB),
                        new KeyValueTimestampHeaders<>("A", "a+c", 20L, headersA),  // reduced: old + new
                        new KeyValueTimestampHeaders<>("B", "b+d", 18L, headersB)), // reduced: old + new
                    supplier.theCapturedProcessor().processedWithHeaders());
            }
        }
    }

    private Set<String> differenceNotNullArgs(final Set<String> left, final Set<String> right) {
        assertNotNull(left);
        assertNotNull(right);

        final HashSet<String> strings = new HashSet<>(left);
        strings.removeAll(right);
        return strings;
    }

    private Set<String> unionNotNullArgs(final Set<String> left, final Set<String> right) {
        assertNotNull(left);
        assertNotNull(right);

        final HashSet<String> strings = new HashSet<>();
        strings.addAll(left);
        strings.addAll(right);
        return strings;
    }
}
