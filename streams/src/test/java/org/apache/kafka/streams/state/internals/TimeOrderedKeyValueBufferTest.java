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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBuffer.Eviction;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueChangeBuffer.CHANGELOG_HEADERS;
import static org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueChangeBuffer.CHANGELOG_HEADERS_WITH_HEADERS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

public class TimeOrderedKeyValueBufferTest<B extends TimeOrderedKeyValueBuffer<String, String, Change<String>>> {

    private static final String APP_ID = "test-app";
    private Function<String, B> bufferSupplier;
    private String testName;

    public static final class NullRejectingStringSerializer extends StringSerializer {
        @Override
        public byte[] serialize(final String topic, final String data) {
            if (data == null) {
                throw new IllegalArgumentException("null data not allowed");
            }
            return super.serialize(topic, data);
        }
    }

    // As we add more buffer implementations/configurations, we can add them here
    public static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of("in-memory buffer",
                (Function<String, InMemoryTimeOrderedKeyValueChangeBuffer<String, String, Change<String>>>) name ->
                    new InMemoryTimeOrderedKeyValueChangeBuffer
                        .Builder<>(name, Serdes.String(), Serdes.serdeFrom(new NullRejectingStringSerializer(), new StringDeserializer()))
                        .build())
        );
    }

    private void setup(final String testName, final Function<String, B> bufferSupplier) {
        this.testName = testName + "_" + new Random().nextInt(Integer.MAX_VALUE);
        this.bufferSupplier = bufferSupplier;
    }

    private static MockInternalProcessorContext<?, ?> makeContext() {
        return makeContext(false);
    }

    private static MockInternalProcessorContext<?, ?> makeContext(final boolean headersEnabled) {
        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:localhost:9092");
        if (headersEnabled) {
            properties.setProperty(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);
        }

        final TaskId taskId = new TaskId(0, 0);

        final MockInternalProcessorContext<?, ?> context = new MockInternalProcessorContext<>(properties, taskId, TestUtils.tempDirectory());
        context.setRecordCollector(new MockRecordCollector());

        return context;
    }


    private static void cleanup(final MockInternalProcessorContext<?, ?> context, final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer) {
        try {
            buffer.close();
            Utils.delete(context.stateDir());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldInit(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);
        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldAcceptData(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "2p93nf");
        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldRejectNullValues(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);
        try {
            buffer.put(0, new Record<>("asdf", null, 0L), getContext(0));
            fail("expected an exception");
        } catch (final NullPointerException expected) {
            // expected
        }
        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldRemoveData(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "qwer");
        assertThat(buffer.numRecords(), is(1));
        buffer.evictWhile(() -> true, kv -> { });
        assertThat(buffer.numRecords(), is(0));
        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldRespectEvictionPredicate(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "eyt");
        putRecord(buffer, context, 1L, 0L, "zxcv", "rtg");
        assertThat(buffer.numRecords(), is(2));
        final List<Eviction<String, Change<String>>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> buffer.numRecords() > 1, evicted::add);
        assertThat(buffer.numRecords(), is(1));
        assertThat(evicted, is(singletonList(
            new Eviction<>("asdf", new Change<>("eyt", null), getContext(0L))
        )));
        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldTrackCount(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "oin");
        assertThat(buffer.numRecords(), is(1));
        putRecord(buffer, context, 1L, 0L, "asdf", "wekjn");
        assertThat(buffer.numRecords(), is(1));
        putRecord(buffer, context, 0L, 0L, "zxcv", "24inf");
        assertThat(buffer.numRecords(), is(2));
        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldTrackSize(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "23roni");
        assertThat(buffer.bufferSize(), is(43L));
        putRecord(buffer, context, 1L, 0L, "asdf", "3l");
        assertThat(buffer.bufferSize(), is(39L));
        putRecord(buffer, context, 0L, 0L, "zxcv", "qfowin");
        assertThat(buffer.bufferSize(), is(82L));
        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldTrackMinTimestamp(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 1L, 0L, "asdf", "2093j");
        assertThat(buffer.minTimestamp(), is(1L));
        putRecord(buffer, context, 0L, 0L, "zxcv", "3gon4i");
        assertThat(buffer.minTimestamp(), is(0L));
        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldEvictOldestAndUpdateSizeAndCountAndMinTimestamp(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);

        putRecord(buffer, context, 1L, 0L, "zxcv", "o23i4");
        assertThat(buffer.numRecords(), is(1));
        assertThat(buffer.bufferSize(), is(42L));
        assertThat(buffer.minTimestamp(), is(1L));

        putRecord(buffer, context, 0L, 0L, "asdf", "3ng");
        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.bufferSize(), is(82L));
        assertThat(buffer.minTimestamp(), is(0L));

        final AtomicInteger callbackCount = new AtomicInteger(0);
        buffer.evictWhile(() -> true, kv -> {
            switch (callbackCount.incrementAndGet()) {
                case 1: {
                    assertThat(kv.key(), is("asdf"));
                    assertThat(buffer.numRecords(), is(2));
                    assertThat(buffer.bufferSize(), is(82L));
                    assertThat(buffer.minTimestamp(), is(0L));
                    break;
                }
                case 2: {
                    assertThat(kv.key(), is("zxcv"));
                    assertThat(buffer.numRecords(), is(1));
                    assertThat(buffer.bufferSize(), is(42L));
                    assertThat(buffer.minTimestamp(), is(1L));
                    break;
                }
                default: {
                    fail("too many invocations");
                    break;
                }
            }
        });
        assertThat(callbackCount.get(), is(2));
        assertThat(buffer.numRecords(), is(0));
        assertThat(buffer.bufferSize(), is(0L));
        assertThat(buffer.minTimestamp(), is(Long.MAX_VALUE));
        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldReturnUndefinedOnPriorValueForNotBufferedKey(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);

        assertThat(buffer.priorValueForBuffered("ASDF"), is(Maybe.undefined()));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldReturnPriorValueForBufferedKey(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);

        final ProcessorRecordContext recordContext = getContext(0L);
        context.setRecordContext(recordContext);
        buffer.put(1L, new Record<>("A", new Change<>("new-value", "old-value"), 0L), recordContext);
        buffer.put(1L, new Record<>("B", new Change<>("new-value", null), 0L), recordContext);
        assertThat(buffer.priorValueForBuffered("A"), is(Maybe.defined(ValueTimestampHeaders.make("old-value", -1, new RecordHeaders()))));
        assertThat(buffer.priorValueForBuffered("B"), is(Maybe.defined(null)));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldPropagateHeadersThroughEviction(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);

        final RecordHeaders headers = new RecordHeaders(new Header[]{new RecordHeader("h1", "v1".getBytes(UTF_8))});
        final ProcessorRecordContext recordContext = getContext(0L);
        context.setRecordContext(recordContext);
        buffer.put(0L, new Record<>("k", new Change<>("v", null), 0L, headers), recordContext);

        final List<Eviction<String, Change<String>>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> true, evicted::add);

        assertThat(evicted.size(), is(1));
        assertThat(evicted.get(0).recordContext().headers(), is(headers));
        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldUseRecordHeadersNotProcessorContextHeadersOnPut(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);

        final RecordHeaders contextHeaders = new RecordHeaders(new Header[]{new RecordHeader("ctx", "from-context".getBytes(UTF_8))});
        final RecordHeaders recordHeaders = new RecordHeaders(new Header[]{new RecordHeader("rec", "from-record".getBytes(UTF_8))});
        final ProcessorRecordContext recordContext = new ProcessorRecordContext(0L, 0, 0, "topic", contextHeaders);
        context.setRecordContext(recordContext);

        buffer.put(0L, new Record<>("k", new Change<>("v", null), 0L, recordHeaders), recordContext);

        final List<Eviction<String, Change<String>>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> true, evicted::add);

        assertThat(evicted.size(), is(1));
        assertThat(evicted.get(0).recordContext().headers(), is(recordHeaders));
        cleanup(context, buffer);
    }

    @Test
    public void shouldDeserializeEvictedValueWithBufferedHeadersNotEvictionTriggerHeaders() {
        // A header-dependent value deserializer (as e.g. Schema Registry serdes are, and String
        // serdes are not) records which headers it is handed. This lets us prove that on eviction the
        // buffered value is deserialized with the headers it was buffered with, and not with the
        // headers of whatever record happened to trigger the eviction.
        final List<String> headerSeenByDeserializer = new ArrayList<>();
        final Deserializer<String> recordingDeserializer = new Deserializer<>() {
            @Override
            public String deserialize(final String topic, final byte[] data) {
                return data == null ? null : new String(data, UTF_8);
            }

            @Override
            public String deserialize(final String topic, final Headers headers, final byte[] data) {
                final Header header = headers.lastHeader("h");
                headerSeenByDeserializer.add(header == null ? "none" : new String(header.value(), UTF_8));
                return deserialize(topic, data);
            }
        };
        final Serde<String> valueSerde = Serdes.serdeFrom(new StringSerializer(), recordingDeserializer);

        final InMemoryTimeOrderedKeyValueChangeBuffer<String, String, Change<String>> buffer =
            new InMemoryTimeOrderedKeyValueChangeBuffer.Builder<>("test-buffer", Serdes.String(), valueSerde).build();
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);

        // Buffer key "k" while the processing context carries header h=A.
        final RecordHeaders bufferedHeaders = new RecordHeaders(new Header[]{new RecordHeader("h", "A".getBytes(UTF_8))});
        final ProcessorRecordContext bufferedContext = new ProcessorRecordContext(0L, 0, 0, "topic", bufferedHeaders);
        context.setRecordContext(bufferedContext);
        buffer.put(0L, new Record<>("k", new Change<>("v", null), 0L, bufferedHeaders), bufferedContext);

        // Eviction happens later, while a DIFFERENT record (header h=B) is being processed.
        context.setRecordContext(new ProcessorRecordContext(1L, 1, 0, "topic",
            new RecordHeaders(new Header[]{new RecordHeader("h", "B".getBytes(UTF_8))})));

        final List<Eviction<String, Change<String>>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> true, evicted::add);

        assertThat(evicted.size(), is(1));
        // The buffered value must be deserialized with its own headers ("A"), not the headers of the
        // record that triggered the eviction ("B").
        assertThat(headerSeenByDeserializer, is(singletonList("A")));
        cleanup(context, buffer);
    }

    @Test
    public void shouldStorePerValueHeadersInChangelogWhenHeadersEnabled() {
        // Discrimination test for headers mode: with dsl.store.format=HEADERS the buffer writes the V4
        // changelog format in which the old and new value parts each carry their OWN headers and
        // timestamp. This is something the plain (V3) format structurally cannot represent.
        final InMemoryTimeOrderedKeyValueChangeBuffer<String, String, Change<String>> buffer =
            new InMemoryTimeOrderedKeyValueChangeBuffer.Builder<>("test-buffer", Serdes.String(), Serdes.String()).build();
        final MockInternalProcessorContext<?, ?> context = makeContext(true);
        buffer.init(context, buffer);

        final RecordHeaders headersA = new RecordHeaders(new Header[]{new RecordHeader("h", "A".getBytes(UTF_8))});
        final RecordHeaders headersB = new RecordHeaders(new Header[]{new RecordHeader("h", "B".getBytes(UTF_8))});

        // First buffer "k" (value "v1") with headers A at record timestamp 10.
        final ProcessorRecordContext contextA = new ProcessorRecordContext(10L, 0, 0, "topic", headersA);
        context.setRecordContext(contextA);
        buffer.put(0L, new Record<>("k", new Change<>("v1", null), 10L, headersA), contextA);

        // In-place update (value "v2", old "v1") with headers B at record timestamp 20. The old value
        // ("v1") should keep the first record's headers/timestamp (A / 10) via carry-forward.
        final ProcessorRecordContext contextB = new ProcessorRecordContext(20L, 1, 0, "topic", headersB);
        context.setRecordContext(contextB);
        buffer.put(0L, new Record<>("k", new Change<>("v2", "v1"), 20L, headersB), contextB);

        buffer.commit(Map.of());

        final List<ProducerRecord<Object, Object>> collected = ((MockRecordCollector) context.recordCollector()).collected();
        assertThat(collected.size(), is(1));
        final ProducerRecord<Object, Object> changelogRecord = collected.get(0);

        // Headers mode uses the V4 changelog format...
        assertThat(changelogRecord.headers(), is(CHANGELOG_HEADERS_WITH_HEADERS));

        // ...and each value part carries its own headers + timestamp.
        final BufferValue bufferValue = BufferValue.deserialize(ByteBuffer.wrap((byte[]) changelogRecord.value()));
        final ValueTimestampHeadersDeserializer<String> deserializer =
            new ValueTimestampHeadersDeserializer<>(new StringDeserializer());

        final ValueTimestampHeaders<String> newValue = deserializer.deserialize("topic", bufferValue.newValue());
        assertThat(newValue.value(), is("v2"));
        assertThat(newValue.timestamp(), is(20L));
        assertThat(newValue.headers(), is(headersB));

        final ValueTimestampHeaders<String> oldValue = deserializer.deserialize("topic", bufferValue.oldValue());
        assertThat(oldValue.value(), is("v1"));
        assertThat(oldValue.timestamp(), is(10L));    // carried forward from the first record
        assertThat(oldValue.headers(), is(headersA));  // carried forward from the first record

        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldPreservePriorValueTimestampAndHeadersWhenHeadersEnabled(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext(true);
        buffer.init(context, buffer);

        final RecordHeaders headers = new RecordHeaders(new Header[]{new RecordHeader("h1", "v1".getBytes(UTF_8))});
        final ProcessorRecordContext recordContext = getContext(0L);
        context.setRecordContext(recordContext);
        buffer.put(1L, new Record<>("A", new Change<>("new-value", "old-value"), 0L, headers), recordContext);
        buffer.put(1L, new Record<>("B", new Change<>("new-value", null), 0L, headers), recordContext);

        // The prior value's original timestamp/headers are unknown when a key is first buffered, so
        // they round-trip through the ValueTimestampHeaders encoding as UNKNOWN/empty.
        assertThat(buffer.priorValueForBuffered("A"), is(Maybe.defined(ValueTimestampHeaders.make("old-value", -1, new RecordHeaders()))));
        assertThat(buffer.priorValueForBuffered("B"), is(Maybe.defined(null)));
        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldRoundTripHeadersThroughCommitAndRestoreWhenHeadersEnabled(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);

        // Buffer a record (with headers and a record timestamp distinct from the buffer time) and
        // commit it to the changelog using the headers-aware V4 format.
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext(true);
        buffer.init(context, buffer);

        final RecordHeaders headers = new RecordHeaders(new Header[]{new RecordHeader("h1", "v1".getBytes(UTF_8))});
        context.setRecordContext(new ProcessorRecordContext(5L, 0, 0, "topic", new RecordHeaders()));
        buffer.put(0L, new Record<>("k", new Change<>("new", "old"), 5L, headers), context.recordContext());
        buffer.commit(Map.of());

        final List<ProducerRecord<Object, Object>> collected = ((MockRecordCollector) context.recordCollector()).collected();
        assertThat(collected.size(), is(1));

        // Restore the changelog into a fresh buffer and confirm the value, record timestamp and
        // headers all survived the V4 serialization round-trip.
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> restored = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> restoreContext = makeContext(true);
        restored.init(restoreContext, restored);
        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) restoreContext.stateRestoreCallback(testName);

        final List<ConsumerRecord<byte[], byte[]>> toRestore = new LinkedList<>();
        for (final ProducerRecord<Object, Object> pr : collected) {
            toRestore.add(new ConsumerRecord<>(
                "changelog-topic", 0, 0, 999, TimestampType.CREATE_TIME, -1, -1,
                ((Bytes) pr.key()).get(), (byte[]) pr.value(), pr.headers(), Optional.empty()));
        }
        stateRestoreCallback.restoreBatch(toRestore);

        final List<Eviction<String, Change<String>>> evicted = new LinkedList<>();
        restored.evictWhile(() -> true, evicted::add);

        assertThat(evicted.size(), is(1));
        assertThat(evicted.get(0).key(), is("k"));
        assertThat(evicted.get(0).value(), is(new Change<>("new", "old")));
        assertThat(evicted.get(0).recordContext().timestamp(), is(5L));
        assertThat(evicted.get(0).recordContext().headers(), is(headers));
        cleanup(restoreContext, restored);
        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldCommit(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 2L, 0L, "asdf", "2093j");
        putRecord(buffer, context, 1L, 1L, "zxcv", "3gon4i");
        putRecord(buffer, context, 0L, 2L, "deleteme", "deadbeef");

        // replace "deleteme" with a tombstone
        buffer.evictWhile(() -> buffer.minTimestamp() < 1, kv -> { });

        // commit everything to the changelog
        buffer.commit(Map.of());

        // the buffer should serialize the buffer time and the value as byte[],
        // which we can't compare for equality using ProducerRecord.
        // As a workaround, I'm deserializing them and shoving them in a KeyValue, just for ease of testing.

        final List<ProducerRecord<String, KeyValue<Long, BufferValue>>> collected =
            ((MockRecordCollector) context.recordCollector())
                .collected()
                .stream()
                .map(pr -> {
                    final KeyValue<Long, BufferValue> niceValue;
                    if (pr.value() == null) {
                        niceValue = null;
                    } else {
                        final byte[] serializedValue = (byte[]) pr.value();
                        final ByteBuffer valueBuffer = ByteBuffer.wrap(serializedValue);
                        final BufferValue contextualRecord = BufferValue.deserialize(valueBuffer);
                        final long timestamp = valueBuffer.getLong();
                        niceValue = new KeyValue<>(timestamp, contextualRecord);
                    }

                    return new ProducerRecord<>(pr.topic(),
                                                pr.partition(),
                                                pr.timestamp(),
                                                pr.key().toString(),
                                                niceValue,
                                                pr.headers());
                })
                .collect(Collectors.toList());

        assertThat(collected, is(asList(
            new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
                                 0,   // Producer will assign
                                 null,
                                 "deleteme",
                                 null,
                                 new RecordHeaders()
            ),
            new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
                                 0,
                                 null,
                                 "zxcv",
                                 new KeyValue<>(1L, getBufferValue("3gon4i", 1)),
                                 CHANGELOG_HEADERS
            ),
            new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
                                 0,
                                 null,
                                 "asdf",
                                 new KeyValue<>(2L, getBufferValue("2093j", 0)),
                                 CHANGELOG_HEADERS
            )
        )));

        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldRestoreOldUnversionedFormat(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);

        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", new RecordHeaders()));

        // These serialized formats were captured by running version 2.1 code.
        // They verify that an upgrade from 2.1 will work.
        // Do not change them.
        final String toDeleteBinaryValue = "0000000000000000FFFFFFFF00000006646F6F6D6564";
        final String asdfBinaryValue = "0000000000000002FFFFFFFF0000000471776572";
        final String zxcvBinaryValue1 = "00000000000000010000000870726576696F757300000005656F34696D";
        final String zxcvBinaryValue2 = "000000000000000100000005656F34696D000000046E657874";

        stateRestoreCallback.restoreBatch(asList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 0,
                                 0,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 hexStringToByteArray(toDeleteBinaryValue),
                                 new RecordHeaders(),
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 1,
                                 1,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "asdf".getBytes(UTF_8),
                                 hexStringToByteArray(asdfBinaryValue),
                                 new RecordHeaders(),
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 2,
                                 2,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 hexStringToByteArray(zxcvBinaryValue1),
                                 new RecordHeaders(),
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 hexStringToByteArray(zxcvBinaryValue2),
                                 new RecordHeaders(),
                                 Optional.empty())
        ));

        assertThat(buffer.numRecords(), is(3));
        assertThat(buffer.minTimestamp(), is(0L));
        assertThat(buffer.bufferSize(), is(172L));

        stateRestoreCallback.restoreBatch(singletonList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 null,
                                 new RecordHeaders(),
                                 Optional.empty())
        ));

        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.minTimestamp(), is(1L));
        assertThat(buffer.bufferSize(), is(115L));

        assertThat(buffer.priorValueForBuffered("todelete"), is(Maybe.undefined()));
        assertThat(buffer.priorValueForBuffered("asdf"), is(Maybe.defined(null)));
        assertThat(buffer.priorValueForBuffered("zxcv"), is(Maybe.defined(ValueTimestampHeaders.make("previous", -1, new RecordHeaders()))));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        final List<Eviction<String, Change<String>>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> true, evicted::add);

        // Several things to note:
        // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
        // * The record timestamps are properly restored, and not conflated with the record's buffer time.
        // * The keys and values are properly restored
        // * The record topic is set to the changelog topic. This was an oversight in the original implementation,
        //   which is fixed in changelog format v1. But upgraded applications still need to be able to handle the
        //   original format.

        assertThat(evicted, is(asList(
            new Eviction<>(
                "zxcv",
                new Change<>("next", "eo4im"),
                new ProcessorRecordContext(3L, 3, 0, "changelog-topic", new RecordHeaders())),
            new Eviction<>(
                "asdf",
                new Change<>("qwer", null),
                new ProcessorRecordContext(1L, 1, 0, "changelog-topic", new RecordHeaders()))
        )));

        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldRestoreV1Format(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);

        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", new RecordHeaders()));

        final RecordHeaders v1FlagHeaders = new RecordHeaders(new Header[] {new RecordHeader("v", new byte[] {(byte) 1})});

        // These serialized formats were captured by running version 2.2 code.
        // They verify that an upgrade from 2.2 will work.
        // Do not change them.
        final String toDeleteBinary = "00000000000000000000000000000000000000000000000000000005746F70696300000000FFFFFFFF0000000EFFFFFFFF00000006646F6F6D6564";
        final String asdfBinary = "00000000000000020000000000000001000000000000000000000005746F70696300000000FFFFFFFF0000000CFFFFFFFF0000000471776572";
        final String zxcvBinary1 = "00000000000000010000000000000002000000000000000000000005746F70696300000000FFFFFFFF000000150000000870726576696F757300000005336F34696D";
        final String zxcvBinary2 = "00000000000000010000000000000003000000000000000000000005746F70696300000000FFFFFFFF0000001100000005336F34696D000000046E657874";

        stateRestoreCallback.restoreBatch(asList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 0,
                                 999,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 hexStringToByteArray(toDeleteBinary),
                                 v1FlagHeaders,
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 1,
                                 9999,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "asdf".getBytes(UTF_8),
                                 hexStringToByteArray(asdfBinary),
                                 v1FlagHeaders,
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 2,
                                 99,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 hexStringToByteArray(zxcvBinary1),
                                 v1FlagHeaders,
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 3,
                                 100,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 hexStringToByteArray(zxcvBinary2),
                                 v1FlagHeaders,
                                 Optional.empty())
        ));

        assertThat(buffer.numRecords(), is(3));
        assertThat(buffer.minTimestamp(), is(0L));
        assertThat(buffer.bufferSize(), is(142L));

        stateRestoreCallback.restoreBatch(singletonList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 null,
                                 new RecordHeaders(),
                                 Optional.empty())
        ));

        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.minTimestamp(), is(1L));
        assertThat(buffer.bufferSize(), is(95L));

        assertThat(buffer.priorValueForBuffered("todelete"), is(Maybe.undefined()));
        assertThat(buffer.priorValueForBuffered("asdf"), is(Maybe.defined(null)));
        assertThat(buffer.priorValueForBuffered("zxcv"), is(Maybe.defined(ValueTimestampHeaders.make("previous", -1, new RecordHeaders()))));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        final List<Eviction<String, Change<String>>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> true, evicted::add);

        // Several things to note:
        // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
        // * The record timestamps are properly restored, and not conflated with the record's buffer time.
        // * The keys and values are properly restored
        // * The record topic is set to the original input topic, *not* the changelog topic
        // * The record offset preserves the original input record's offset, *not* the offset of the changelog record


        assertThat(evicted, is(asList(
            new Eviction<>(
                "zxcv",
                new Change<>("next", "3o4im"),
                getContext(3L)),
            new Eviction<>(
                "asdf",
                new Change<>("qwer", null),
                getContext(1L)
            ))));

        cleanup(context, buffer);
    }


    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldRestoreV2Format(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);

        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", new RecordHeaders()));

        final RecordHeaders v2FlagHeaders = new RecordHeaders(new Header[] {new RecordHeader("v", new byte[] {(byte) 2})});

        // These serialized formats were captured by running version 2.3 code.
        // They verify that an upgrade from 2.3 will work.
        // Do not change them.
        final String toDeleteBinary = "0000000000000000000000000000000000000005746F70696300000000FFFFFFFF0000000EFFFFFFFF00000006646F6F6D6564FFFFFFFF0000000000000000";
        final String asdfBinary = "0000000000000001000000000000000000000005746F70696300000000FFFFFFFF0000000CFFFFFFFF0000000471776572FFFFFFFF0000000000000002";
        final String zxcvBinary1 = "0000000000000002000000000000000000000005746F70696300000000FFFFFFFF000000140000000749474E4F52454400000005336F34696D0000000870726576696F75730000000000000001";
        final String zxcvBinary2 = "0000000000000003000000000000000000000005746F70696300000000FFFFFFFF0000001100000005336F34696D000000046E6578740000000870726576696F75730000000000000001";

        stateRestoreCallback.restoreBatch(asList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 0,
                                 999,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 hexStringToByteArray(toDeleteBinary),
                                 v2FlagHeaders,
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 1,
                                 9999,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "asdf".getBytes(UTF_8),
                                 hexStringToByteArray(asdfBinary),
                                 v2FlagHeaders,
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 2,
                                 99,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 hexStringToByteArray(zxcvBinary1),
                                 v2FlagHeaders,
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 2,
                                 100,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 hexStringToByteArray(zxcvBinary2),
                                 v2FlagHeaders,
                                 Optional.empty())
        ));

        assertThat(buffer.numRecords(), is(3));
        assertThat(buffer.minTimestamp(), is(0L));
        assertThat(buffer.bufferSize(), is(142L));

        stateRestoreCallback.restoreBatch(singletonList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 null,
                                 new RecordHeaders(),
                                 Optional.empty())
        ));

        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.minTimestamp(), is(1L));
        assertThat(buffer.bufferSize(), is(95L));

        assertThat(buffer.priorValueForBuffered("todelete"), is(Maybe.undefined()));
        assertThat(buffer.priorValueForBuffered("asdf"), is(Maybe.defined(null)));
        assertThat(buffer.priorValueForBuffered("zxcv"), is(Maybe.defined(ValueTimestampHeaders.make("previous", -1, new RecordHeaders()))));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        final List<Eviction<String, Change<String>>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> true, evicted::add);

        // Several things to note:
        // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
        // * The record timestamps are properly restored, and not conflated with the record's buffer time.
        // * The keys and values are properly restored
        // * The record topic is set to the original input topic, *not* the changelog topic
        // * The record offset preserves the original input record's offset, *not* the offset of the changelog record


        assertThat(evicted, is(asList(
            new Eviction<>(
                "zxcv",
                new Change<>("next", "3o4im"),
                getContext(3L)),
            new Eviction<>(
                "asdf",
                new Change<>("qwer", null),
                getContext(1L)
            ))));

        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldRestoreV3FormatWithV2Header(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        // versions 2.4.0, 2.4.1, and 2.5.0 would have erroneously encoded a V3 record with the
        // V2 header, so we need to be sure to handle this case as well.
        // Note the data is the same as the V3 test.
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);

        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", new RecordHeaders()));

        final RecordHeaders headers = new RecordHeaders(new Header[] {new RecordHeader("v", new byte[] {(byte) 2})});

        // These serialized formats were captured by running version 2.4 code.
        // They verify that an upgrade from 2.4 will work.
        // Do not change them.
        final String toDeleteBinary = "0000000000000000000000000000000000000005746F70696300000000FFFFFFFFFFFFFFFFFFFFFFFF00000006646F6F6D65640000000000000000";
        final String asdfBinary = "0000000000000001000000000000000000000005746F70696300000000FFFFFFFFFFFFFFFFFFFFFFFF00000004717765720000000000000002";
        final String zxcvBinary1 = "0000000000000002000000000000000000000005746F70696300000000FFFFFFFF0000000870726576696F75730000000749474E4F52454400000005336F34696D0000000000000001";
        final String zxcvBinary2 = "0000000000000003000000000000000000000005746F70696300000000FFFFFFFF0000000870726576696F757300000005336F34696D000000046E6578740000000000000001";

        stateRestoreCallback.restoreBatch(asList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 0,
                                 999,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 hexStringToByteArray(toDeleteBinary),
                                 headers,
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 1,
                                 9999,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "asdf".getBytes(UTF_8),
                                 hexStringToByteArray(asdfBinary),
                                 headers,
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 2,
                                 99,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 hexStringToByteArray(zxcvBinary1),
                                 headers,
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 2,
                                 100,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 hexStringToByteArray(zxcvBinary2),
                                 headers,
                                 Optional.empty())
        ));

        assertThat(buffer.numRecords(), is(3));
        assertThat(buffer.minTimestamp(), is(0L));
        assertThat(buffer.bufferSize(), is(142L));

        stateRestoreCallback.restoreBatch(singletonList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 null,
                                 new RecordHeaders(),
                                 Optional.empty())
        ));

        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.minTimestamp(), is(1L));
        assertThat(buffer.bufferSize(), is(95L));

        assertThat(buffer.priorValueForBuffered("todelete"), is(Maybe.undefined()));
        assertThat(buffer.priorValueForBuffered("asdf"), is(Maybe.defined(null)));
        assertThat(buffer.priorValueForBuffered("zxcv"), is(Maybe.defined(ValueTimestampHeaders.make("previous", -1, new RecordHeaders()))));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        final List<Eviction<String, Change<String>>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> true, evicted::add);

        // Several things to note:
        // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
        // * The record timestamps are properly restored, and not conflated with the record's buffer time.
        // * The keys and values are properly restored
        // * The record topic is set to the original input topic, *not* the changelog topic
        // * The record offset preserves the original input record's offset, *not* the offset of the changelog record


        assertThat(evicted, is(asList(
            new Eviction<>(
                "zxcv",
                new Change<>("next", "3o4im"),
                getContext(3L)),
            new Eviction<>(
                "asdf",
                new Change<>("qwer", null),
                getContext(1L)
            ))));

        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldRestoreV3Format(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);

        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", new RecordHeaders()));

        final RecordHeaders headers = new RecordHeaders(new Header[] {new RecordHeader("v", new byte[] {(byte) 3})});

        // These serialized formats were captured by running version 2.4 code.
        // They verify that an upgrade from 2.4 will work.
        // Do not change them.
        final String toDeleteBinary = "0000000000000000000000000000000000000005746F70696300000000FFFFFFFFFFFFFFFFFFFFFFFF00000006646F6F6D65640000000000000000";
        final String asdfBinary = "0000000000000001000000000000000000000005746F70696300000000FFFFFFFFFFFFFFFFFFFFFFFF00000004717765720000000000000002";
        final String zxcvBinary1 = "0000000000000002000000000000000000000005746F70696300000000FFFFFFFF0000000870726576696F75730000000749474E4F52454400000005336F34696D0000000000000001";
        final String zxcvBinary2 = "0000000000000003000000000000000000000005746F70696300000000FFFFFFFF0000000870726576696F757300000005336F34696D000000046E6578740000000000000001";

        stateRestoreCallback.restoreBatch(asList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 0,
                                 999,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 hexStringToByteArray(toDeleteBinary),
                                 headers,
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 1,
                                 9999,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "asdf".getBytes(UTF_8),
                                 hexStringToByteArray(asdfBinary),
                                 headers,
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 2,
                                 99,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 hexStringToByteArray(zxcvBinary1),
                                 headers,
                                 Optional.empty()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 2,
                                 100,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 hexStringToByteArray(zxcvBinary2),
                                 headers,
                                 Optional.empty())
        ));

        assertThat(buffer.numRecords(), is(3));
        assertThat(buffer.minTimestamp(), is(0L));
        assertThat(buffer.bufferSize(), is(142L));

        stateRestoreCallback.restoreBatch(singletonList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 null,
                                 new RecordHeaders(),
                                 Optional.empty())
        ));

        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.minTimestamp(), is(1L));
        assertThat(buffer.bufferSize(), is(95L));

        assertThat(buffer.priorValueForBuffered("todelete"), is(Maybe.undefined()));
        assertThat(buffer.priorValueForBuffered("asdf"), is(Maybe.defined(null)));
        assertThat(buffer.priorValueForBuffered("zxcv"), is(Maybe.defined(ValueTimestampHeaders.make("previous", -1, new RecordHeaders()))));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        final List<Eviction<String, Change<String>>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> true, evicted::add);

        // Several things to note:
        // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
        // * The record timestamps are properly restored, and not conflated with the record's buffer time.
        // * The keys and values are properly restored
        // * The record topic is set to the original input topic, *not* the changelog topic
        // * The record offset preserves the original input record's offset, *not* the offset of the changelog record


        assertThat(evicted, is(asList(
            new Eviction<>(
                "zxcv",
                new Change<>("next", "3o4im"),
                getContext(3L)),
            new Eviction<>(
                "asdf",
                new Change<>("qwer", null),
                getContext(1L)
            ))));

        cleanup(context, buffer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldNotRestoreUnrecognizedVersionRecord(final String testName, final Function<String, B> bufferSupplier) {
        setup(testName, bufferSupplier);
        final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext<?, ?> context = makeContext();
        buffer.init(context, buffer);

        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", new RecordHeaders()));

        final RecordHeaders unknownFlagHeaders = new RecordHeaders(new Header[] {new RecordHeader("v", new byte[] {(byte) -1})});

        final byte[] todeleteValue = getBufferValue("doomed", 0).serialize(0).array();
        try {
            stateRestoreCallback.restoreBatch(singletonList(
                new ConsumerRecord<>("changelog-topic",
                                     0,
                                     0,
                                     999,
                                     TimestampType.CREATE_TIME,
                                     -1,
                                     -1,
                                     "todelete".getBytes(UTF_8),
                                     ByteBuffer.allocate(Long.BYTES + todeleteValue.length).putLong(0L).put(todeleteValue).array(),
                                     unknownFlagHeaders,
                                     Optional.empty())
            ));
            fail("expected an exception");
        } catch (final IllegalArgumentException expected) {
            // nothing to do.
        } finally {
            cleanup(context, buffer);
        }
    }

    private static void putRecord(final TimeOrderedKeyValueBuffer<String, String, Change<String>> buffer,
                                  final MockInternalProcessorContext<?, ?> context,
                                  final long streamTime,
                                  final long recordTimestamp,
                                  final String key,
                                  final String value) {
        final ProcessorRecordContext recordContext = getContext(recordTimestamp);
        context.setRecordContext(recordContext);
        buffer.put(streamTime, new Record<>(key, new Change<>(value, null), 0L), recordContext);
    }

    @SuppressWarnings("resource")
    private static BufferValue getBufferValue(final String value, final long timestamp) {
        return new BufferValue(
            null,
            null,
            new StringSerializer().serialize(null, value),
            getContext(timestamp)
        );
    }

    private static ProcessorRecordContext getContext(final long recordTimestamp) {
        return new ProcessorRecordContext(recordTimestamp, 0, 0, "topic", new RecordHeaders());
    }


    // to be used to generate future hex-encoded values
//    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
//    private static String bytesToHex(final byte[] bytes) {
//        final char[] hexChars = new char[bytes.length * 2];
//        for (int j = 0; j < bytes.length; j++) {
//            final int v = bytes[j] & 0xFF;
//            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
//            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
//        }
//        return new String(hexChars);
//    }

    private static byte[] hexStringToByteArray(final String hexString) {
        final int len = hexString.length();
        final byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                + Character.digit(hexString.charAt(i + 1), 16));
        }
        return data;
    }
}
