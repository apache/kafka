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
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBuffer.Eviction;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.MockInternalProcessorContext.MockRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TimeOrderedKeyValueBufferTest<B extends TimeOrderedKeyValueBuffer<String, String>> {
    private static final RecordHeaders V_2_CHANGELOG_HEADERS =
        new RecordHeaders(new Header[] {new RecordHeader("v", new byte[] {(byte) 2})});

    private static final String APP_ID = "test-app";
    private final Function<String, B> bufferSupplier;
    private final String testName;

    // As we add more buffer implementations/configurations, we can add them here
    @Parameterized.Parameters(name = "{index}: test={0}")
    public static Collection<Object[]> parameters() {
        return singletonList(
            new Object[] {
                "in-memory buffer",
                (Function<String, InMemoryTimeOrderedKeyValueBuffer<String, String>>) name ->
                    new InMemoryTimeOrderedKeyValueBuffer
                        .Builder<>(name, Serdes.String(), Serdes.String())
                        .build()
            }
        );
    }

    public TimeOrderedKeyValueBufferTest(final String testName, final Function<String, B> bufferSupplier) {
        this.testName = testName + "_" + new Random().nextInt(Integer.MAX_VALUE);
        this.bufferSupplier = bufferSupplier;
    }

    private static MockInternalProcessorContext makeContext() {
        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        final TaskId taskId = new TaskId(0, 0);

        final MockInternalProcessorContext context = new MockInternalProcessorContext(properties, taskId, TestUtils.tempDirectory());
        context.setRecordCollector(new MockRecordCollector());

        return context;
    }


    private static void cleanup(final MockInternalProcessorContext context, final TimeOrderedKeyValueBuffer<String, String> buffer) {
        try {
            buffer.close();
            Utils.delete(context.stateDir());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldInit() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        cleanup(context, buffer);
    }

    @Test
    public void shouldAcceptData() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "2p93nf");
        cleanup(context, buffer);
    }

    @Test
    public void shouldRejectNullValues() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        try {
            buffer.put(0, "asdf", null, getContext(0));
            fail("expected an exception");
        } catch (final NullPointerException expected) {
            // expected
        }
        cleanup(context, buffer);
    }

    @Test
    public void shouldRemoveData() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "qwer");
        assertThat(buffer.numRecords(), is(1));
        buffer.evictWhile(() -> true, kv -> { });
        assertThat(buffer.numRecords(), is(0));
        cleanup(context, buffer);
    }

    @Test
    public void shouldRespectEvictionPredicate() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "eyt");
        putRecord(buffer, context, 1L, 0L, "zxcv", "rtg");
        assertThat(buffer.numRecords(), is(2));
        final List<Eviction<String, String>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> buffer.numRecords() > 1, evicted::add);
        assertThat(buffer.numRecords(), is(1));
        assertThat(evicted, is(singletonList(
            new Eviction<>("asdf", new Change<>("eyt", null), getContext(0L))
        )));
        cleanup(context, buffer);
    }

    @Test
    public void shouldTrackCount() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "oin");
        assertThat(buffer.numRecords(), is(1));
        putRecord(buffer, context, 1L, 0L, "asdf", "wekjn");
        assertThat(buffer.numRecords(), is(1));
        putRecord(buffer, context, 0L, 0L, "zxcv", "24inf");
        assertThat(buffer.numRecords(), is(2));
        cleanup(context, buffer);
    }

    @Test
    public void shouldTrackSize() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "23roni");
        assertThat(buffer.bufferSize(), is(51L));
        putRecord(buffer, context, 1L, 0L, "asdf", "3l");
        assertThat(buffer.bufferSize(), is(47L));
        putRecord(buffer, context, 0L, 0L, "zxcv", "qfowin");
        assertThat(buffer.bufferSize(), is(98L));
        cleanup(context, buffer);
    }

    @Test
    public void shouldTrackMinTimestamp() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 1L, 0L, "asdf", "2093j");
        assertThat(buffer.minTimestamp(), is(1L));
        putRecord(buffer, context, 0L, 0L, "zxcv", "3gon4i");
        assertThat(buffer.minTimestamp(), is(0L));
        cleanup(context, buffer);
    }

    @Test
    public void shouldEvictOldestAndUpdateSizeAndCountAndMinTimestamp() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        putRecord(buffer, context, 1L, 0L, "zxcv", "o23i4");
        assertThat(buffer.numRecords(), is(1));
        assertThat(buffer.bufferSize(), is(50L));
        assertThat(buffer.minTimestamp(), is(1L));

        putRecord(buffer, context, 0L, 0L, "asdf", "3ng");
        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.bufferSize(), is(98L));
        assertThat(buffer.minTimestamp(), is(0L));

        final AtomicInteger callbackCount = new AtomicInteger(0);
        buffer.evictWhile(() -> true, kv -> {
            switch (callbackCount.incrementAndGet()) {
                case 1: {
                    assertThat(kv.key(), is("asdf"));
                    assertThat(buffer.numRecords(), is(2));
                    assertThat(buffer.bufferSize(), is(98L));
                    assertThat(buffer.minTimestamp(), is(0L));
                    break;
                }
                case 2: {
                    assertThat(kv.key(), is("zxcv"));
                    assertThat(buffer.numRecords(), is(1));
                    assertThat(buffer.bufferSize(), is(50L));
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

    @Test
    public void shouldReturnUndefinedOnPriorValueForNotBufferedKey() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        assertThat(buffer.priorValueForBuffered("ASDF"), is(Maybe.undefined()));
    }

    @Test
    public void shouldReturnPriorValueForBufferedKey() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        final ProcessorRecordContext recordContext = getContext(0L);
        context.setRecordContext(recordContext);
        buffer.put(1L, "A", new Change<>("new-value", "old-value"), recordContext);
        buffer.put(1L, "B", new Change<>("new-value", null), recordContext);
        assertThat(buffer.priorValueForBuffered("A"), is(Maybe.defined(ValueAndTimestamp.make("old-value", -1))));
        assertThat(buffer.priorValueForBuffered("B"), is(Maybe.defined(null)));
    }

    @Test
    public void shouldFlush() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 2L, 0L, "asdf", "2093j");
        putRecord(buffer, context, 1L, 1L, "zxcv", "3gon4i");
        putRecord(buffer, context, 0L, 2L, "deleteme", "deadbeef");

        // replace "deleteme" with a tombstone
        buffer.evictWhile(() -> buffer.minTimestamp() < 1, kv -> { });

        // flush everything to the changelog
        buffer.flush();

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
                        final byte[] serializedValue = pr.value();
                        final ByteBuffer valueBuffer = ByteBuffer.wrap(serializedValue);
                        final BufferValue contextualRecord = BufferValue.deserialize(valueBuffer);
                        final long timestamp = valueBuffer.getLong();
                        niceValue = new KeyValue<>(timestamp, contextualRecord);
                    }

                    return new ProducerRecord<>(pr.topic(),
                                                pr.partition(),
                                                pr.timestamp(),
                                                new String(pr.key(), UTF_8),
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
                                 V_2_CHANGELOG_HEADERS
            ),
            new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
                                 0,
                                 null,
                                 "asdf",
                                 new KeyValue<>(2L, getBufferValue("2093j", 0)),
                                 V_2_CHANGELOG_HEADERS
            )
        )));

        cleanup(context, buffer);
    }

    @Test
    public void shouldRestoreOldFormat() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

        final Serializer<Change<String>> serializer = FullChangeSerde.castOrWrap(Serdes.String()).serializer();

        final byte[] todeleteValue = serializer.serialize(null, new Change<>("doomed", null));
        final byte[] asdfValue = serializer.serialize(null, new Change<>("qwer", null));
        final byte[] zxcvValue1 = serializer.serialize(null, new Change<>("eo4im", "previous"));
        final byte[] zxcvValue2 = serializer.serialize(null, new Change<>("next", "eo4im"));
        stateRestoreCallback.restoreBatch(asList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 0,
                                 0,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + todeleteValue.length).putLong(0L).put(todeleteValue).array()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 1,
                                 1,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 -1,
                                 "asdf".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + asdfValue.length).putLong(2L).put(asdfValue).array()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 2,
                                 2,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + zxcvValue1.length).putLong(1L).put(zxcvValue1).array()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + zxcvValue2.length).putLong(1L).put(zxcvValue2).array())
        ));

        assertThat(buffer.numRecords(), is(3));
        assertThat(buffer.minTimestamp(), is(0L));
        assertThat(buffer.bufferSize(), is(196L));

        stateRestoreCallback.restoreBatch(singletonList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 null)
        ));

        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.minTimestamp(), is(1L));
        assertThat(buffer.bufferSize(), is(131L));

        assertThat(buffer.priorValueForBuffered("todelete"), is(Maybe.undefined()));
        assertThat(buffer.priorValueForBuffered("asdf"), is(Maybe.defined(null)));
        assertThat(buffer.priorValueForBuffered("zxcv"), is(Maybe.defined(ValueAndTimestamp.make("previous", -1))));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        final List<Eviction<String, String>> evicted = new LinkedList<>();
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

    @Test
    public void shouldRestoreV1Format() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

        final RecordHeaders v1FlagHeaders = new RecordHeaders(new Header[] {new RecordHeader("v", new byte[] {(byte) 1})});

        final byte[] todeleteValue = getContextualRecord("doomed", 0).serialize(0).array();
        final byte[] asdfValue = getContextualRecord("qwer", 1).serialize(0).array();
        final FullChangeSerde<String> fullChangeSerde = FullChangeSerde.castOrWrap(Serdes.String());
        final byte[] zxcvValue1 = new ContextualRecord(
            fullChangeSerde.serializer().serialize(null, new Change<>("3o4im", "previous")),
            getContext(2L)
        ).serialize(0).array();
        final FullChangeSerde<String> fullChangeSerde1 = FullChangeSerde.castOrWrap(Serdes.String());
        final byte[] zxcvValue2 = new ContextualRecord(
            fullChangeSerde1.serializer().serialize(null, new Change<>("next", "3o4im")),
            getContext(3L)
        ).serialize(0).array();
        stateRestoreCallback.restoreBatch(asList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 0,
                                 999,
                                 TimestampType.CREATE_TIME,
                                 -1L,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + todeleteValue.length).putLong(0L).put(todeleteValue).array(),
                                 v1FlagHeaders),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 1,
                                 9999,
                                 TimestampType.CREATE_TIME,
                                 -1L,
                                 -1,
                                 -1,
                                 "asdf".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + asdfValue.length).putLong(2L).put(asdfValue).array(),
                                 v1FlagHeaders),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 2,
                                 99,
                                 TimestampType.CREATE_TIME,
                                 -1L,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + zxcvValue1.length).putLong(1L).put(zxcvValue1).array(),
                                 v1FlagHeaders),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 3,
                                 100,
                                 TimestampType.CREATE_TIME,
                                 -1L,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + zxcvValue2.length).putLong(1L).put(zxcvValue2).array(),
                                 v1FlagHeaders)
        ));

        assertThat(buffer.numRecords(), is(3));
        assertThat(buffer.minTimestamp(), is(0L));
        assertThat(buffer.bufferSize(), is(166L));

        stateRestoreCallback.restoreBatch(singletonList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CREATE_TIME,
                                 -1L,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 null)
        ));

        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.minTimestamp(), is(1L));
        assertThat(buffer.bufferSize(), is(111L));

        assertThat(buffer.priorValueForBuffered("todelete"), is(Maybe.undefined()));
        assertThat(buffer.priorValueForBuffered("asdf"), is(Maybe.defined(null)));
        assertThat(buffer.priorValueForBuffered("zxcv"), is(Maybe.defined(ValueAndTimestamp.make("previous", -1))));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        final List<Eviction<String, String>> evicted = new LinkedList<>();
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

    @Test
    public void shouldRestoreV2Format() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

        final RecordHeaders v2FlagHeaders = new RecordHeaders(new Header[] {new RecordHeader("v", new byte[] {(byte) 2})});

        final byte[] todeleteValue = getBufferValue("doomed", 0).serialize(0).array();
        final byte[] asdfValue = getBufferValue("qwer", 1).serialize(0).array();
        final FullChangeSerde<String> fullChangeSerde = FullChangeSerde.castOrWrap(Serdes.String());
        final byte[] zxcvValue1 =
            new BufferValue(
                new ContextualRecord(
                    fullChangeSerde.serializer().serialize(null, new Change<>("3o4im", "IGNORED")),
                    getContext(2L)
                ),
                Serdes.String().serializer().serialize(null, "previous")
            ).serialize(0).array();
        final FullChangeSerde<String> fullChangeSerde1 = FullChangeSerde.castOrWrap(Serdes.String());
        final byte[] zxcvValue2 =
            new BufferValue(
                new ContextualRecord(
                    fullChangeSerde1.serializer().serialize(null, new Change<>("next", "3o4im")),
                    getContext(3L)
                ),
                Serdes.String().serializer().serialize(null, "previous")
            ).serialize(0).array();
        stateRestoreCallback.restoreBatch(asList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 0,
                                 999,
                                 TimestampType.CREATE_TIME,
                                 -1L,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + todeleteValue.length).put(todeleteValue).putLong(0L).array(),
                                 v2FlagHeaders),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 1,
                                 9999,
                                 TimestampType.CREATE_TIME,
                                 -1L,
                                 -1,
                                 -1,
                                 "asdf".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + asdfValue.length).put(asdfValue).putLong(2L).array(),
                                 v2FlagHeaders),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 2,
                                 99,
                                 TimestampType.CREATE_TIME,
                                 -1L,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + zxcvValue1.length).put(zxcvValue1).putLong(1L).array(),
                                 v2FlagHeaders),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 2,
                                 100,
                                 TimestampType.CREATE_TIME,
                                 -1L,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + zxcvValue2.length).put(zxcvValue2).putLong(1L).array(),
                                 v2FlagHeaders)
        ));

        assertThat(buffer.numRecords(), is(3));
        assertThat(buffer.minTimestamp(), is(0L));
        assertThat(buffer.bufferSize(), is(166L));

        stateRestoreCallback.restoreBatch(singletonList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CREATE_TIME,
                                 -1L,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 null)
        ));

        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.minTimestamp(), is(1L));
        assertThat(buffer.bufferSize(), is(111L));

        assertThat(buffer.priorValueForBuffered("todelete"), is(Maybe.undefined()));
        assertThat(buffer.priorValueForBuffered("asdf"), is(Maybe.defined(null)));
        assertThat(buffer.priorValueForBuffered("zxcv"), is(Maybe.defined(ValueAndTimestamp.make("previous", -1))));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        final List<Eviction<String, String>> evicted = new LinkedList<>();
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

    @Test
    public void shouldNotRestoreUnrecognizedVersionRecord() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

        final RecordHeaders unknownFlagHeaders = new RecordHeaders(new Header[] {new RecordHeader("v", new byte[] {(byte) -1})});

        final byte[] todeleteValue = getBufferValue("doomed", 0).serialize(0).array();
        try {
            stateRestoreCallback.restoreBatch(singletonList(
                new ConsumerRecord<>("changelog-topic",
                                     0,
                                     0,
                                     999,
                                     TimestampType.CREATE_TIME,
                                     -1L,
                                     -1,
                                     -1,
                                     "todelete".getBytes(UTF_8),
                                     ByteBuffer.allocate(Long.BYTES + todeleteValue.length).putLong(0L).put(todeleteValue).array(),
                                     unknownFlagHeaders)
            ));
            fail("expected an exception");
        } catch (final IllegalArgumentException expected) {
            // nothing to do.
        } finally {
            cleanup(context, buffer);
        }
    }

    private static void putRecord(final TimeOrderedKeyValueBuffer<String, String> buffer,
                                  final MockInternalProcessorContext context,
                                  final long streamTime,
                                  final long recordTimestamp,
                                  final String key,
                                  final String value) {
        final ProcessorRecordContext recordContext = getContext(recordTimestamp);
        context.setRecordContext(recordContext);
        buffer.put(streamTime, key, new Change<>(value, null), recordContext);
    }

    private static BufferValue getBufferValue(final String value, final long timestamp) {
        final ContextualRecord contextualRecord = getContextualRecord(value, timestamp);
        return new BufferValue(contextualRecord, null);
    }

    private static ContextualRecord getContextualRecord(final String value, final long timestamp) {
        final FullChangeSerde<String> fullChangeSerde = FullChangeSerde.castOrWrap(Serdes.String());
        return new ContextualRecord(
            fullChangeSerde.serializer().serialize(null, new Change<>(value, null)),
            getContext(timestamp)
        );
    }

    private static ProcessorRecordContext getContext(final long recordTimestamp) {
        return new ProcessorRecordContext(recordTimestamp, 0, 0, "topic", null);
    }
}
