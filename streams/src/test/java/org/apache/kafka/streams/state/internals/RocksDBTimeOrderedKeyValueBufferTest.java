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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class RocksDBTimeOrderedKeyValueBufferTest {
    public RocksDBTimeOrderedKeyValueBuffer<String, String> buffer;
    @Mock
    public SerdeGetter serdeGetter;
    public InternalProcessorContext<String, String> context;
    public StreamsMetricsImpl streamsMetrics;
    @Mock
    public Sensor sensor;
    public long offset;

    @BeforeEach
    public void setUp() {
        final Metrics metrics = new Metrics();
        offset = 0;
        streamsMetrics = new StreamsMetricsImpl(metrics, "test-client", new MockTime());
        context = new MockInternalProcessorContext<>(StreamsTestUtils.getStreamsConfig(), new TaskId(0, 0), TestUtils.tempDirectory());
    }

    private void createBuffer(final Duration grace, final Serde<String> serde) {
        final RocksDBTimeOrderedKeyValueBytesStore store = new RocksDBTimeOrderedKeyValueBytesStoreSupplier("testing").get();

        buffer = new RocksDBTimeOrderedKeyValueBuffer<>(store, serde, serde, grace, "testing", false);
        buffer.setSerdesIfNull(serdeGetter);
        buffer.init(context, store);
    }

    private boolean pipeRecord(final String key, final String value, final long time) {
        final Record<String, String> record = new Record<>(key, value, time);
        context.setRecordContext(new ProcessorRecordContext(time, offset++, 0, "testing", new RecordHeaders()));
        return buffer.put(time, record, context.recordContext());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldReturnIfRecordWasAdded() {
        when(serdeGetter.keySerde()).thenReturn((Serde) new Serdes.StringSerde());
        when(serdeGetter.valueSerde()).thenReturn((Serde) new Serdes.StringSerde());
        createBuffer(Duration.ofMillis(1), null);
        assertThat(pipeRecord("K", "V", 2L), equalTo(true));
        assertThat(pipeRecord("K", "V", 0L), equalTo(false));
    }

    @Test
    public void shouldPutInBufferAndUpdateFields() {
        createBuffer(Duration.ofMinutes(1), Serdes.String());
        assertNumSizeAndTimestamp(buffer, 0, Long.MAX_VALUE, 0);
        pipeRecord("1", "0", 0L);
        assertNumSizeAndTimestamp(buffer, 1, 0, 42);
        pipeRecord("3", "0", 2L);
        assertNumSizeAndTimestamp(buffer, 2, 0, 84);
    }

    @Test
    public void shouldAddAndEvictRecord() {
        createBuffer(Duration.ZERO, Serdes.String());
        final AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 0L);
        assertNumSizeAndTimestamp(buffer, 1, 0, 42);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertNumSizeAndTimestamp(buffer, 0, Long.MAX_VALUE, 0);
        assertThat(count.get(), equalTo(1));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldAddAndEvictRecordTwice() {
        when(serdeGetter.keySerde()).thenReturn((Serde) new Serdes.StringSerde());
        when(serdeGetter.valueSerde()).thenReturn((Serde) new Serdes.StringSerde());
        createBuffer(Duration.ZERO, null);
        final AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 0L);
        assertNumSizeAndTimestamp(buffer, 1, 0, 42);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertNumSizeAndTimestamp(buffer, 0, Long.MAX_VALUE, 0);
        assertThat(count.get(), equalTo(1));
        pipeRecord("2", "0", 1L);
        assertNumSizeAndTimestamp(buffer, 1, 1, 42);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertNumSizeAndTimestamp(buffer, 0, Long.MAX_VALUE, 0);
        assertThat(count.get(), equalTo(2));
    }

    @Test
    public void shouldAddAndEvictRecordTwiceWithNonZeroGrace() {
        createBuffer(Duration.ofMillis(1), Serdes.String());
        final AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertNumSizeAndTimestamp(buffer, 1, 0, 42);
        assertThat(count.get(), equalTo(0));
        pipeRecord("2", "0", 1L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertNumSizeAndTimestamp(buffer, 1, 1, 42);
        assertThat(count.get(), equalTo(1));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldAddRecordsTwiceAndEvictRecordsOnce() {
        when(serdeGetter.keySerde()).thenReturn((Serde) new Serdes.StringSerde());
        when(serdeGetter.valueSerde()).thenReturn((Serde) new Serdes.StringSerde());
        createBuffer(Duration.ZERO, null);
        final AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 1, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(0));
        pipeRecord("2", "0", 1L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(2));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldDropLateRecords() {
        when(serdeGetter.keySerde()).thenReturn((Serde) new Serdes.StringSerde());
        when(serdeGetter.valueSerde()).thenReturn((Serde) new Serdes.StringSerde());
        createBuffer(Duration.ZERO, null);
        pipeRecord("1", "0", 1L);
        assertNumSizeAndTimestamp(buffer, 1, 1, 42);
        pipeRecord("2", "0", 0L);
        assertNumSizeAndTimestamp(buffer, 1, 1, 42);
    }

    @Test
    public void shouldDropLateRecordsWithNonZeroGrace() {
        createBuffer(Duration.ofMillis(1), Serdes.String());
        pipeRecord("1", "0", 2L);
        assertNumSizeAndTimestamp(buffer, 1, 2, 42);
        pipeRecord("2", "0", 1L);
        assertNumSizeAndTimestamp(buffer, 2, 1, 84);
        pipeRecord("3", "0", 0L);
        assertNumSizeAndTimestamp(buffer, 2, 1, 84);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldHandleCollidingKeys() {
        when(serdeGetter.keySerde()).thenReturn((Serde) new Serdes.StringSerde());
        when(serdeGetter.valueSerde()).thenReturn((Serde) new Serdes.StringSerde());
        createBuffer(Duration.ofMillis(1), null);
        final AtomicInteger count = new AtomicInteger(0);
        pipeRecord("2", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(0));
        assertNumSizeAndTimestamp(buffer, 1, 0, 42);
        pipeRecord("2", "2", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(0));
        assertNumSizeAndTimestamp(buffer, 2, 0, 84);
        pipeRecord("1", "0", 7L);
        assertNumSizeAndTimestamp(buffer, 3, 0, 126);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(2));
        assertNumSizeAndTimestamp(buffer, 1, 7, 42);
    }

    @Test
    public void shouldDeserializeWithPutTimeHeadersEvenAfterContextMutation() {
        final HeaderCapturingSerde serde = new HeaderCapturingSerde();
        createBuffer(Duration.ZERO, serde);
        final RecordHeaders putHeaders = new RecordHeaders(new Header[]{
            new RecordHeader("at-put", "first".getBytes(StandardCharsets.UTF_8))
        });
        // Give the record its own headers, distinct from the context headers, so the assertions below prove that
        // eviction deserialization reads from the stored recordContext snapshot rather than from record.headers().
        final RecordHeaders recordHeaders = new RecordHeaders(new Header[]{
            new RecordHeader("on-record", "rec".getBytes(StandardCharsets.UTF_8))
        });
        context.setRecordContext(new ProcessorRecordContext(0L, offset++, 0, "testing", putHeaders));
        buffer.put(0L, new Record<>("k", "v", 0L, recordHeaders), context.recordContext());

        // Simulate the processor moving on to another record with different headers before eviction runs.
        final RecordHeaders laterHeaders = new RecordHeaders(new Header[]{
            new RecordHeader("at-evict", "second".getBytes(StandardCharsets.UTF_8))
        });
        context.setRecordContext(new ProcessorRecordContext(0L, offset++, 0, "testing", laterHeaders));

        final List<TimeOrderedKeyValueBuffer.Eviction<String, String>> evicted = new ArrayList<>();
        buffer.evictWhile(() -> buffer.numRecords() > 0, evicted::add);

        assertThat(evicted.size(), is(1));
        // The key/value deserializers must see the headers captured at put time, not the mutated context headers.
        assertThat(serde.capturedHeaders, hasItem(putHeaders));
        assertThat(serde.capturedHeaders, everyItem(is(putHeaders)));
        assertThat(evicted.get(0).recordContext().headers(), is(putHeaders));
    }

    @Test
    public void shouldNotBeAffectedByProcessorContextHeaderMutationBetweenPutAndEvict() {
        final HeaderCapturingSerde serde = new HeaderCapturingSerde();
        createBuffer(Duration.ofMillis(1), serde);
        final RecordHeaders putHeaders = new RecordHeaders(new Header[]{
            new RecordHeader("at-put", "first".getBytes(StandardCharsets.UTF_8))
        });
        // Give the record its own headers, distinct from the context headers, so the assertions below prove that
        // eviction deserialization reads from the stored recordContext snapshot rather than from record.headers().
        final RecordHeaders recordHeaders = new RecordHeaders(new Header[]{
            new RecordHeader("on-record", "rec".getBytes(StandardCharsets.UTF_8))
        });
        context.setRecordContext(new ProcessorRecordContext(0L, offset++, 0, "testing", putHeaders));
        buffer.put(0L, new Record<>("k", "v", 0L, recordHeaders), context.recordContext());

        // Simulate the processor moving on to handle a different record with different headers
        // before the grace period expires and eviction runs.
        final RecordHeaders laterHeaders = new RecordHeaders(new Header[]{
            new RecordHeader("at-evict", "second".getBytes(StandardCharsets.UTF_8))
        });
        final RecordHeaders triggerRecordHeaders = new RecordHeaders(new Header[]{
            new RecordHeader("on-trigger-record", "trig".getBytes(StandardCharsets.UTF_8))
        });
        context.setRecordContext(new ProcessorRecordContext(10L, offset++, 0, "testing", laterHeaders));
        // Advance stream time past the grace period for the original record.
        buffer.put(10L, new Record<>("trigger", "v", 10L, triggerRecordHeaders), context.recordContext());

        final List<TimeOrderedKeyValueBuffer.Eviction<String, String>> evicted = new ArrayList<>();
        buffer.evictWhile(() -> true, evicted::add);

        // Only the original "k" record at t=0 falls outside the grace window of t=10.
        assertThat(evicted.size(), is(1));
        assertThat(evicted.get(0).key(), is("k"));
        // The deserializers for the evicted record must see its put-time headers, not the later context headers.
        assertThat(serde.capturedHeaders, hasItem(putHeaders));
        assertThat(serde.capturedHeaders, everyItem(is(putHeaders)));
        assertThat(evicted.get(0).recordContext().headers(), is(putHeaders));
    }

    /**
     * A {@link Serde} whose deserializer records the {@link Headers} it is handed on each call, so tests can assert
     * which headers reached the key/value deserializers during eviction. Serialization behaves like a plain String serde.
     */
    private static final class HeaderCapturingSerde implements Serde<String> {
        private final List<Headers> capturedHeaders = new ArrayList<>();

        @Override
        public Serializer<String> serializer() {
            return new StringSerializer();
        }

        @Override
        public Deserializer<String> deserializer() {
            return new StringDeserializer() {
                @Override
                public String deserialize(final String topic, final Headers headers, final byte[] data) {
                    capturedHeaders.add(headers);
                    return super.deserialize(topic, data);
                }
            };
        }
    }

    private void assertNumSizeAndTimestamp(final TimeOrderedKeyValueBuffer<String, String, String> buffer,
                                           final int num,
                                           final long time,
                                           final long size) {
        assertThat(buffer.numRecords(), equalTo(num));
        assertThat(buffer.minTimestamp(), equalTo(time));
        assertThat(buffer.bufferSize(), equalTo(size));
    }
}