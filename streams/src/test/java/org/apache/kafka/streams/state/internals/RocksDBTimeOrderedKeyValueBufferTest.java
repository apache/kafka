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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
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

    @SuppressWarnings({"rawtypes", "unchecked"})
    @BeforeEach
    public void setUp() {
        when(serdeGetter.keySerde()).thenReturn((Serde) new Serdes.StringSerde());
        when(serdeGetter.valueSerde()).thenReturn((Serde) new Serdes.StringSerde());
        final Metrics metrics = new Metrics();
        offset = 0;
        streamsMetrics = new StreamsMetricsImpl(metrics, "test-client", "processId", new MockTime());
        context = new MockInternalProcessorContext<>(StreamsTestUtils.getStreamsConfig(), new TaskId(0, 0), TestUtils.tempDirectory());
    }

    private void createBuffer(final Duration grace) {
        final RocksDBTimeOrderedKeyValueBytesStore store = new RocksDBTimeOrderedKeyValueBytesStoreSupplier("testing").get();

        buffer = new RocksDBTimeOrderedKeyValueBuffer<>(store, grace, "testing", false);
        buffer.setSerdesIfNull(serdeGetter);
        buffer.init(context, store);
    }

    private boolean pipeRecord(final String key, final String value, final long time) {
        final Record<String, String> record = new Record<>(key, value, time);
        context.setRecordContext(new ProcessorRecordContext(time, offset++, 0, "testing", new RecordHeaders()));
        return buffer.put(time, record, context.recordContext());
    }

    @Test
    public void shouldReturnIfRecordWasAdded() {
        createBuffer(Duration.ofMillis(1));
        assertThat(pipeRecord("K", "V", 2L), equalTo(true));
        assertThat(pipeRecord("K", "V", 0L), equalTo(false));
    }

    @Test
    public void shouldPutInBufferAndUpdateFields() {
        createBuffer(Duration.ofMinutes(1));
        assertNumSizeAndTimestamp(buffer, 0, Long.MAX_VALUE, 0);
        pipeRecord("1", "0", 0L);
        assertNumSizeAndTimestamp(buffer, 1, 0, 42);
        pipeRecord("3", "0", 2L);
        assertNumSizeAndTimestamp(buffer, 2, 0, 84);
    }

    @Test
    public void shouldAddAndEvictRecord() {
        createBuffer(Duration.ZERO);
        final AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 0L);
        assertNumSizeAndTimestamp(buffer, 1, 0, 42);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertNumSizeAndTimestamp(buffer, 0, Long.MAX_VALUE, 0);
        assertThat(count.get(), equalTo(1));
    }

    @Test
    public void shouldAddAndEvictRecordTwice() {
        createBuffer(Duration.ZERO);
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
        createBuffer(Duration.ofMillis(1));
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

    @Test
    public void shouldAddRecordsTwiceAndEvictRecordsOnce() {
        createBuffer(Duration.ZERO);
        final AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 1, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(0));
        pipeRecord("2", "0", 1L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(2));
    }

    @Test
    public void shouldDropLateRecords() {
        createBuffer(Duration.ZERO);
        pipeRecord("1", "0", 1L);
        assertNumSizeAndTimestamp(buffer, 1, 1, 42);
        pipeRecord("2", "0", 0L);
        assertNumSizeAndTimestamp(buffer, 1, 1, 42);
    }

    @Test
    public void shouldDropLateRecordsWithNonZeroGrace() {
        createBuffer(Duration.ofMillis(1));
        pipeRecord("1", "0", 2L);
        assertNumSizeAndTimestamp(buffer, 1, 2, 42);
        pipeRecord("2", "0", 1L);
        assertNumSizeAndTimestamp(buffer, 2, 1, 84);
        pipeRecord("3", "0", 0L);
        assertNumSizeAndTimestamp(buffer, 2, 1, 84);
    }

    @Test
    public void shouldHandleCollidingKeys() {
        createBuffer(Duration.ofMillis(1));
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

    private void assertNumSizeAndTimestamp(final TimeOrderedKeyValueBuffer<String, String, String> buffer,
                                           final int num,
                                           final long time,
                                           final long size) {
        assertThat(buffer.numRecords(), equalTo(num));
        assertThat(buffer.minTimestamp(), equalTo(time));
        assertThat(buffer.bufferSize(), equalTo(size));
    }
}