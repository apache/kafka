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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.test.MockInternalNewProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RocksDBTimeOrderedKeyValueBufferTest {
    public RocksDBTimeOrderedKeyValueBuffer<String, String> buffer;
    @Mock
    public SerdeGetter serdeGetter;
    public InternalProcessorContext<String, String> context;
    public StreamsMetricsImpl streamsMetrics;
    @Mock
    public Sensor sensor;
    public long offset;

    @Before
    public void setUp() {
        when(serdeGetter.keySerde()).thenReturn(new Serdes.StringSerde());
        when(serdeGetter.valueSerde()).thenReturn(new Serdes.StringSerde());
        final Metrics metrics = new Metrics();
        offset = 0;
        streamsMetrics = new StreamsMetricsImpl(metrics, "test-client", StreamsConfig.METRICS_LATEST, new MockTime());
        context = new MockInternalNewProcessorContext<>(StreamsTestUtils.getStreamsConfig(), new TaskId(0, 0), TestUtils.tempDirectory());
    }

    public void createJoin(final Duration grace) {
        final RocksDBTimeOrderedKeyValueSegmentedBytesStore store = new RocksDbTimeOrderedKeyValueBytesStoreSupplier("testing",  100).get();
        buffer = new RocksDBTimeOrderedKeyValueBuffer<>(store, grace, "testing");
        buffer.setSerdesIfNull(serdeGetter);
        store.init((StateStoreContext) context, store);
        buffer.init((StateStoreContext) context, store);
    }

    private void pipeRecord(final String key, final String value, final long time) {
        final Record<String, String> record = new Record<>(key, value, time);
        context.setRecordContext(new ProcessorRecordContext(time, offset++, 0, "testing", new RecordHeaders()));
        buffer.put(time, record, context.recordContext());
    }

    @Test
    public void shouldAddAndEvictRecord() {
        createJoin(Duration.ZERO);
        final AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(1));
    }

    @Test
    public void shouldAddAndEvictRecordTwice() {
        createJoin(Duration.ZERO);
        final AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(1));
        pipeRecord("2", "0", 1L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(2));
    }

    @Test
    public void shouldAddAndEvictRecordTwiceWithNonZeroGrace() {
        createJoin(Duration.ofMillis(1));
        final AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(0));
        pipeRecord("2", "0", 1L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(1));
    }

    @Test
    public void shouldAddRecordsTwiceAndEvictRecordsOnce() {
        createJoin(Duration.ZERO);
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
        createJoin(Duration.ZERO);
        final AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 1L);
        buffer.evictWhile(() -> buffer.numRecords() > 1, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(0));
        pipeRecord("2", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(1));
    }

    @Test
    public void shouldDropLateRecordsWithNonZeroGrace() {
        createJoin(Duration.ofMillis(1));
        final AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 2L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(0));
        pipeRecord("2", "0", 1L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(1));
        pipeRecord("2", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(1));
    }

    @Test
    public void shouldHandleCollidingKeys() {
        createJoin(Duration.ofMillis(1));
        final AtomicInteger count = new AtomicInteger(0);
        pipeRecord("2", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(0));
        pipeRecord("2", "2", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(0));
        pipeRecord("1", "0", 7L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(2));
    }
}