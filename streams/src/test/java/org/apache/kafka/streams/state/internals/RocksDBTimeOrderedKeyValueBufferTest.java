package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.internals.Change;
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
    public Duration grace;
    @Mock
    public SerdeGetter serdeGetter;
    public InternalProcessorContext<String, String> context;
    public StreamsMetricsImpl streamsMetrics;
    @Mock
    public Sensor sensor;

    @Before
    public void setUp() {
        grace = Duration.ZERO;
        final RocksDBTimeOrderedKeyValueSegmentedBytesStore store = new RocksDbTimeOrderedKeyValueBytesStoreSupplier("testing",  1,false).get();
        buffer = new RocksDBTimeOrderedKeyValueBuffer<>(store, grace, "testing");
        when(serdeGetter.keySerde()).thenReturn(new Serdes.StringSerde());
        when(serdeGetter.valueSerde()).thenReturn(new Serdes.StringSerde());
        buffer.setSerdesIfNull(serdeGetter);
        final Metrics metrics = new Metrics();
        streamsMetrics = new StreamsMetricsImpl(metrics, "test-client", StreamsConfig.METRICS_LATEST, new MockTime());
        context = new MockInternalNewProcessorContext<>(StreamsTestUtils.getStreamsConfig(), new TaskId(0,0), TestUtils.tempDirectory());
        store.init((StateStoreContext) context, store);
        buffer.init((StateStoreContext) context, store);
    }

    private void pipeRecord(final String key, final String value, final long time) {
        Record<String, Change<String>> record = new Record<>(key, new Change<>(value, value), time);
        context.setRecordContext(new ProcessorRecordContext(time, 30, 0, "testing", new RecordHeaders()));
        buffer.put(time, record, context.recordContext());
    }

    @Test
    public void shouldAddAndEvictRecord() {
        AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(1));
    }

    @Test
    public void shouldAddAndEvictRecordTwice() {
        AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(1));
        pipeRecord("2", "0", 1L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(2));
    }

    @Test
    public void shouldAddRecrodsTwiceAndEvictRecordsOnce() {
        AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 1, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(0));
        pipeRecord("2", "0", 1L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(2));
    }

    @Test
    public void shouldDropLateRecords() {
        AtomicInteger count = new AtomicInteger(0);
        pipeRecord("1", "0", 1L);
        buffer.evictWhile(() -> buffer.numRecords() > 1, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(0));
        pipeRecord("2", "0", 0L);
        buffer.evictWhile(() -> buffer.numRecords() > 0, r -> count.getAndIncrement());
        assertThat(count.get(), equalTo(1));
    }
}