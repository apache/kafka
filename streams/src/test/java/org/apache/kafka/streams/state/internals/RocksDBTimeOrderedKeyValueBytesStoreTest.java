package org.apache.kafka.streams.state.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.WriteBatch;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RocksDBTimeOrderedKeyValueBytesStoreTest {

    private InternalMockProcessorContext context;
    private RocksDBTimeOrderedKeyValueBytesStore bytesStore;
    private File stateDir;    final String storeName = "bytes-store";
    private final static String METRICS_SCOPE = "metrics-scope";
    private final String topic = "changelog";


    @BeforeEach
    public void before() {
        bytesStore = new RocksDBTimeOrderedKeyValueBytesStore(storeName, METRICS_SCOPE);

        stateDir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
            stateDir,
            Serdes.String(),
            Serdes.Long(),
            new MockRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
        bytesStore.init((StateStoreContext) context, bytesStore);
    }

    @AfterEach
    public void close() {
        bytesStore.close();
    }

    @Test
    public void shouldCreateWriteBatches() {
        final String key = "a";
        final Collection<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(key, 0, 0L).get(), serializeValue(50L)));
        records.add(new ConsumerRecord<>("", 0, 0L, serializeKey(key, 1, 1L).get(), serializeValue(100L)));
        final Map<KeyValueSegment, WriteBatch> writeBatchMap = bytesStore.getWriteBatches(records);
        assertEquals(1, writeBatchMap.size());

        for (final WriteBatch batch : writeBatchMap.values()) {
            // 2 includes base and index record
            assertEquals(2, batch.count());
        }
    }

    private byte[] serializeValue(final Long value) {
        final Serde<Long> valueSerde = new Serdes.LongSerde();
        final byte[] valueBytes = valueSerde.serializer().serialize(topic, value);
        final BufferValue buffered = new BufferValue(null, null, valueBytes, new ProcessorRecordContext(0, 0, 0, topic, new RecordHeaders()));
        return buffered.serialize(0).array();
    }

    private Bytes serializeKey(final String key, final int seqnum, final long timestamp) {
        final Serde<String> keySerde = new Serdes.StringSerde();
        return Bytes.wrap(
            PrefixedWindowKeySchemas.TimeFirstWindowKeySchema.toStoreKeyBinary(keySerde.serializer().serialize(topic, key),
                timestamp,
                seqnum).get());
    }
}