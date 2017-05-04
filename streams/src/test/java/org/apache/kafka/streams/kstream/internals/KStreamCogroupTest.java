package org.apache.kafka.streams.kstream.internals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KStreamCogroupTest {

    private boolean sendOldValues = false;
    private final KTableValueGetterSupplier<String, Long> PARENT_VALUE_GETTER_SUPPLIER = new KTableValueGetterSupplier<String, Long>() {
            @Override
            public KTableValueGetter<String, Long> get() {
                return null;
            }
    
            @Override
            public String[] storeNames() {
                return null;
            }
        };
    private final KStreamAggProcessorSupplier PARENT = new KStreamAggProcessorSupplier<String, String, Change<Long>, Long>() {
            @Override
            public Processor<String, Change<Long>> get() {
                return null;
            }
    
            @Override
            public KTableValueGetterSupplier<String, Long> view() {
                return PARENT_VALUE_GETTER_SUPPLIER;
            }
    
            @Override
            public void enableSendingOldValues() {
                sendOldValues = true;
            }
        };
    private final KStreamCogroup<String, Long> KSTREAM_COGROUP = new KStreamCogroup<String, Long>(Collections.singleton(PARENT));
    private final Processor<String, Change<Long>> processor = KSTREAM_COGROUP.get();
    private MockProcessorContext context;
    private List<KeyValue> results = new ArrayList<>();

    @Before
    public void setup() {
        context = new MockProcessorContext(null, Serdes.String(), Serdes.Long(), new NoOpRecordCollector(), new ThreadCache("testCache", 100000, new MockStreamsMetrics(new Metrics()))) {
                @Override
                public <K, V> void forward(final K key, final V value) {
                    results.add(KeyValue.pair(key, value));
                }
            };
        processor.init(context);
    }

    @After
    public void tearDown() {
        results.clear();
        sendOldValues = false;
    }

    @Test
    public void shouldEnableSendingOldValuesOfParent() {
        KSTREAM_COGROUP.enableSendingOldValues();
        assertTrue(sendOldValues);
    }

    @Test
    public void shouldReturnViewOfParent() {
        final KTableValueGetterSupplier<String, Long> valueGetterSupplier = KSTREAM_COGROUP.view();
        assertEquals(PARENT_VALUE_GETTER_SUPPLIER, valueGetterSupplier);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPassChangeWithOldValueRemoved() {
        processor.process("key", new Change<>(1L, 0L));
        assertEquals(new KeyValue<>("key", new Change<>(1L, null)), (KeyValue<String, Change<Long>>) results.get(0));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldPassChangeUnchanged() {
        KSTREAM_COGROUP.enableSendingOldValues();
        processor.process("key", new Change<>(1L, 0L));
        assertEquals(new KeyValue<>("key", new Change<>(1L, 0L)), (KeyValue<String, Change<Long>>) results.get(0));
    }
}
