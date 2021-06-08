package org.apache.kafka.jmh.streams.statestores.windowed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.state.Stores;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

public class PersistentTimestampedKVStoreBenchmark extends WindowedStoreBenchmark {

    @Setup(Level.Trial)
    public void setUp() {
        storeKeys();
        this.timestampedKeyValueStore = Stores.timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("rocks"),
            Serdes.String(),
            Serdes.String())
            .withCachingDisabled()
            .withLoggingDisabled()
            .build();

        ProcessorContextImpl context = (ProcessorContextImpl) setupProcessorContext();
        this.timestampedKeyValueStore.init((StateStoreContext) context, this.timestampedKeyValueStore);
        storeScanKeys();
    }
}
