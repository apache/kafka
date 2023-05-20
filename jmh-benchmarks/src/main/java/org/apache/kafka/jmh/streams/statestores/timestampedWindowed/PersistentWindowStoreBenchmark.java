package org.apache.kafka.jmh.streams.statestores.timestampedWindowed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.jmh.streams.statestores.windowed.WindowedStoreBenchmark;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.state.Stores;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

import java.time.Duration;

public class PersistentWindowStoreBenchmark extends WindowedStoreBenchmark {

    @Setup(Level.Trial)
    public void setUp() {
        generateWindowedKeys();
        Stores.timestampedWindowStoreBuilder(Stores.persistentWindowStore("rocksdb", Duration.ofDays(1), Duration.ofMinutes(WINDOW_SIZE), false),
            Serdes.String(),
            Serdes.String())
            .withCachingDisabled()
            .withLoggingDisabled()
            .build();
        ProcessorContextImpl context = (ProcessorContextImpl) setupProcessorContext();
        this.windowStore.init((StateStoreContext) context, this.windowStore);
        putWindowedKeys();
    }
}
