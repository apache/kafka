package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowStore;

public interface WindowedKTable<K, V> extends AbstractTable<Windowed<K>, V, WindowStore<Bytes, byte[]>> {
    
}
