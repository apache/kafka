package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

public class KeyValueMaterializedInternal<K, V> extends MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> {

    public KeyValueMaterializedInternal(Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        super(materialized);
    }
}
