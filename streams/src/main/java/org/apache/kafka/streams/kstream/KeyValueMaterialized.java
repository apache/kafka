package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

public class KeyValueMaterialized<K, V> extends Materialized<K, V, KeyValueStore<Bytes, byte[]>> {
    /**
     * Copy constructor.
     *
     * @param materialized the {@link Materialized} instance to copy.
     */
    protected KeyValueMaterialized(Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        super(materialized);
    }
}
