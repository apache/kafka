package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowStore;

public class TimeWindowedMaterialized<K, V> extends Materialized<K, V, WindowStore<Bytes, byte[]>> {

    /**
     * Copy constructor.
     *
     * @param materialized the {@link Materialized} instance to copy.
     */
    protected TimeWindowedMaterialized(Materialized<K, V, WindowStore<Bytes, byte[]>> materialized) {
        super(materialized);
    }
}
