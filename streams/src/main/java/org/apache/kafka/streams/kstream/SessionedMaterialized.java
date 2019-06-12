package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.SessionStore;

public class SessionedMaterialized<K, V> extends Materialized<K, V, SessionStore<Bytes, byte[]>> {
    /**
     * Copy constructor.
     *
     * @param materialized the {@link Materialized} instance to copy.
     */
    protected SessionedMaterialized(Materialized<K, V, SessionStore<Bytes, byte[]>> materialized) {
        super(materialized);
    }
}
