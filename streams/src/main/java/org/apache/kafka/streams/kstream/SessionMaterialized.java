package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.processor.StateStore;

public class SessionMaterialized<K, V, S extends StateStore> extends Materialized<Windowed<K>, V, S> {
    /**
     * Copy constructor.
     *
     * @param materialized the {@link Materialized} instance to copy.
     */
    protected SessionMaterialized(Materialized<Windowed<K>, V, S> materialized) {
        super(materialized);
    }
}
