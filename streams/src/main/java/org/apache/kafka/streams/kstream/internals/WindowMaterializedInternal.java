package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.WindowedMaterialized;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;

public class WindowMaterializedInternal<K, V, S extends StateStore> extends WindowedMaterialized<K, V, S> {


    /**
     * Copy constructor.
     *
     * @param materialized the {@link Materialized} instance to copy.
     */
    protected WindowMaterializedInternal(Materialized<Windowed<K>, V, S> materialized) {
        super(materialized);
    }
}
