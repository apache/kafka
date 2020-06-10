package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.TransformerSupplier;

/**
 * Shared functions to handle verifications of a valid {@link org.apache.kafka.streams.kstream.KStream}.
 */
final class KStreamUtil {

    private KStreamUtil() {}

    /**
     * @throws IllegalArgumentException if the same transformer instance is obtained each time
     */
    static void checkTransformerSupplier(final TransformerSupplier<?, ?, ?> supplier) {
        if (supplier.get() == supplier.get()) {
            throw new IllegalArgumentException("TransformerSupplier generates single transformer reference. Supplier " +
                    "pattern violated.");
        }
    }

}
