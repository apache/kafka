package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * Shared functions to handle verifications of a valid {@link org.apache.kafka.streams.Topology}.
 */
public final class TopologyUtil {

    private TopologyUtil() {}

    /**
     * @throws TopologyException if the same processor instance is obtained each time
     */
    public static void checkProcessorSupplier(final ProcessorSupplier<?, ?> supplier) {
        if (supplier.get() == supplier.get()) {
            throw new TopologyException("ProcessorSupplier generates single processor reference. Supplier pattern" +
                    " violated.");
        }
    }

}
