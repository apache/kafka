package org.apache.kafka.streams.processor.api;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;

import java.time.Duration;

/**
 * A processor of key-value pair records.
 *
 * @param <KIn> the type of input keys
 * @param <VIn> the type of input values
 * @param <KOut> the type of output keys
 * @param <VOut> the type of output values
 */
public interface Processor<KIn, VIn, KOut, VOut> {

    /**
     * Initialize this processor with the given context. The framework ensures this is called once per processor when the topology
     * that contains it is initialized. When the framework is done with the processor, {@link #close()} will be called on it; the
     * framework may later re-use the processor by calling {@code #init()} again.
     * <p>
     * The provided {@link ProcessorContext context} can be used to access topology and record meta data, to
     * {@link ProcessorContext#schedule(Duration, PunctuationType, Punctuator) schedule} a method to be
     * {@link Punctuator#punctuate(long) called periodically} and to access attached {@link StateStore}s.
     *
     * @param context the context; may not be null
     */
    default void init(final ProcessorContext<KOut, VOut> context) {}

    /**
     * Process the record with the given key and value.
     *
     * @param key the key for the record
     * @param value the value for the record
     */
    void process(KIn key, VIn value);

    /**
     * Close this processor and clean up any resources. Be aware that {@code #close()} is called after an internal cleanup.
     * Thus, it is not possible to write anything to Kafka as underlying clients are already closed. The framework may
     * later re-use this processor by calling {@code #init()} on it again.
     * <p>
     * Note: Do not close any streams managed resources, like {@link StateStore}s here, as they are managed by the library.
     */
    default void close() {}
}
