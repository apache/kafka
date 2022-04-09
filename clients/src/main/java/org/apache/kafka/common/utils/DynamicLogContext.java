package org.apache.kafka.common.utils;

import java.util.function.Supplier;

/**
 * A log context whose prefix may vary across invocations, which can be used in cases where
 * contextual information changes over time within a single context. For example, in the
 * ConsumerCoordinator, it may be useful to know not only the client ID and group ID,
 * are static across the lifetime of the consumer, but also the current generation ID, which
 * changes over time.
 * */
public class DynamicLogContext extends AbstractLogContext {

    private static final Supplier<String> EMPTY_PREFIX = () -> "";

    private final Supplier<String> logPrefix;

    public DynamicLogContext(Supplier<String> logPrefix) {
        this.logPrefix = logPrefix == null ? EMPTY_PREFIX : logPrefix;
    }

    @Override
    protected String addContext(String message) {
        return logPrefix.get() + message;
    }

}
