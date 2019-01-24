package org.apache.kafka.streams.state.internals;

/**
 * Marker interface so we can distinguish stores that persist timestamps
 * All stores that do so MUST implement this interface, including wrappers
 */
public interface StoreWithTimestamps {
}
