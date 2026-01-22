package org.apache.kafka.streams.state;

/**
 * A key-(value/timestamp/headers) store that supports put/get/delete.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public interface TimestampedKeyValueStoreWithHeaders<K, V>
    extends KeyValueStore<K, ValueTimestampHeaders<V>> {
}