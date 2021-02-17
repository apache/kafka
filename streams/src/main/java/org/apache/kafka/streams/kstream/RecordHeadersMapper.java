package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.header.Headers;

/**
 * Function to map Key Value pair into {@link Headers}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface RecordHeadersMapper<K, V> {

    Headers get(final K key, final V value);
}
