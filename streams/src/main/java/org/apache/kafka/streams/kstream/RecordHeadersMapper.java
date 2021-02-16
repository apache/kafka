package org.apache.kafka.streams.kstream;


import org.apache.kafka.common.header.Headers;

public interface RecordHeadersMapper<K, V> {

  Headers get(final K key, final V value);
}
