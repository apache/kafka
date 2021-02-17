package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.header.Headers;
import org.apache.kafka.streams.header.StreamHeaders;

/**
 * Record value plus metadata (read-only) representation.
 *
 * @param <V> value type.
 */
public class RecordValue<V> {

    final String topic;
    final int partition;
    final long offset;
    final V value;
    final long timestamp;
    final Headers headers;

    public RecordValue(
        String topic,
        int partition,
        long offset,
        V value,
        long timestamp,
        org.apache.kafka.common.header.Headers headers
    ) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.value = value;
        this.headers = StreamHeaders.wrap(headers);
        this.timestamp = timestamp;
    }

    public RecordValue(
        String topic,
        int partition,
        long offset,
        V value,
        long timestamp,
        org.apache.kafka.common.header.Header[] headers
    ) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.value = value;
        this.headers = StreamHeaders.wrap(headers);
        this.timestamp = timestamp;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public Headers headers() {
        return headers;
    }

    public V value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }
}
