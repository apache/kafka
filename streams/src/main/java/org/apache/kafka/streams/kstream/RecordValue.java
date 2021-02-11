package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.Headers;

public class RecordValue<V> {

  final V value;
  final Headers headers;
  final long timestamp;

  public RecordValue(
      V value,
      org.apache.kafka.common.header.Headers headers,
      long timestamp
  ) {
    this.value = value;
    this.headers = null; //TODO headers.toArray();
    this.timestamp = timestamp;
  }

  V value() {
    return value;
  }

  public Headers headers() {
    return headers;
  }
}
