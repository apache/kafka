package io.confluent.streaming;

import io.confluent.streaming.internal.ChooserImpl;
import io.confluent.streaming.internal.RegulatedConsumer;

/**
 * Created by yasuhiro on 6/24/15.
 */
public class StreamSynchronizerFactory<K, V> {

  private TimestampExtractor<K, V> timestampExtractor;
  private int desiredNumberOfUnprocessedRecords;

  public StreamSynchronizerFactory(TimestampExtractor<K, V> timestampExtractor, int desiredNumberOfUnprocessedRecords) {
    this.timestampExtractor = timestampExtractor;
    this.desiredNumberOfUnprocessedRecords = desiredNumberOfUnprocessedRecords;
  }

  public StreamSynchronizer<K, V> create(String name, RegulatedConsumer<K, V> consumer) {
    return new StreamSynchronizer<K, V>(consumer, new ChooserImpl<K, V>(), timestampExtractor, desiredNumberOfUnprocessedRecords);
  }

}
