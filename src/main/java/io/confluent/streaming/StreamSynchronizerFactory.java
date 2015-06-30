package io.confluent.streaming;

import io.confluent.streaming.internal.ChooserImpl;
import io.confluent.streaming.internal.IngestorImpl;

/**
 * Created by yasuhiro on 6/24/15.
 */
public class StreamSynchronizerFactory<K, V> {

  private TimestampExtractor<K, V> timestampExtractor;

  public StreamSynchronizerFactory(TimestampExtractor<K, V> timestampExtractor) {
    this.timestampExtractor = timestampExtractor;
  }

  public StreamSynchronizer<K, V> create(String name, IngestorImpl<K, V> consumer, int desiredNumberOfUnprocessedRecords) {
    return new StreamSynchronizer<K, V>(name, consumer, new ChooserImpl<K, V>(), timestampExtractor, desiredNumberOfUnprocessedRecords);
  }

}
