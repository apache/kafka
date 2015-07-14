package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KeyValueMapper;
import io.confluent.streaming.KeyValue;
import io.confluent.streaming.SyncGroup;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamFlatMap<K, V, K1, V1> extends KStreamImpl<K, V> {

  private final KeyValueMapper<K, ? extends Iterable<V>, K1, V1> mapper;

  KStreamFlatMap(KeyValueMapper<K, ? extends Iterable<V>, K1, V1> mapper, SyncGroup syncGroup, KStreamContext context) {
    super(KStreamMetadata.unjoinable(syncGroup), context);
    this.mapper = mapper;
  }

  @Override
  public void receive(String topic, Object key, Object value, long timestamp, long streamTime) {
    synchronized(this) {
      KeyValue<K, ? extends Iterable<V>> newPair = mapper.apply((K1)key, (V1)value);
      for (V v : newPair.value) {
        forward(KStreamMetadata.UNKNOWN_TOPICNAME, newPair.key, v, timestamp, streamTime);
      }
    }
  }

}
