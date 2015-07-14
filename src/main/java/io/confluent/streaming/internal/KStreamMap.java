package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KeyValueMapper;
import io.confluent.streaming.KeyValue;
import io.confluent.streaming.SyncGroup;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamMap<K, V, K1, V1> extends KStreamImpl<K, V> {

  private final KeyValueMapper<K, V, K1, V1> mapper;

  KStreamMap(KeyValueMapper<K, V, K1, V1> mapper, SyncGroup syncGroup, KStreamContext context) {
    super(KStreamMetadata.unjoinable(syncGroup), context);
    this.mapper = mapper;
  }

  @Override
  public void receive(String topic, Object key, Object value, long timestamp, long streamTime) {
    synchronized (this) {
      KeyValue<K, V> newPair = mapper.apply((K1)key, (V1)value);
      forward(KStreamMetadata.UNKNOWN_TOPICNAME, newPair.key, newPair.value, timestamp, streamTime);
    }
  }

}
