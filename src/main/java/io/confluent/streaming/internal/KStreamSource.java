package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamSource<K, V> extends KStreamImpl<K, V> {

  KStreamSource(PartitioningInfo partitioningInfo, KStreamContext context) {
    super(partitioningInfo, context);
  }

  @Override
  public void receive(Object key, Object value, long timestamp, long streamTime) {
    synchronized(this) {
      forward(key, value, timestamp, streamTime);
    }
  }

}
