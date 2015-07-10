package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamSource<K, V> extends KStreamImpl<K, V, K, V> {

  private KStreamContext context;

  KStreamSource(PartitioningInfo partitioningInfo, KStreamContextImpl context) {
    super(partitioningInfo, context);

    this.context = context;
  }

  public KStreamContext context() {
    return this.context;
  }

  @Override
  public void receive(Object key, Object value, long timestamp, long streamTime) {
    synchronized(this) {
      forward(key, value, timestamp, streamTime);
    }
  }

}
