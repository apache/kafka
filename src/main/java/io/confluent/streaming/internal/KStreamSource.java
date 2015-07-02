package io.confluent.streaming.internal;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamSource<K, V> extends KStreamImpl<K, V, K, V> {

  KStreamSource(PartitioningInfo partitioningInfo, KStreamContextImpl context) {
    super(partitioningInfo, context);
  }

  @Override
  public void receive(K key, V value, long timestamp, long streamTime) {
    synchronized(this) {
      forward(key, value, timestamp, streamTime);
    }
  }

}
