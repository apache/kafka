package io.confluent.streaming.internal;

import io.confluent.streaming.ValueJoiner;
import io.confluent.streaming.Window;

import java.util.Iterator;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamNestedLoop<K, V, V1, V2> extends KStreamImpl<K, V, K, V1> {

  private final Window<K, V2> window;
  private final ValueJoiner<V, V1, V2> joiner;

  KStreamNestedLoop(Window<K, V2> window, ValueJoiner<V, V1, V2> joiner, PartitioningInfo partitioningInfo, KStreamContextImpl context) {
    super(partitioningInfo, context);
    this.window = window;
    this.joiner = joiner;
  }

  public void receive(K key, V1 value, long timestamp) {
    Iterator<V2> iter = window.find(key, timestamp);
    if (iter != null) {
      while (iter.hasNext()) {
        forward(key, joiner.apply(value, iter.next()), timestamp);
      }
    }
  }

}
