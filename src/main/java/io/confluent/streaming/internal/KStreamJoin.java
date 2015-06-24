package io.confluent.streaming.internal;

import io.confluent.streaming.ValueJoiner;
import io.confluent.streaming.Window;

import java.util.Iterator;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamJoin<K, V, V1, V2> extends KStreamImpl<K, V, K, V1> {

  private final Window<K, V1> window1;
  private final Window<K, V2> window2;
  private final ValueJoiner<V, V1, V2> joiner;
  final Receiver<K, V2> receiverForOtherStream;

  private boolean flushed = true;

  KStreamJoin(final Window<K, V1> window1, Window<K, V2> window2, ValueJoiner<V, V1, V2> joiner, PartitioningInfo partitioningInfo, KStreamContextImpl context) {
    super(partitioningInfo, context);

    this.window1 = window1;
    this.window2 = window2;
    this.joiner = joiner;

    this.receiverForOtherStream = getReceiverForOther();
  }

  public void receive(K key, V1 value, long timestamp) {
    flushed = false;
    Iterator<V2> iter = window2.find(key, timestamp);
    if (iter != null) {
      while (iter.hasNext()) {
        doJoin(key, value, iter.next(), timestamp);
      }
    }
  }

  public void flush() {
    if (!flushed) {
      super.flush();
      flushed = true;
    }
  }

  private Receiver<K, V2> getReceiverForOther() {
    return new Receiver<K, V2>() {

      public void receive(K key, V2 value2, long timestamp) {
        flushed = false;
        Iterator<V1> iter = window1.find(key, timestamp);
        if (iter != null) {
          while (iter.hasNext()) {
            doJoin(key, iter.next(), value2, timestamp);
          }
        }
      }

      public void punctuate(long timestamp) {
        KStreamJoin.this.punctuate(timestamp);
      }

      public void flush() {
        KStreamJoin.this.flush();
      }
    };
  }

  private void doJoin(K key, V1 value1, V2 value2, long timestamp) {
    forward(key, joiner.apply(value1, value2), timestamp);
  }

}
