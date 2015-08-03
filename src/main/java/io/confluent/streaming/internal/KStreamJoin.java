package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamTopology;
import io.confluent.streaming.NotCopartitionedException;
import io.confluent.streaming.ValueJoiner;
import io.confluent.streaming.Window;

import java.util.Iterator;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamJoin<K, V, V1, V2> extends KStreamImpl<K, V> {

  private static abstract class Finder<K, T> {
    abstract Iterator<T> find(K key, long timestamp);
  }

  private final Finder<K, V1> finder1;
  private final Finder<K, V2> finder2;
  private final ValueJoiner<V, V1, V2> joiner;
  final Receiver receiverForOtherStream;
  private KStreamMetadata thisMetadata;
  private KStreamMetadata otherMetadata;

  KStreamJoin(KStreamWindowedImpl<K, V1> stream1, KStreamWindowedImpl<K, V2> stream2, boolean prior, ValueJoiner<V, V1, V2> joiner, KStreamTopology initializer) {
    super(initializer);

    final Window<K, V1> window1 = stream1.window;
    final Window<K, V2> window2 = stream2.window;

    if (prior) {
      this.finder1 = new Finder<K, V1>() {
        Iterator<V1> find(K key, long timestamp) {
          return window1.findAfter(key, timestamp);
        }
      };
      this.finder2 = new Finder<K, V2>() {
        Iterator<V2> find(K key, long timestamp) {
          return window2.findBefore(key, timestamp);
        }
      };
    }
    else {
      this.finder1 = new Finder<K, V1>() {
        Iterator<V1> find(K key, long timestamp) {
          return window1.find(key, timestamp);
        }
      };
      this.finder2 = new Finder<K, V2>() {
        Iterator<V2> find(K key, long timestamp) {
          return window2.find(key, timestamp);
        }
      };
    }

    this.joiner = joiner;

    this.receiverForOtherStream = getReceiverForOther();
  }

  @Override
  public void bind(KStreamContext context, KStreamMetadata metadata) {
    super.bind(context, metadata);

    thisMetadata = metadata;
    if (otherMetadata != null && !thisMetadata.isJoinCompatibleWith(otherMetadata)) throw new NotCopartitionedException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp) {
    Iterator<V2> iter = finder2.find((K)key, timestamp);
    if (iter != null) {
      while (iter.hasNext()) {
        doJoin((K)key, (V1)value, iter.next(), timestamp);
      }
    }
  }

  private Receiver getReceiverForOther() {
    return new Receiver() {
      @Override
      public void bind(KStreamContext context, KStreamMetadata metadata) {
        otherMetadata = metadata;
        if (thisMetadata != null && !thisMetadata.isJoinCompatibleWith(otherMetadata)) throw new NotCopartitionedException();
      }
      @SuppressWarnings("unchecked")
      @Override
      public void receive(Object key, Object value2, long timestamp) {
        Iterator<V1> iter = finder1.find((K)key, timestamp);
        if (iter != null) {
          while (iter.hasNext()) {
            doJoin((K)key, iter.next(), (V2)value2, timestamp);
          }
        }
      }
      @Override
      public void close() {
        // down stream instances are close when the primary stream is closed
      }
    };
  }

  // TODO: use the "outer-stream" topic as the resulted join stream topic
  private void doJoin(K key, V1 value1, V2 value2, long timestamp) {
    forward(key, joiner.apply(value1, value2), timestamp);
  }

}
