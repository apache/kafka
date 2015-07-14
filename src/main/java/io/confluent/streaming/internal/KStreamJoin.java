package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
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

  KStreamJoin(final Window<K, V1> window1, final Window<K, V2> window2, boolean prior, ValueJoiner<V, V1, V2> joiner, KStreamMetadata streamMetadata, KStreamContext context) {
    super(streamMetadata, context);

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
  public void receive(String topic, Object key, Object value, long timestamp, long streamTime) {
    Iterator<V2> iter = finder2.find((K)key, timestamp);
    if (iter != null) {
      while (iter.hasNext()) {
        doJoin((K)key, (V1)value, iter.next(), timestamp, streamTime);
      }
    }
  }

  private Receiver getReceiverForOther() {
    return new Receiver() {

      @Override
      public void receive(String topic, Object key, Object value2, long timestamp, long streamTime) {
        Iterator<V1> iter = finder1.find((K)key, timestamp);
        if (iter != null) {
          while (iter.hasNext()) {
            doJoin((K)key, iter.next(), (V2)value2, timestamp, streamTime);
          }
        }
      }
    };
  }

  // TODO: use the "outer-stream" topic as the resulted join stream topic
  private void doJoin(K key, V1 value1, V2 value2, long timestamp, long streamTime) {
    forward(KStreamMetadata.UNKNOWN_TOPICNAME, key, joiner.apply(value1, value2), timestamp, streamTime);
  }

}
