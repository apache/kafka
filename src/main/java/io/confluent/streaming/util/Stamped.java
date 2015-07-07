package io.confluent.streaming.util;

/**
 * Created by yasuhiro on 6/23/15.
 */
public class Stamped<V> implements Comparable {

  public final V value;
  public final long timestamp;

  public Stamped(V value, long timestamp) {
    this.value = value;
    this.timestamp = timestamp;
  }

  public int compareTo(Object other) {
    long otherTimestamp = ((Stamped<?>) other).timestamp;

    if (timestamp < otherTimestamp) return -1;
    else if (timestamp > otherTimestamp) return 1;
    return 0;
  }

}
