package io.confluent.streaming.util;

/**
 * Created by yasuhiro on 6/23/15.
 */
public class Stamped<V> implements Comparable {

  public final V value;
  public final long timestamp;
  private final long nanoTime = System.nanoTime(); // used for tie-breaking

  public Stamped(V value, long timestamp) {
    this.value = value;
    this.timestamp = timestamp;
  }

  @Override
  public int compareTo(Object other) {
    long otherTimestamp = ((Stamped<Object>) other).timestamp;

    if (timestamp < otherTimestamp) return -1;
    else if (timestamp > otherTimestamp) return 1;

    // tie breaking
    otherTimestamp = ((Stamped<Object>) other).nanoTime;

    if (nanoTime < otherTimestamp) return -1;
    else if (nanoTime > otherTimestamp) return 1;

    return 0;
  }

}
