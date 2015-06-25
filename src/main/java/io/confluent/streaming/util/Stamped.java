package io.confluent.streaming.util;

/**
 * Created by yasuhiro on 6/23/15.
 */
public class Stamped<V> implements Comparable {

  public final V value;
  public final long timestamp;
  private final long tieBreaker;

  public Stamped(V value, long timestamp) {
    this(value, timestamp, System.nanoTime());
  }

  public Stamped(V value, long timestamp, long tieBreaker) {
    this.value = value;
    this.timestamp = timestamp;
    this.tieBreaker = tieBreaker;
  }

  public int compareTo(Object other) {
    long otherTimestamp = ((Stamped<?>) other).timestamp;

    if (timestamp < otherTimestamp) return -1;
    else if (timestamp > otherTimestamp) return 1;

    // tie breaking
    otherTimestamp = ((Stamped<?>) other).tieBreaker;

    if (tieBreaker < otherTimestamp) return -1;
    else if (tieBreaker > otherTimestamp) return 1;

    return 0;
  }

}
