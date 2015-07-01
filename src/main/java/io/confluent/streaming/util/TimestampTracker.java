package io.confluent.streaming.util;

/**
 * TimestampTracker is a helper class for a sliding window implementation.
 * It is assumed that elements are added or removed in a FIFO manner.
 * It maintains the timestamp, like the min timestamp, the max timestamp, etc. of stamped elements
 * that were added but not yet removed.
 */
public interface TimestampTracker<E> {

  /**
   * Adds a stamped elements to this tracker.
   * @param elem
   */
  void addStampedElement(Stamped<E> elem);

  /**
   * Removed a stamped elements to this tracker.
   * @param elem
   */
  void removeStampedElement(Stamped<E> elem);

  /**
   * Returns the timestamp
   * @return timestamp, or -1L when empty
   */
  long get();

  /**
   * Returns the size of internal structure. The meaning of "size" depends on the implementation.
   */
  int size();

}
