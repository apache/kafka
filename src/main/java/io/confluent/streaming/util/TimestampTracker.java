package io.confluent.streaming.util;

/**
 * TimestampTracker maintains the timestamp, like the min timestamp, the max timestamp, etc. of stamped elements
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
   * @return
   */
  long get();

  /**
   * Returns the size of internal structure. The meaning of "size" depends on the implementation.
   */
  int size();

}
