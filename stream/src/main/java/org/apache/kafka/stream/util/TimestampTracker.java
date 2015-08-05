package org.apache.kafka.stream.util;

/**
 * TimestampTracker is a helper class for a sliding window implementation.
 * It is assumed that elements are added or removed in a FIFO manner.
 * It maintains the timestamp, like the min timestamp, the max timestamp, etc. of stamped elements
 * that were added but not yet removed.
 */
public interface TimestampTracker<E> {

  /**
   * Adds a stamped elements to this tracker.
   * @param elem the added element
   */
  void addStampedElement(Stamped<E> elem);

  /**
   * Removed a stamped elements to this tracker.
   * @param elem the removed element
   */
  void removeStampedElement(Stamped<E> elem);

  /**
   * Returns the timestamp
   * @return timestamp, or -1L when empty
   */
  long get();

  /**
   * Returns the size of internal structure. The meaning of "size" depends on the implementation.
   * @return size
   */
  int size();

}
