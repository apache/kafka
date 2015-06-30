package io.confluent.streaming.util;

import java.util.LinkedList;

/**
 * MinTimestampTracker maintains the minimum timestamp of stamped elements that were added but not yet removed.
 */
public class MinTimestampTracker<E> implements TimestampTracker<E> {

  private final LinkedList<Stamped<E>> descendingSubsequence = new LinkedList<Stamped<E>>();

  public void addStampedElement(Stamped<E> elem) {
    Stamped<E> minElem = descendingSubsequence.peekLast();
    while (minElem.compareTo(elem) >= 0) {
      descendingSubsequence.removeLast();
      minElem = descendingSubsequence.peekLast();
    }
    descendingSubsequence.offerLast(elem);
  }

  public void removeStampedElement(Stamped<E> elem) {
    if (elem != null && descendingSubsequence.peekFirst() == elem)
      descendingSubsequence.removeFirst();
  }

  public int size() {
    return descendingSubsequence.size();
  }

  public long get() {
    Stamped<E> stamped = descendingSubsequence.peekFirst();
    if (stamped == null) return -1L;

    return stamped.timestamp;
  }

}
