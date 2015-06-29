package io.confluent.streaming.util;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;

/**
 * Created by yasuhiro on 6/24/15.
 */
public class QueueWithMinTimestampTracking<E> {

  private final Deque<Stamped<E>> queue = new ArrayDeque<Stamped<E>>();
  private final LinkedList<Stamped<E>> descendingSubsequence = new LinkedList<Stamped<E>>();

  public void add(E value, long timestamp) {

    Stamped<E> elem = new Stamped<E>(value, timestamp);
    queue.addLast(elem);

    Stamped<E> minElem = descendingSubsequence.peekLast();
    while (minElem.compareTo(elem) >= 0) {
      descendingSubsequence.removeLast();
      minElem = descendingSubsequence.peekLast();
    }
    descendingSubsequence.offerLast(elem);
  }

  public E next() {
    Stamped<E> stamped = queue.getFirst();

    if (stamped == null) return null;

    if (descendingSubsequence.peekFirst() == stamped)
      descendingSubsequence.removeFirst();

    return stamped.value;
  }

  public E peekNext() {
    if (queue.size() > 0)
      return queue.peekFirst().value;
    else
      return null;
  }

  public E peekLast() {
    if (queue.size() > 0)
      return queue.peekLast().value;
    else
      return null;
  }

  public int size() {
    return queue.size();
  }

  public long timestamp() {
    Stamped<E> stamped = descendingSubsequence.peekFirst();
    if (stamped == null) return -1L;

    return stamped.timestamp;
  }
}
