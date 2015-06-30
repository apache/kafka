package io.confluent.streaming.util;

import java.util.Iterator;

/**
 * Created by yasuhiro on 6/29/15.
 */
public abstract class FilteredIterator<T, S> implements Iterator<T> {

  private Iterator<S> inner;
  private T nextValue = null;

  public FilteredIterator(Iterator<S> inner) {
    this.inner = inner;

    findNext();
  }

  @Override
  public boolean hasNext() {
    return nextValue != null;
  }

  @Override
  public T next() {
    T value = nextValue;
    findNext();

    return value;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private void findNext() {
    while (inner.hasNext()) {
      S item = inner.next();
      nextValue = filter(item);
      if (nextValue != null) {
        return;
      }
    }
    nextValue = null;
  }

  protected abstract T filter(S item);
}
