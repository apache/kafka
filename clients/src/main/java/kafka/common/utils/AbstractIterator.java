package kafka.common.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A base class that simplifies implementing an iterator
 * @param <T> The type of thing we are iterating over
 */
public abstract class AbstractIterator<T> implements Iterator<T> {

    private static enum State {
        READY, NOT_READY, DONE, FAILED
    };

    private State state = State.NOT_READY;
    private T next;

    @Override
    public boolean hasNext() {
        switch (state) {
            case FAILED:
                throw new IllegalStateException("Iterator is in failed state");
            case DONE:
                return false;
            case READY:
                return true;
            default:
                return maybeComputeNext();
        }
    }

    @Override
    public T next() {
        if (!hasNext())
            throw new NoSuchElementException();
        state = State.NOT_READY;
        if (next == null)
            throw new IllegalStateException("Expected item but none found.");
        return next;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removal not supported");
    }

    public T peek() {
        if (!hasNext())
            throw new NoSuchElementException();
        return next;
    }

    protected T allDone() {
        state = State.DONE;
        return null;
    }

    protected abstract T makeNext();

    private Boolean maybeComputeNext() {
        state = State.FAILED;
        next = makeNext();
        if (state == State.DONE) {
            return false;
        } else {
            state = State.READY;
            return true;
        }
    }

}
