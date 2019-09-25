package org.apache.kafka.common.requests;

import org.apache.kafka.common.utils.AbstractIterator;

import java.util.Iterator;
import java.util.List;

abstract class NestedIterator<O, I> extends AbstractIterator<I> {
    private final Iterator<O> outerIterator;
    private Iterator<I> innerIterator;

    NestedIterator(List<O> topicStates) {
        outerIterator = topicStates.iterator();
    }

    public abstract Iterable<I> innerIterable(O outer);

    @Override
    public I makeNext() {
        while (innerIterator == null || !innerIterator.hasNext()) {
            if (outerIterator.hasNext())
                innerIterator = innerIterable(outerIterator.next()).iterator();
            else
                return allDone();
        }
        return innerIterator.next();
    }
}
