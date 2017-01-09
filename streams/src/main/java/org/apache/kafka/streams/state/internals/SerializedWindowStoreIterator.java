package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.NoSuchElementException;

class SerializedWindowStoreIterator<V> implements WindowStoreIterator<V> {
    protected final KeyValueIterator<Bytes, byte[]> underlying;
    private final StateSerdes<?, V> serdes;

    // this is optimizing the case when underlying is already a bytes store iterator, in which we can avoid Bytes.wrap() costs
    private static class WrapperWindowStoreIterator extends SerializedWindowStoreIterator<byte[]> {
        WrapperWindowStoreIterator(final KeyValueIterator<Bytes, byte[]> underlying) {
            super(underlying, null);
        }

        @Override
        public KeyValue<Long, byte[]> next() {
            if (!underlying.hasNext()) {
                throw new NoSuchElementException();
            }

            final KeyValue<Bytes, byte[]> next = underlying.next();
            final long timestamp = WindowStoreUtils.timestampFromBinaryKey(next.key.get());
            final byte[] value = next.value;
            return KeyValue.pair(timestamp, value);
        }
    }

    static SerializedWindowStoreIterator<byte[]> bytesIterator(final KeyValueIterator<Bytes, byte[]> underlying) {
        return new WrapperWindowStoreIterator(underlying);
    }

    SerializedWindowStoreIterator(final KeyValueIterator<Bytes, byte[]> underlying, final StateSerdes<?, V> serdes) {
        this.underlying = underlying;
        this.serdes = serdes;
    }

    @Override
    public boolean hasNext() {
        return underlying.hasNext();
    }

    /**
     * @throws NoSuchElementException if no next element exists
     */
    @Override
    public KeyValue<Long, V> next() {
        if (!underlying.hasNext()) {
            throw new NoSuchElementException();
        }

        final KeyValue<Bytes, byte[]> next = underlying.next();
        final long timestamp = WindowStoreUtils.timestampFromBinaryKey(next.key.get());
        final V value = serdes.valueFrom(next.value);
        return KeyValue.pair(timestamp, value);
    }

    @Override
    public Long peekNextKey() {
        if (!underlying.hasNext()) {
            throw new NoSuchElementException();
        }

        final Bytes nextKey = underlying.peekNextKey();

        return WindowStoreUtils.timestampFromBinaryKey(nextKey.get());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported.");
    }

    @Override
    public void close() {
        underlying.close();
    }
}
