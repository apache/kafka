package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.NoSuchElementException;

class WrappedWindowStoreIterator<V> implements WindowStoreIterator<V> {
    final KeyValueIterator<Bytes, byte[]> bytesIterator;
    private final StateSerdes<?, V> serdes;

    // this is optimizing the case when underlying is already a bytes store iterator, in which we can avoid Bytes.wrap() costs
    private static class WrappedWindowStoreBytesIterator extends WrappedWindowStoreIterator<byte[]> {
        WrappedWindowStoreBytesIterator(final KeyValueIterator<Bytes, byte[]> underlying,
                                        final StateSerdes<Bytes, byte[]> serdes) {
            super(underlying, serdes);
        }

        @Override
        public KeyValue<Long, byte[]> next() {
            if (!bytesIterator.hasNext()) {
                throw new NoSuchElementException();
            }

            final KeyValue<Bytes, byte[]> next = bytesIterator.next();
            final long timestamp = WindowStoreUtils.timestampFromBinaryKey(next.key.get());
            final byte[] value = next.value;
            return KeyValue.pair(timestamp, value);
        }
    }

    static WrappedWindowStoreIterator<byte[]> bytesIterator(final KeyValueIterator<Bytes, byte[]> underlying,
                                                            final StateSerdes<Bytes, byte[]> serdes) {
        return new WrappedWindowStoreBytesIterator(underlying, serdes);
    }

    WrappedWindowStoreIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator, final StateSerdes<?, V> serdes) {
        this.bytesIterator = bytesIterator;
        this.serdes = serdes;
    }

    @Override
    public boolean hasNext() {
        return bytesIterator.hasNext();
    }

    /**
     * @throws NoSuchElementException if no next element exists
     */
    @Override
    public KeyValue<Long, V> next() {
        if (!bytesIterator.hasNext()) {
            throw new NoSuchElementException();
        }
        final KeyValue<Bytes, byte[]> next = bytesIterator.next();
        final long timestamp = WindowStoreUtils.timestampFromBinaryKey(next.key.get());
        final V value = serdes.valueFrom(next.value);
        return KeyValue.pair(timestamp, value);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported in WrappedWindowStoreIterator.");
    }

    @Override
    public void close() {
        bytesIterator.close();
    }

    @Override
    public Long peekNextKey() {
        if (!bytesIterator.hasNext()) {
            throw new NoSuchElementException();
        }
        return WindowStoreUtils.timestampFromBinaryKey(bytesIterator.peekNextKey().get());
    }
}
