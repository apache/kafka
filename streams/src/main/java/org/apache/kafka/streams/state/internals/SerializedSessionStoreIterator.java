package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;

class SerializedSessionStoreIterator<K, AGG> implements KeyValueIterator<Windowed<K>, AGG> {

    protected final KeyValueIterator<Bytes, byte[]> bytesIterator;
    private final StateSerdes<K, AGG> serdes;

    // this is optimizing the case when underlying is already a bytes store iterator, in which we can avoid Bytes.wrap() costs
    private static class WrapperSessionStoreIterator extends SerializedSessionStoreIterator<Bytes, byte[]> {
        WrapperSessionStoreIterator(final KeyValueIterator<Bytes, byte[]> underlying) {
            super(underlying, null);
        }

        @Override
        public Windowed<Bytes> peekNextKey() {
            final Bytes key = bytesIterator.peekNextKey();

            return SessionKeySerde.fromBytes(key);
        }

        @Override
        public KeyValue<Windowed<Bytes>, byte[]> next() {
            final KeyValue<Bytes, byte[]> next = bytesIterator.next();
            return KeyValue.pair(SessionKeySerde.fromBytes(next.key), next.value);
        }
    }

    static SerializedSessionStoreIterator<Bytes, byte[]> bytesIterator(final KeyValueIterator<Bytes, byte[]> underlying) {
        return new WrapperSessionStoreIterator(underlying);
    }

    SerializedSessionStoreIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator, final StateSerdes<K, AGG> serdes) {
        this.bytesIterator = bytesIterator;
        this.serdes = serdes;
    }

    @Override
    public void close() {
        bytesIterator.close();
    }

    @Override
    public Windowed<K> peekNextKey() {
        final Bytes bytes = bytesIterator.peekNextKey();
        return SessionKeySerde.from(bytes.get(), serdes.keyDeserializer());
    }

    @Override
    public boolean hasNext() {
        return bytesIterator.hasNext();
    }

    @Override
    public KeyValue<Windowed<K>, AGG> next() {
        final KeyValue<Bytes, byte[]> next = bytesIterator.next();
        return KeyValue.pair(SessionKeySerde.from(next.key.get(), serdes.keyDeserializer()), serdes.valueFrom(next.value));
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported by SerializedSessionStoreIterator");
    }
}
