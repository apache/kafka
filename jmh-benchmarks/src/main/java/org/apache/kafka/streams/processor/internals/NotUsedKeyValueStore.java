package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

public class NotUsedKeyValueStore implements KeyValueStore<Bytes, byte[]> {

    @Override
    public void put(final Bytes key, final byte[] value) {
        throw new RuntimeException();
    }

    @Override
    public byte[] putIfAbsent(final Bytes key, final byte[] value) {
        throw new RuntimeException();
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        throw new RuntimeException();
    }

    @Override
    public byte[] delete(final Bytes key) {
        throw new RuntimeException();
    }

    @Override
    public String name() {
        return "name";
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {

    }

    @Override
    public void flush() {
        throw new RuntimeException();
    }

    @Override
    public void close() {
        throw new RuntimeException();
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public byte[] get(final Bytes key) {
        throw new RuntimeException();
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        throw new RuntimeException();
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        throw new RuntimeException();
    }

    @Override
    public long approximateNumEntries() {
        throw new RuntimeException();
    }
}