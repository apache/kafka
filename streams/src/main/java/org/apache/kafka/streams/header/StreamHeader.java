package org.apache.kafka.streams.header;

public class StreamHeader implements Header {

    final String key;
    final byte[] value;

    public StreamHeader(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public static StreamHeader wrap(org.apache.kafka.common.header.Header header) {
        return new StreamHeader(header.key(), header.value());
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public byte[] value() {
        return value;
    }
}
