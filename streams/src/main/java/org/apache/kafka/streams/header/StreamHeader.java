package org.apache.kafka.streams.header;

import java.util.Arrays;
import org.apache.kafka.common.utils.Utils;

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

    @Override
    public String valueAsUtf8() {
        return Utils.utf8(value);
    }

    @Override
    public String toString() {
        return "StreamHeader(" +
            "key='" + key + '\'' +
            ",value=" + Arrays.toString(value) +
            ')';
    }
}
