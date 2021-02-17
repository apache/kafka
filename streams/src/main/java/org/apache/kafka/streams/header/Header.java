package org.apache.kafka.streams.header;

public interface Header {
    /**
     * The header's key, which is not necessarily unique within the set of headers on a Kafka message.
     *
     * @return the header's key; never null
     */
    String key();

    byte[] value();

    String valueAsUtf8();
}
