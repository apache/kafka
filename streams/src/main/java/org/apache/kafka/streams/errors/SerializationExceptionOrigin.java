package org.apache.kafka.streams.errors;

/**
 * Indicates whether a {@link org.apache.kafka.common.errors.SerializationException}  comes from the key or the value
 */
public enum SerializationExceptionOrigin {
    KEY,
    VALUE
}
