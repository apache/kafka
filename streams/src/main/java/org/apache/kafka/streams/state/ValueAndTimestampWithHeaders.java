package org.apache.kafka.streams.state;

import org.apache.kafka.common.header.Headers;

import java.util.Objects;

public final class ValueAndTimestampWithHeaders<V> {
    private final V value;
    private final long timestamp;
    private final Headers headers;
    
    private ValueAndTimestampWithHeaders(final V value,
                                         final long timestamp,
                                         final Headers headers) {
        this.value = value;
        this.timestamp = timestamp;
        this.headers = headers;
    }

    /**
     * Create a new {@link ValueAndTimestampWithHeaders} instance if the provided {@code value} is not {@code null}.
     *
     * @param value      the value
     * @param timestamp  the timestamp
     * @param headers    the headers
     * @param <V> the type of the value
     * @return a new {@link ValueAndTimestampWithHeaders} instance if the provided {@code value} is not {@code null};
     *         otherwise {@code null} is returned
     */
    public static <V> ValueAndTimestampWithHeaders<V> make(final V value, final long timestamp, final Headers headers) {
        return value == null ? null : new ValueAndTimestampWithHeaders<>(value, timestamp, headers);
    }

    /**
     * Create a new {@link ValueAndTimestampWithHeaders} instance. The provided {@code value} may be {@code null}.
     *
     * @param value      the value
     * @param timestamp  the timestamp
     * @param headers    the headers
     * @param <V> the type of the value
     * @return a new {@link ValueAndTimestampWithHeaders} instance
     */
    public static <V> ValueAndTimestampWithHeaders<V> makeAllowNullable(
        final V value, final long timestamp, final Headers headers) {
        return new ValueAndTimestampWithHeaders<>(value, timestamp, headers);
    }

    /**
     * Return the wrapped {@code value} of the given {@code ValueTimestampHeaders} parameter
     * if the parameter is not {@code null}.
     *
     * @param ValueAndTimestampWithHeaders a {@link ValueAndTimestampWithHeaders} instance; can be {@code null}
     * @param <V> the type of the value
     * @return the wrapped {@code value} of {@code ValueTimestampHeaders} if not {@code null}; otherwise {@code null}
     */
    public static <V> V getValueOrNull(final ValueAndTimestampWithHeaders<V> ValueAndTimestampWithHeaders) {
        return ValueAndTimestampWithHeaders == null ? null : ValueAndTimestampWithHeaders.value();
    }

    public V value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    public Headers headers() {
        return headers;
    }

    @Override
    public String toString() {
        return "<" + value + "," + timestamp + "," + headers + ">";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ValueAndTimestampWithHeaders<?> that = (ValueAndTimestampWithHeaders<?>) o;
        return timestamp == that.timestamp &&
            Objects.equals(value, that.value) &&
            Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, timestamp, headers);
    }
}
