package org.apache.kafka.streams.errors;

public class ShutdownRequestedException extends StreamsException {
    public ShutdownRequestedException(final String message) {
        super(message);
    }

    public ShutdownRequestedException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public ShutdownRequestedException(final Throwable throwable) {
        super(throwable);
    }
}
