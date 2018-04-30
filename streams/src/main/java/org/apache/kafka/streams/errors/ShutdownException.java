package org.apache.kafka.streams.errors;

public class ShutdownException extends StreamsException {
    public ShutdownException(final String message) {
        super(message);
    }

    public ShutdownException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public ShutdownException(final Throwable throwable) {
        super(throwable);
    }
}
