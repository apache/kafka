package org.apache.kafka.common.errors;

/**
 * Indicates that the leader is not able to guarantee monotonically increasing offsets
 * due to a recent leader election and high-water mark lag
 */
public class OffsetNotAvailableException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public OffsetNotAvailableException(String message) {
        super(message);
    }

    public OffsetNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
