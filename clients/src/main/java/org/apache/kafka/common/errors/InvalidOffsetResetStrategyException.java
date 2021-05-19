package org.apache.kafka.common.errors;

public class InvalidOffsetResetStrategyException extends ApiException {
    private static final long serialVersionUID = 1L;

    public InvalidOffsetResetStrategyException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidOffsetResetStrategyException(String message) {
        super(message);
    }
}