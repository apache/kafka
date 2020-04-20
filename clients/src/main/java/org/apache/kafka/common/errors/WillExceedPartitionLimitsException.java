package org.apache.kafka.common.errors;

public class WillExceedPartitionLimitsException extends ApiException {

    private static final long serialVersionUID = 1L;

    public WillExceedPartitionLimitsException(String message, Throwable cause) {
        super(message, cause);
    }

    public WillExceedPartitionLimitsException(String message) {
        super(message);
    }
}
