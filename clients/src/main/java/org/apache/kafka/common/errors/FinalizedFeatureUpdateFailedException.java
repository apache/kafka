package org.apache.kafka.common.errors;

public class FinalizedFeatureUpdateFailedException extends ApiException {
    private static final long serialVersionUID = 1L;

    public FinalizedFeatureUpdateFailedException(String message) {
        super(message);
    }

    public FinalizedFeatureUpdateFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
