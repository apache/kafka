package org.apache.kafka.common.errors;

public class DesignatedLeaderNotAvailableException extends InvalidMetadataException {
    public DesignatedLeaderNotAvailableException(String message) {
        super(message);
    }

    public DesignatedLeaderNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
