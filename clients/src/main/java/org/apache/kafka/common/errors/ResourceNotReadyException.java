package org.apache.kafka.common.errors;

public class ResourceNotReadyException extends RetriableException {

    public static final long serialVersionUID = 1L;

    public ResourceNotReadyException() {
        super();
    }

    public ResourceNotReadyException(String message) {
        super(message);
    }
}
