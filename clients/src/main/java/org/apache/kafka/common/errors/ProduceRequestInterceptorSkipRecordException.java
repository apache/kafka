package org.apache.kafka.common.errors;

public class ProduceRequestInterceptorSkipRecordException extends Exception {
    public ProduceRequestInterceptorSkipRecordException() {
        super();
    }

    public ProduceRequestInterceptorSkipRecordException(String message) {
        super(message);
    }

    public ProduceRequestInterceptorSkipRecordException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProduceRequestInterceptorSkipRecordException(Throwable cause) {
        super(cause);
    }
}
