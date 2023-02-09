package org.apache.kafka.common.errors;

/**
 * Fatal exception to indicate that the produce request interceptors failed
 */
public class ProduceRequestInterceptorUnhandledException extends ApiException {

    public ProduceRequestInterceptorUnhandledException(String msg) {
        super(msg);
    }

    public ProduceRequestInterceptorUnhandledException(Throwable throwable) {
        super(throwable);
    }

}
