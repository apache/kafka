package org.apache.kafka.common.errors;

public class MemberIdRequiredException extends ApiException {

    private static final long serialVersionUID = 1L;

    public MemberIdRequiredException(String message) {
        super(message);
    }

    public MemberIdRequiredException(String message, Throwable cause) {
        super(message, cause);
    }
}
