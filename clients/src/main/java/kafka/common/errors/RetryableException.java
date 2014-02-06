package kafka.common.errors;

/**
 * A retryable exception is an exception that is safe to retry. To be retryable an exception should be
 * <ol>
 * <li>Transient, there is no point retrying a error due to a non-existant topic or message too large
 * <li>Idempotent, the exception is known to not change any state on the server
 * </ol>
 * A client may choose to retry any request they like, but exceptions extending this class are always safe and sane to
 * retry.
 */
public abstract class RetryableException extends ApiException {

    private static final long serialVersionUID = 1L;

    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(Throwable cause) {
        super(cause);
    }

    public RetryableException() {
    }

}
