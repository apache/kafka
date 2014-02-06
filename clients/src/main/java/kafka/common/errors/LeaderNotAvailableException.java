package kafka.common.errors;

public class LeaderNotAvailableException extends RetryableException {

    private static final long serialVersionUID = 1L;

    public LeaderNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public LeaderNotAvailableException(String message) {
        super(message);
    }

    public LeaderNotAvailableException(Throwable cause) {
        super(cause);
    }

}
