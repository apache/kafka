package kafka.common.errors;

public class NotLeaderForPartitionException extends RetryableException {

    private static final long serialVersionUID = 1L;

    public NotLeaderForPartitionException() {
        super();
    }

    public NotLeaderForPartitionException(String message) {
        super(message);
    }

    public NotLeaderForPartitionException(Throwable cause) {
        super(cause);
    }

    public NotLeaderForPartitionException(String message, Throwable cause) {
        super(message, cause);
    }

}
