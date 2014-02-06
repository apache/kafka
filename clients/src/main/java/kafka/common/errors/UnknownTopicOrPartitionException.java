package kafka.common.errors;

public class UnknownTopicOrPartitionException extends ApiException {

    private static final long serialVersionUID = 1L;

    public UnknownTopicOrPartitionException() {
    }

    public UnknownTopicOrPartitionException(String message) {
        super(message);
    }

    public UnknownTopicOrPartitionException(Throwable throwable) {
        super(throwable);
    }

    public UnknownTopicOrPartitionException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
