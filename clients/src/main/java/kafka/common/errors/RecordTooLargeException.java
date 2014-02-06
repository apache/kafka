package kafka.common.errors;

public class RecordTooLargeException extends ApiException {

    private static final long serialVersionUID = 1L;

    public RecordTooLargeException() {
        super();
    }

    public RecordTooLargeException(String message, Throwable cause) {
        super(message, cause);
    }

    public RecordTooLargeException(String message) {
        super(message);
    }

    public RecordTooLargeException(Throwable cause) {
        super(cause);
    }

}
