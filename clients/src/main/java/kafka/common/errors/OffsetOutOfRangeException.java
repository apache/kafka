package kafka.common.errors;

public class OffsetOutOfRangeException extends ApiException {

    private static final long serialVersionUID = 1L;

    public OffsetOutOfRangeException() {
    }

    public OffsetOutOfRangeException(String message) {
        super(message);
    }

    public OffsetOutOfRangeException(Throwable cause) {
        super(cause);
    }

    public OffsetOutOfRangeException(String message, Throwable cause) {
        super(message, cause);
    }

}
