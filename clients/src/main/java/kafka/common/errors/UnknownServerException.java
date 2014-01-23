package kafka.common.errors;

public class UnknownServerException extends ApiException {

    private static final long serialVersionUID = 1L;

    public UnknownServerException() {
    }

    public UnknownServerException(String message) {
        super(message);
    }

    public UnknownServerException(Throwable cause) {
        super(cause);
    }

    public UnknownServerException(String message, Throwable cause) {
        super(message, cause);
    }

}
