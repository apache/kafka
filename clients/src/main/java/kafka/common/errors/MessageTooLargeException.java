package kafka.common.errors;

public class MessageTooLargeException extends ApiException {

    private static final long serialVersionUID = 1L;

    public MessageTooLargeException() {
        super();
    }

    public MessageTooLargeException(String message, Throwable cause) {
        super(message, cause);
    }

    public MessageTooLargeException(String message) {
        super(message);
    }

    public MessageTooLargeException(Throwable cause) {
        super(cause);
    }

}
