package kafka.common.errors;

public class CorruptMessageException extends ApiException {

    private static final long serialVersionUID = 1L;

    public CorruptMessageException() {
        super("This message has failed it's CRC checksum or is otherwise corrupt.");
    }

    public CorruptMessageException(String message) {
        super(message);
    }

    public CorruptMessageException(Throwable cause) {
        super(cause);
    }

    public CorruptMessageException(String message, Throwable cause) {
        super(message, cause);
    }

}
