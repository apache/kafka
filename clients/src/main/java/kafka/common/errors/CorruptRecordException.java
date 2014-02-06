package kafka.common.errors;

public class CorruptRecordException extends ApiException {

    private static final long serialVersionUID = 1L;

    public CorruptRecordException() {
        super("This message has failed it's CRC checksum or is otherwise corrupt.");
    }

    public CorruptRecordException(String message) {
        super(message);
    }

    public CorruptRecordException(Throwable cause) {
        super(cause);
    }

    public CorruptRecordException(String message, Throwable cause) {
        super(message, cause);
    }

}
