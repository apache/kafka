package kafka.common.errors;

public class OffsetMetadataTooLarge extends ApiException {

    private static final long serialVersionUID = 1L;

    public OffsetMetadataTooLarge() {
    }

    public OffsetMetadataTooLarge(String message) {
        super(message);
    }

    public OffsetMetadataTooLarge(Throwable cause) {
        super(cause);
    }

    public OffsetMetadataTooLarge(String message, Throwable cause) {
        super(message, cause);
    }

}
