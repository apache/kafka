package kafka.common.record;

public class InvalidRecordException extends RuntimeException {

    private static final long serialVersionUID = 1;

    public InvalidRecordException(String s) {
        super(s);
    }

}
