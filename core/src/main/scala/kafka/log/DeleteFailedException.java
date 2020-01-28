package kafka.log;

public class DeleteFailedException extends RuntimeException {
    public DeleteFailedException(String message, Exception exp){
        super(message, exp);
    }
}
