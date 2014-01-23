package kafka.common.protocol;

import java.util.HashMap;
import java.util.Map;

import kafka.common.errors.ApiException;
import kafka.common.errors.CorruptMessageException;
import kafka.common.errors.LeaderNotAvailableException;
import kafka.common.errors.MessageTooLargeException;
import kafka.common.errors.NetworkException;
import kafka.common.errors.NotLeaderForPartitionException;
import kafka.common.errors.OffsetMetadataTooLarge;
import kafka.common.errors.OffsetOutOfRangeException;
import kafka.common.errors.TimeoutException;
import kafka.common.errors.UnknownServerException;
import kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * This class contains all the client-server errors--those errors that must be sent from the server to the client. These
 * are thus part of the protocol. The names can be changed but the error code cannot.
 * 
 * Do not add exceptions that occur only on the client or only on the server here.
 */
public enum Errors {
    UNKNOWN(-1, new UnknownServerException("The server experienced an unexpected error when processing the request")),
    NONE(0, null),
    OFFSET_OUT_OF_RANGE(1,
                        new OffsetOutOfRangeException("The requested offset is not within the range of offsets maintained by the server.")),
    CORRUPT_MESSAGE(2,
                    new CorruptMessageException("The message contents does not match the message CRC or the message is otherwise corrupt.")),
    UNKNOWN_TOPIC_OR_PARTITION(3, new UnknownTopicOrPartitionException("This server does not host this topic-partition.")),
    LEADER_NOT_AVAILABLE(5,
                         new LeaderNotAvailableException("There is no leader for this topic-partition as we are in the middle of a leadership election.")),
    NOT_LEADER_FOR_PARTITION(6, new NotLeaderForPartitionException("This server is not the leader for that topic-partition.")),
    REQUEST_TIMED_OUT(7, new TimeoutException("The request timed out.")),
    MESSAGE_TOO_LARGE(10,
                      new MessageTooLargeException("The request included a message larger than the max message size the server will accept.")),
    OFFSET_METADATA_TOO_LARGE(12, new OffsetMetadataTooLarge("The metadata field of the offset request was too large.")),
    NETWORK_EXCEPTION(13, new NetworkException("The server disconnected before a response was received."));

    private static Map<Class<?>, Errors> classToError = new HashMap<Class<?>, Errors>();
    private static Map<Short, Errors> codeToError = new HashMap<Short, Errors>();
    static {
        for (Errors error : Errors.values()) {
            codeToError.put(error.code(), error);
            if (error.exception != null)
                classToError.put(error.exception.getClass(), error);
        }

    }

    private final short code;
    private final ApiException exception;

    private Errors(int code, ApiException exception) {
        this.code = (short) code;
        this.exception = exception;
    }

    /**
     * An instance of the exception
     */
    public ApiException exception() {
        return this.exception;
    }

    /**
     * The error code for the exception
     */
    public short code() {
        return this.code;
    }

    /**
     * Throw the exception corresponding to this error if there is one
     */
    public void maybeThrow() {
        if (exception != null)
            throw this.exception;
    }

    /**
     * Throw the exception if there is one
     */
    public static Errors forCode(short code) {
        Errors error = codeToError.get(code);
        return error == null ? UNKNOWN : error;
    }

    /**
     * Return the error instance associated with this exception (or UKNOWN if there is none)
     */
    public static Errors forException(Throwable t) {
        Errors error = classToError.get(t.getClass());
        return error == null ? UNKNOWN : error;
    }
}
