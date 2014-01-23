package kafka.clients.producer;

import kafka.common.KafkaException;

/**
 * This exception is thrown if the producer is in non-blocking mode and the rate of data production exceeds the rate at
 * which data can be sent for long enough for the alloted buffer to be exhausted.
 */
public class BufferExhaustedException extends KafkaException {

    private static final long serialVersionUID = 1L;

    public BufferExhaustedException(String message) {
        super(message);
    }

}
