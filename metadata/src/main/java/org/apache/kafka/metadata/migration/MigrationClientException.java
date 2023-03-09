package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.KafkaException;

public class MigrationClientException extends KafkaException {
    public MigrationClientException(String message, Throwable t) {
        super(message, t);
    }

    public MigrationClientException(Throwable t) {
        super(t);
    }

    public MigrationClientException(String message) {
        super(message);
    }
}
