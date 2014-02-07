package org.apache.kafka.common.protocol.types;

import org.apache.kafka.common.KafkaException;

public class SchemaException extends KafkaException {

    private static final long serialVersionUID = 1L;

    public SchemaException(String message) {
        super(message);
    }

}
