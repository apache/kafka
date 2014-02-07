package org.apache.kafka.common.metrics;

import org.apache.kafka.common.KafkaException;

/**
 * Thrown when a sensor records a value that causes a metric to go outside the bounds configured as its quota
 */
public class QuotaViolationException extends KafkaException {

    private static final long serialVersionUID = 1L;

    public QuotaViolationException(String m) {
        super(m);
    }

}
