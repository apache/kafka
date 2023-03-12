package org.apache.kafka.common.serialization;

import org.apache.kafka.common.errors.SerializationException;

public class BooleanDeserializer implements Deserializer<Boolean> {
    private static final byte TRUE = 0x01;
    private static final byte FALSE = 0x00;

    @Override
    public Boolean deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }

        if (data.length != 1) {
            throw new SerializationException("Size of data received by BooleanDeserializer is not 1");
        }

        if (data[0] == TRUE) {
            return true;
        } else if (data[0] == FALSE) {
            return false;
        } else {
            throw new SerializationException("Unexpected byte received by BooleanDeserializer: " + data[0]);
        }
    }
}
