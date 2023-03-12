package org.apache.kafka.common.serialization;

public class BooleanSerializer implements Serializer<Boolean> {

    private static final byte TRUE = 0x01;
    private static final byte FALSE = 0x00;
    @Override
    public byte[] serialize(final String topic, final Boolean data) {
        if (data == null) {
            return null;
        }

        return new byte[] {
                data ? TRUE : FALSE
        };
    }
}
