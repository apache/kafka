package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Murmur3;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class SubscriptionResponseWrapperSerdeTest {

    @Test
    @SuppressWarnings("unchecked")
    public void serdeTest(){
        long[] hashedValue = Murmur3.hash128(new byte[]{(byte)(0x01), (byte)(0x9A), (byte)(0xFF), (byte)(0x00)});
        SubscriptionResponseWrapper<String> srw = new SubscriptionResponseWrapper<>(hashedValue, null);
        SubscriptionResponseWrapperSerde srwSerde = new SubscriptionResponseWrapperSerde(Serdes.String().serializer(), Serdes.String().deserializer());

        byte[] serResponse = srwSerde.serializer().serialize(null, srw);
        SubscriptionResponseWrapper<String> result = (SubscriptionResponseWrapper<String>)srwSerde.deserializer().deserialize(null, serResponse);

        assertEquals(hashedValue, result.getOriginalValueHash());
        assertEquals(null, result.getForeignValue());
    }
}
