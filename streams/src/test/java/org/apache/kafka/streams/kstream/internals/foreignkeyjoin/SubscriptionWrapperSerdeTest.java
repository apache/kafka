package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.utils.Murmur3;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SubscriptionWrapperSerdeTest {

    @Test
    @SuppressWarnings("unchecked")
    public void serdeTest() {
        SubscriptionWrapperSerde swSerde = new SubscriptionWrapperSerde();
        long[] hashedValue = Murmur3.hash128(new byte[]{(byte)(0xFF), (byte)(0xAA), (byte)(0x00), (byte)(0x19)});
        SubscriptionWrapper wrapper = new SubscriptionWrapper(hashedValue, false);
        byte[] serialized = swSerde.serializer().serialize(null, wrapper);
        SubscriptionWrapper deserialized = (SubscriptionWrapper)swSerde.deserializer().deserialize(null, serialized);

        assertFalse(deserialized.isPropagate());
        assertEquals(hashedValue, deserialized.getHash());
    }
}
