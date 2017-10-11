package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class StateSerdesTest {

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfTopicNameIsNullForBuiltinTypes() {
        StateSerdes.withBuiltinTypes(null, byte[].class, byte[].class);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfKeyClassIsNullForBuiltinTypes() {
        StateSerdes.withBuiltinTypes("anyName", null, byte[].class);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfValueClassIsNullForBuiltinTypes() {
        StateSerdes.withBuiltinTypes("anyName", byte[].class, null);
    }

    @Test
    public void shouldReturnSerdesForBuiltInKeyAndValueTypesForBuiltinTypes() {
        final Class[] supportedBuildInTypes = new Class[] {
            String.class,
            Short.class,
            Integer.class,
            Long.class,
            Float.class,
            Double.class,
            byte[].class,
            ByteBuffer.class,
            Bytes.class
        };

        for (final Class keyClass : supportedBuildInTypes) {
            for (final Class valueClass : supportedBuildInTypes) {
                Assert.assertNotNull(StateSerdes.withBuiltinTypes("anyName", keyClass, valueClass));
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowForUnknownKeyTypeForBuiltinTypes() {
        StateSerdes.withBuiltinTypes("anyName", Class.class, byte[].class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowForUnknownValueTypeForBuiltinTypes() {
        StateSerdes.withBuiltinTypes("anyName", byte[].class, Class.class);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfTopicNameIsNull() {
        new StateSerdes<>(null, Serdes.ByteArray(), Serdes.ByteArray());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfKeyClassIsNull() {
        new StateSerdes<>("anyName", null, Serdes.ByteArray());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfValueClassIsNull() {
        new StateSerdes<>("anyName", Serdes.ByteArray(), null);
    }

}
