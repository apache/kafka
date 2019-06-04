package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class CombinedKeySerdeTest {

    @Test
    public void nonNullPrimaryKeySerdeTest() {
        CombinedKeySerde<String, Integer> cks = new CombinedKeySerde<>(Serdes.String(), Serdes.Integer());
        CombinedKey<String, Integer> combinedKey = new CombinedKey<>("foreignKey", -999);
        byte[] result = cks.serializer().serialize("someTopic", combinedKey);

        CombinedKey<String, Integer> deserializedKey = cks.deserializer().deserialize("someTopic", result);
        assertEquals(combinedKey.getForeignKey(), deserializedKey.getForeignKey());
        assertEquals(combinedKey.getPrimaryKey(), deserializedKey.getPrimaryKey());
    }

    @Test
    public void nullPrimaryKeySerdeTest() {
        CombinedKeySerde<String, Integer> cks = new CombinedKeySerde<>(Serdes.String(), Serdes.Integer());
        CombinedKey<String, Integer> combinedKey = new CombinedKey<>("foreignKey", null);
        byte[] result = cks.serializer().serialize("someTopic", combinedKey);

        CombinedKey<String, Integer> deserializedKey = cks.deserializer().deserialize("someTopic", result);
        assertEquals(combinedKey.getForeignKey(), deserializedKey.getForeignKey());
        assertEquals(combinedKey.getPrimaryKey(), deserializedKey.getPrimaryKey());
    }

    @Test(expected = NullPointerException.class)
    public void nullForeignKeySerdeTest() {
        CombinedKeySerde<String, Integer> cks = new CombinedKeySerde<>(Serdes.String(), Serdes.Integer());
        CombinedKey<String, Integer> combinedKey = new CombinedKey<>(null, 10);
        byte[] result = cks.serializer().serialize("someTopic", combinedKey);

        CombinedKey<String, Integer> deserializedKey = cks.deserializer().deserialize("someTopic", result);
        assertEquals(combinedKey.getForeignKey(), deserializedKey.getForeignKey());
        assertEquals(combinedKey.getPrimaryKey(), deserializedKey.getPrimaryKey());
    }
}
