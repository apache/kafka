/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class CombinedKeySchemaTest {

    @Test
    public void nonNullPrimaryKeySerdeTest() {
        final CombinedKeySchema<String, Integer> cks = new CombinedKeySchema<>(
            () -> "fkTopic", Serdes.String(),
            () -> "pkTopic", Serdes.Integer()
        );
        final Integer primary = -999;
        final Bytes result = cks.toBytes("foreignKey", primary);

        final CombinedKey<String, Integer> deserializedKey = cks.fromBytes(result);
        assertEquals("foreignKey", deserializedKey.getForeignKey());
        assertEquals(primary, deserializedKey.getPrimaryKey());
    }

    @Test(expected = NullPointerException.class)
    public void nullPrimaryKeySerdeTest() {
        final CombinedKeySchema<String, Integer> cks = new CombinedKeySchema<>(
            () -> "fkTopic", Serdes.String(),
            () -> "pkTopic", Serdes.Integer()
        );
        cks.toBytes("foreignKey", null);
    }

    @Test(expected = NullPointerException.class)
    public void nullForeignKeySerdeTest() {
        final CombinedKeySchema<String, Integer> cks = new CombinedKeySchema<>(
            () -> "fkTopic", Serdes.String(),
            () -> "pkTopic", Serdes.Integer()
        );
        cks.toBytes(null, 10);
    }

    @Test
    public void prefixKeySerdeTest() {
        final CombinedKeySchema<String, Integer> cks = new CombinedKeySchema<>(
            () -> "fkTopic", Serdes.String(),
            () -> "pkTopic", Serdes.Integer()
        );
        final String foreignKey = "someForeignKey";
        final byte[] foreignKeySerializedData =
            Serdes.String().serializer().serialize("fkTopic", foreignKey);
        final Bytes prefix = cks.prefixBytes(foreignKey);

        final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + foreignKeySerializedData.length);
        buf.putInt(foreignKeySerializedData.length);
        buf.put(foreignKeySerializedData);
        final Bytes expectedPrefixBytes = Bytes.wrap(buf.array());

        assertEquals(expectedPrefixBytes, prefix);
    }

    @Test(expected = NullPointerException.class)
    public void nullPrefixKeySerdeTest() {
        final CombinedKeySchema<String, Integer> cks = new CombinedKeySchema<>(
            () -> "fkTopic", Serdes.String(),
            () -> "pkTopic", Serdes.Integer()
        );
        final String foreignKey = null;
        cks.prefixBytes(foreignKey);
    }
}
