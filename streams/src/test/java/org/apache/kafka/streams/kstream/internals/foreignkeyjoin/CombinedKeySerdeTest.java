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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CombinedKeySerdeTest {

    @Test
    public void nonNullPrimaryKeySerdeTest() {
        final CombinedKeySerde<String, Integer> cks = new CombinedKeySerde<>(Serdes.String(), Serdes.Integer());
        final CombinedKey<String, Integer> combinedKey = new CombinedKey<>("foreignKey", -999);
        final byte[] result = cks.serializer().serialize("someTopic", combinedKey);

        final CombinedKey<String, Integer> deserializedKey = cks.deserializer().deserialize("someTopic", result);
        assertEquals(combinedKey.getForeignKey(), deserializedKey.getForeignKey());
        assertEquals(combinedKey.getPrimaryKey(), deserializedKey.getPrimaryKey());
    }

    @Test
    public void nullPrimaryKeySerdeTest() {
        final CombinedKeySerde<String, Integer> cks = new CombinedKeySerde<>(Serdes.String(), Serdes.Integer());
        final CombinedKey<String, Integer> combinedKey = new CombinedKey<>("foreignKey", null);
        final byte[] result = cks.serializer().serialize("someTopic", combinedKey);

        final CombinedKey<String, Integer> deserializedKey = cks.deserializer().deserialize("someTopic", result);
        assertEquals(combinedKey.getForeignKey(), deserializedKey.getForeignKey());
        assertEquals(combinedKey.getPrimaryKey(), deserializedKey.getPrimaryKey());
    }

    @Test(expected = NullPointerException.class)
    public void nullForeignKeySerdeTest() {
        final CombinedKeySerde<String, Integer> cks = new CombinedKeySerde<>(Serdes.String(), Serdes.Integer());
        final CombinedKey<String, Integer> combinedKey = new CombinedKey<>(null, 10);
        final byte[] result = cks.serializer().serialize("someTopic", combinedKey);

        final CombinedKey<String, Integer> deserializedKey = cks.deserializer().deserialize("someTopic", result);
        assertEquals(combinedKey.getForeignKey(), deserializedKey.getForeignKey());
        assertEquals(combinedKey.getPrimaryKey(), deserializedKey.getPrimaryKey());
    }
}
