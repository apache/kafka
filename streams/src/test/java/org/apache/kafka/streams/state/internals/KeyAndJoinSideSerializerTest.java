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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;

public class KeyAndJoinSideSerializerTest {
    private static final String TOPIC = "some-topic";

    private static final KeyAndJoinSideSerde<String> STRING_SERDE =
        new KeyAndJoinSideSerde<>(Serdes.String());

    @Test
    public void shouldSerializeKeyWithJoinSideAsTrue() {
        final String value = "some-string";

        final KeyAndJoinSide<String> keyAndJoinSide = KeyAndJoinSide.make(true, value);

        final byte[] serialized =
            STRING_SERDE.serializer().serialize(TOPIC, keyAndJoinSide);

        assertThat(serialized, is(notNullValue()));

        final KeyAndJoinSide<String> deserialized =
            STRING_SERDE.deserializer().deserialize(TOPIC, serialized);

        assertThat(deserialized, is(keyAndJoinSide));
    }

    @Test
    public void shouldSerializeKeyWithJoinSideAsFalse() {
        final String value = "some-string";

        final KeyAndJoinSide<String> keyAndJoinSide = KeyAndJoinSide.make(false, value);

        final byte[] serialized =
            STRING_SERDE.serializer().serialize(TOPIC, keyAndJoinSide);

        assertThat(serialized, is(notNullValue()));

        final KeyAndJoinSide<String> deserialized =
            STRING_SERDE.deserializer().deserialize(TOPIC, serialized);

        assertThat(deserialized, is(keyAndJoinSide));
    }

    @Test
    public void shouldThrowIfSerializeNullData() {
        assertThrows(NullPointerException.class,
            () -> STRING_SERDE.serializer().serialize(TOPIC, KeyAndJoinSide.make(true, null)));
    }
}
