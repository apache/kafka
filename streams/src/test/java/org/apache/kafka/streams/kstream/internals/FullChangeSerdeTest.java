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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class FullChangeSerdeTest {
    private final FullChangeSerde<String> serde = FullChangeSerde.wrap(Serdes.String());

    /**
     * We used to serialize a Change into a single byte[]. Now, we don't anymore, but we still keep this logic here
     * so that we can produce the legacy format to test that we can still deserialize it.
     */
    private static byte[] mergeChangeArraysIntoSingleLegacyFormattedArray(final Change<byte[]> serialChange) {
        if (serialChange == null) {
            return null;
        }

        final int oldSize = serialChange.oldValue == null ? -1 : serialChange.oldValue.length;
        final int newSize = serialChange.newValue == null ? -1 : serialChange.newValue.length;

        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2 + Math.max(0, oldSize) + Math.max(0, newSize));


        buffer.putInt(oldSize);
        if (serialChange.oldValue != null) {
            buffer.put(serialChange.oldValue);
        }

        buffer.putInt(newSize);
        if (serialChange.newValue != null) {
            buffer.put(serialChange.newValue);
        }
        return buffer.array();
    }

    @Test
    public void shouldRoundTripNull() {
        assertThat(serde.serializeParts(null, null), nullValue());
        assertThat(mergeChangeArraysIntoSingleLegacyFormattedArray(null), nullValue());
        assertThat(FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(null), nullValue());
        assertThat(serde.deserializeParts(null, null), nullValue());
    }


    @Test
    public void shouldRoundTripNullChange() {
        assertThat(
            serde.serializeParts(null, new Change<>(null, null)),
            is(new Change<byte[]>(null, null))
        );

        assertThat(
            serde.deserializeParts(null, new Change<>(null, null)),
            is(new Change<String>(null, null))
        );

        final byte[] legacyFormat = mergeChangeArraysIntoSingleLegacyFormattedArray(new Change<>(null, null));
        assertThat(
            FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat),
            is(new Change<byte[]>(null, null))
        );
    }

    @Test
    public void shouldRoundTripOldNull() {
        final Change<byte[]> serialized = serde.serializeParts(null, new Change<>("new", null));
        final byte[] legacyFormat = mergeChangeArraysIntoSingleLegacyFormattedArray(serialized);
        final Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat);
        assertThat(
            serde.deserializeParts(null, decomposedLegacyFormat),
            is(new Change<>("new", null))
        );
    }

    @Test
    public void shouldRoundTripNewNull() {
        final Change<byte[]> serialized = serde.serializeParts(null, new Change<>(null, "old"));
        final byte[] legacyFormat = mergeChangeArraysIntoSingleLegacyFormattedArray(serialized);
        final Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat);
        assertThat(
            serde.deserializeParts(null, decomposedLegacyFormat),
            is(new Change<>(null, "old"))
        );
    }

    @Test
    public void shouldRoundTripChange() {
        final Change<byte[]> serialized = serde.serializeParts(null, new Change<>("new", "old"));
        final byte[] legacyFormat = mergeChangeArraysIntoSingleLegacyFormattedArray(serialized);
        final Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat);
        assertThat(
            serde.deserializeParts(null, decomposedLegacyFormat),
            is(new Change<>("new", "old"))
        );
    }
}
