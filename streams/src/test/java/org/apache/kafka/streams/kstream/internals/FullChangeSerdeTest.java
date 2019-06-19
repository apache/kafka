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

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class FullChangeSerdeTest {
    private final FullChangeSerde<String> serde = FullChangeSerde.wrap(Serdes.String());

    @Test
    public void shouldRoundTripNull() {
        assertThat(serde.serializeParts(null, null), nullValue());
        assertThat(FullChangeSerde.composeLegacyFormat(null), nullValue());
        assertThat(FullChangeSerde.decomposeLegacyFormat(null), nullValue());
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

        final byte[] legacyFormat = FullChangeSerde.composeLegacyFormat(new Change<>(null, null));
        assertThat(
            FullChangeSerde.decomposeLegacyFormat(legacyFormat),
            is(new Change<byte[]>(null, null))
        );
    }

    @Test
    public void shouldRoundTripOldNull() {
        final Change<byte[]> serialized = serde.serializeParts(null, new Change<>("new", null));
        final byte[] legacyFormat = FullChangeSerde.composeLegacyFormat(serialized);
        final Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormat(legacyFormat);
        assertThat(
            serde.deserializeParts(null, decomposedLegacyFormat),
            is(new Change<>("new", null))
        );
    }

    @Test
    public void shouldRoundTripNewNull() {
        final Change<byte[]> serialized = serde.serializeParts(null, new Change<>(null, "old"));
        final byte[] legacyFormat = FullChangeSerde.composeLegacyFormat(serialized);
        final Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormat(legacyFormat);
        assertThat(
            serde.deserializeParts(null, decomposedLegacyFormat),
            is(new Change<>(null, "old"))
        );
    }

    @Test
    public void shouldRoundTripChange() {
        final Change<byte[]> serialized = serde.serializeParts(null, new Change<>("new", "old"));
        final byte[] legacyFormat = FullChangeSerde.composeLegacyFormat(serialized);
        final Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormat(legacyFormat);
        assertThat(
            serde.deserializeParts(null, decomposedLegacyFormat),
            is(new Change<>("new", "old"))
        );
    }
}
