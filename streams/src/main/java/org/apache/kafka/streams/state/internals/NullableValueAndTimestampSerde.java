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

import static java.util.Objects.requireNonNull;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerde;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Similar to {@link ValueAndTimestampSerde} but this serde additionally supports (de)serializing
 * {@link ValueAndTimestamp} instances for which the {@code value} is {@code null}.
 * <p>
 * The serialized format is:
 * <pre>
 *     <timestamp> + <bool indicating whether value is null> + <raw value>
 * </pre>
 * where the boolean is needed in order to distinguish between null and empty values (i.e., between
 * tombstones and {@code byte[0]} values).
 */
public class NullableValueAndTimestampSerde<V> extends WrappingNullableSerde<ValueAndTimestamp<V>, Void, V> {

    static final int RAW_TIMESTAMP_LENGTH = 8;
    static final int RAW_BOOLEAN_LENGTH = 1;

    public NullableValueAndTimestampSerde(final Serde<V> valueSerde) {
        super(
            new NullableValueAndTimestampSerializer<>(requireNonNull(valueSerde).serializer()),
            new NullableValueAndTimestampDeserializer<>(requireNonNull(valueSerde).deserializer())
        );
    }

}