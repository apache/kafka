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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.internals.WrappingNullableDeserializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;

import java.util.Map;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableDeserializer;

public class LeftOrRightValueDeserializer<V1, V2> implements WrappingNullableDeserializer<LeftOrRightValue<V1, V2>, Void, Object> {
    public Deserializer<V1> leftDeserializer;
    public Deserializer<V2> rightDeserializer;

    public LeftOrRightValueDeserializer(final Deserializer<V1> leftDeserializer, final Deserializer<V2> rightDeserializer) {
        this.leftDeserializer = leftDeserializer;
        this.rightDeserializer = rightDeserializer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setIfUnset(final SerdeGetter getter) {
        if (leftDeserializer == null) {
            leftDeserializer = (Deserializer<V1>) getter.valueSerde().deserializer();
        }

        if (rightDeserializer == null) {
            rightDeserializer = (Deserializer<V2>) getter.valueSerde().deserializer();
        }

        initNullableDeserializer(leftDeserializer, getter);
        initNullableDeserializer(rightDeserializer, getter);
    }

    @Override
    public void configure(final Map<String, ?> configs,
                          final boolean isKey) {
        leftDeserializer.configure(configs, isKey);
        rightDeserializer.configure(configs, isKey);
    }

    @Override
    public LeftOrRightValue<V1, V2> deserialize(final String topic, final byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        return (data[0] == 1)
            ? LeftOrRightValue.makeLeftValue(leftDeserializer.deserialize(topic, rawValue(data)))
            : LeftOrRightValue.makeRightValue(rightDeserializer.deserialize(topic, rawValue(data)));
    }

    private byte[] rawValue(final byte[] data) {
        final byte[] rawValue = new byte[data.length - 1];
        System.arraycopy(data, 1, rawValue, 0, rawValue.length);
        return rawValue;
    }
}