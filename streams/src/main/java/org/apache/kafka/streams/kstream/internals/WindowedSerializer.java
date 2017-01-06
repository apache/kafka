/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;

import java.nio.ByteBuffer;
import java.util.Map;

public class WindowedSerializer<T> implements Serializer<Windowed<T>> {

    private static final int TIMESTAMP_SIZE = 8;

    private Serializer<T> inner;

    public WindowedSerializer(Serializer<T> inner) {
        this.inner = inner;
    }

    // Default constructor needed by Kafka
    public WindowedSerializer() {}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (inner == null) {
            inner = new StreamsConfig(configs).getConfiguredInstance(
                    isKey ? StreamsConfig.KEY_SERIALIZER_INNER_CLASS_CONFIG : StreamsConfig.VALUE_SERIALIZER_INNER_CLASS_CONFIG,
                    Serializer.class);
            inner.configure(configs, isKey);
        }
    }

    @Override
    public byte[] serialize(String topic, Windowed<T> data) {
        byte[] serializedKey = inner.serialize(topic, data.key());

        ByteBuffer buf = ByteBuffer.allocate(serializedKey.length + TIMESTAMP_SIZE);
        buf.put(serializedKey);
        buf.putLong(data.window().start());

        return buf.array();
    }

    @Override
    public void close() {
        inner.close();
    }

    public byte[] serializeBaseKey(String topic, Windowed<T> data) {
        return inner.serialize(topic, data.key());
    }

    public Serializer<T> innerSerializer() {
        return inner;
    }

}
