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
package org.apache.kafka.common.serialization;

import org.apache.kafka.common.header.Headers;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class PipeDeserializer<T> implements Deserializer<T> {
    private Deserializer<byte[]> from;
    private Deserializer<T> to;

    public PipeDeserializer() {
    }

    public PipeDeserializer(Deserializer<byte[]> from, Deserializer<T> to) {
        this.from = from;
        this.to = to;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        try {
            from = (Deserializer<byte[]>) Class.forName(String.valueOf(configs.get("pipe.deserializer.from"))).getDeclaredConstructor().newInstance();
            to = (Deserializer<T>) Class.forName(String.valueOf(configs.get("pipe.deserializer.to"))).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
        from.configure(configs, isKey);
        to.configure(configs, isKey);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return to.deserialize(topic, from.deserialize(topic, data));
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return to.deserialize(topic, headers, from.deserialize(topic, headers, data));
    }
}
