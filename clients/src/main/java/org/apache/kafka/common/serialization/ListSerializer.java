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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ListSerializer<T> implements Serializer<List<T>> {

    private final Serializer<T> serializer;
    private final Boolean isPrimitive;

    private List<Class> primitiveSerializers = Arrays.asList(
            LongSerializer.class,
            IntegerSerializer.class,
            ShortSerializer.class,
            FloatSerializer.class,
            DoubleSerializer.class,
            BytesSerializer.class);

    public ListSerializer(Serializer<T> serializer) {
        this.serializer = serializer;
        this.isPrimitive = primitiveSerializers.contains(serializer.getClass());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, List<T> data) {
        if (data == null || data.size() == 0) {
            return null;
        }
        final int size = data.size();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(size);
            for (T entry : data) {
                final byte[] bytes = serializer.serialize(topic, entry);
                if (!isPrimitive) {
                    out.writeInt(bytes.length);
                }
                out.write(bytes);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize List", e);
        }
    }

    @Override
    public void close() {
        serializer.close();
    }

}