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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ListDeserializer<T> implements Deserializer<List<T>> {

    private final Class listClass;
    private final Deserializer<T> deserializer;
    private final Integer primitiveSize;

    private Map<Class, Integer> primitiveDeserializers = Stream.of(new Object[][]{
            {LongDeserializer.class, 8},
            {IntegerDeserializer.class, 4},
            {ShortDeserializer.class, 2},
            {FloatDeserializer.class, 4},
            {DoubleDeserializer.class, 8},
            {BytesDeserializer.class, 1}
    }).collect(Collectors.toMap(e -> (Class) e[0], e -> (Integer) e[1]));

    public ListDeserializer(Class listClass, Deserializer<T> deserializer) {
        this.listClass = listClass;
        this.deserializer = deserializer;
        this.primitiveSize = primitiveDeserializers.get(deserializer.getClass());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        deserializer.configure(configs, isKey);
    }

    private List<T> getListInstance(int listSize) {
        try {
            Constructor<?> listConstructor = listClass.getConstructor(Integer.TYPE);
            return (List<T>) listConstructor.newInstance(listSize);
        } catch (Exception e) {
            throw new RuntimeException("Could not construct a list instance of \"" + listClass.getCanonicalName() + "\"", e);
        }
    }

    @Override
    public List<T> deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            final int size = dis.readInt();
            List<T> deserializedList = getListInstance(size);
            for (int i = 0; i < size; i++) {
                byte[] payload;
                payload = new byte[primitiveSize == null ? dis.readInt() : primitiveSize];
                dis.read(payload);
                deserializedList.add(deserializer.deserialize(topic, payload));
            }
            return deserializedList;
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize into a List", e);
        }
    }

    @Override
    public void close() {
        deserializer.close();
    }

}