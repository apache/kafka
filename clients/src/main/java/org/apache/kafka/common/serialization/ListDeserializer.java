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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListDeserializer<T> implements Deserializer<List<T>> {

    private Deserializer<T> inner;
    private Class listClass;
    private Integer primitiveSize;

    private Map<Class, Integer> primitiveDeserializers = new HashMap<Class, Integer>() {{
        put(LongDeserializer.class, 8);
        put(IntegerDeserializer.class, 4);
        put(ShortDeserializer.class, 2);
        put(FloatDeserializer.class, 4);
        put(DoubleDeserializer.class, 8);
    }};

    public ListDeserializer() {
    }

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