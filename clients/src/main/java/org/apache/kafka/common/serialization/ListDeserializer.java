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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

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

    private Map<Class, Integer> fixedLengthDeserializers = new HashMap<Class, Integer>() {{
        put(ShortDeserializer.class, 2);
        put(IntegerDeserializer.class, 4);
        put(FloatDeserializer.class, 4);
        put(LongDeserializer.class, 8);
        put(DoubleDeserializer.class, 8);
        put(UUIDDeserializer.class, 16);
    }};

    public ListDeserializer() {
    }

    public ListDeserializer(Class listClass, Deserializer<T> deserializer) {
        this.listClass = listClass;
        this.inner = deserializer;
        this.primitiveSize = fixedLengthDeserializers.get(deserializer.getClass());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (inner == null) {
            String listTypePropertyName = isKey ? CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS : CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS;
            String innerSerdePropertyName = isKey ? CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS : CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS;
            String listType = (String) configs.get(listTypePropertyName);
            String innerSerde = (String) configs.get(innerSerdePropertyName);
            try {
                listClass = Class.forName(listType);
            } catch (ClassNotFoundException e) {
                throw new ConfigException(listTypePropertyName, listType, "List type class " + listType + " could not be found.");
            }
            try {
                inner = Utils.newInstance(innerSerde, Serde.class).deserializer();
            } catch (ClassNotFoundException e) {
                throw new ConfigException(innerSerdePropertyName, innerSerde, "Serde class " + innerSerde + " could not be found.");
            }
            inner.configure(configs, isKey);
        }
    }

    private List<T> getListInstance(int listSize) {
        try {
            Constructor<?> listConstructor;
            try {
                listConstructor = listClass.getConstructor(Integer.TYPE);
                return (List<T>) listConstructor.newInstance(listSize);
            } catch (NoSuchMethodException e) {
                listConstructor = listClass.getConstructor();
                return (List<T>) listConstructor.newInstance();
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not construct a list instance of \"" + listClass.getCanonicalName() + "\"", e);
        }
    }

    @Override
    public List<T> deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            final int size = dis.readInt();
            List<T> deserializedList = getListInstance(size);
            for (int i = 0; i < size; i++) {
                byte[] payload;
                payload = new byte[primitiveSize == null ? dis.readInt() : primitiveSize];
                dis.read(payload);
                deserializedList.add(inner.deserialize(topic, payload));
            }
            return deserializedList;
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize into a List", e);
        }
    }

    @Override
    public void close() {
        inner.close();
    }

}