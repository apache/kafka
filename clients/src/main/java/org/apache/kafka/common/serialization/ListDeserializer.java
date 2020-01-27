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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.utils.Utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public class ListDeserializer<Inner> implements Deserializer<List<Inner>> {

    private Deserializer<Inner> inner;
    private Class<?> listClass;
    private Integer primitiveSize;

    static private Map<Class<? extends Deserializer<?>>, Integer> fixedLengthDeserializers = mkMap(
        mkEntry(ShortDeserializer.class, 2),
        mkEntry(IntegerDeserializer.class, 4),
        mkEntry(FloatDeserializer.class, 4),
        mkEntry(LongDeserializer.class, 8),
        mkEntry(DoubleDeserializer.class, 8),
        mkEntry(UUIDDeserializer.class, 36)
    );

    public ListDeserializer() {}

    public <L extends List<Inner>> ListDeserializer(Class<L> listClass, Deserializer<Inner> innerDeserializer) {
        this.listClass = listClass;
        this.inner = innerDeserializer;
        if (innerDeserializer != null) {
            this.primitiveSize = fixedLengthDeserializers.get(innerDeserializer.getClass());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (listClass == null) {
            String listTypePropertyName = isKey ? CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS : CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS;
            listClass = (Class<List<Inner>>) configs.get(listTypePropertyName);
            if (listClass == null) {
                throw new ConfigException("Not able to determine the list class because it was neither passed via the constructor nor set in the config");
            }
        }
        if (inner == null) {
            String innerDeserializerPropertyName = isKey ? CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS : CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS;
            Class<Deserializer<Inner>> innerDeserializerClass = (Class<Deserializer<Inner>>) configs.get(innerDeserializerPropertyName);
            inner = Utils.newInstance(innerDeserializerClass);
            inner.configure(configs, isKey);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Inner> getListInstance(int listSize) {
        try {
            Constructor<List<Inner>> listConstructor;
            try {
                listConstructor = (Constructor<List<Inner>>) listClass.getConstructor(Integer.TYPE);
                return listConstructor.newInstance(listSize);
            } catch (NoSuchMethodException e) {
                listConstructor = (Constructor<List<Inner>>) listClass.getConstructor();
                return listConstructor.newInstance();
            }
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
                IllegalArgumentException | InvocationTargetException e) {
            throw new KafkaException("Could not construct a list instance of \"" + listClass.getCanonicalName() + "\"", e);
        }
    }

    @Override
    public List<Inner> deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            final int size = dis.readInt();
            List<Inner> deserializedList = getListInstance(size);
            for (int i = 0; i < size; i++) {
                byte[] payload = new byte[primitiveSize == null ? dis.readInt() : primitiveSize];
                if (dis.read(payload) == -1) {
                    throw new SerializationException("End of the stream was reached prematurely");
                }
                deserializedList.add(inner.deserialize(topic, payload));
            }
            return deserializedList;
        } catch (IOException e) {
            throw new KafkaException("Unable to deserialize into a List", e);
        }
    }

    @Override
    public void close() {
        if (inner != null) {
            inner.close();
        }
    }

}