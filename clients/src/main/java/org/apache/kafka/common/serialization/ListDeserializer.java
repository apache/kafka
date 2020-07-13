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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.serialization.Serdes.ListSerde.SerializationStrategy;
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

    public Deserializer<Inner> getInnerDeserializer() {
        return inner;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (listClass == null) {
            configureListClass(configs, isKey);
        }
        if (inner == null) {
            configureInnerSerde(configs, isKey);
        }
    }

    private void configureListClass(Map<String, ?> configs, boolean isKey) {
        String listTypePropertyName = isKey ? CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS : CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS;
        final Object listClassOrName = configs.get(listTypePropertyName);
        if (listClassOrName == null) {
            throw new ConfigException("Not able to determine the list class because it was neither passed via the constructor nor set in the config.");
        }
        try {
            if (listClassOrName instanceof String) {
                listClass = Utils.loadClass((String) listClassOrName, Object.class);
            } else if (listClassOrName instanceof Class) {
                listClass = (Class<?>) listClassOrName;
            } else {
                throw new KafkaException("Could not determine the list class instance using \"" + listTypePropertyName + "\" property.");
            }
        } catch (final ClassNotFoundException e) {
            throw new ConfigException(listTypePropertyName, listClassOrName, "Deserializer's list class \"" + listClassOrName + "\" could not be found.");
        }
    }

    @SuppressWarnings("unchecked")
    private void configureInnerSerde(Map<String, ?> configs, boolean isKey) {
        String innerSerdePropertyName = isKey ? CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS : CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS;
        final Object innerSerdeClassOrName = configs.get(innerSerdePropertyName);
        if (innerSerdeClassOrName == null) {
            throw new ConfigException("Not able to determine the inner serde class because it was neither passed via the constructor nor set in the config.");
        }
        try {
            if (innerSerdeClassOrName instanceof String) {
                inner = Utils.newInstance((String) innerSerdeClassOrName, Serde.class).deserializer();
            } else if (innerSerdeClassOrName instanceof Class) {
                inner = (Deserializer<Inner>) ((Serde) Utils.newInstance((Class) innerSerdeClassOrName)).deserializer();
            } else {
                throw new KafkaException("Could not determine the inner serde class instance using \"" + innerSerdePropertyName + "\" property.");
            }
            inner.configure(configs, isKey);
        } catch (final ClassNotFoundException e) {
            throw new ConfigException(innerSerdePropertyName, innerSerdeClassOrName, "Deserializer's inner serde class \"" + innerSerdeClassOrName + "\" could not be found.");
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

    private SerializationStrategy parseSerializationStrategyFlag(final int serializationStrategyFlag) throws IOException {
        if (serializationStrategyFlag < 0 || serializationStrategyFlag >= SerializationStrategy.VALUES.length) {
            throw new SerializationException("Invalid serialization strategy flag value");
        }
        return SerializationStrategy.VALUES[serializationStrategyFlag];
    }

    private List<Integer> deserializeNullIndexList(final DataInputStream dis) throws IOException {
        int nullIndexListSize = dis.readInt();
        List<Integer> nullIndexList = new ArrayList<>(nullIndexListSize);
        while (nullIndexListSize != 0) {
            nullIndexList.add(dis.readInt());
            nullIndexListSize--;
        }
        return nullIndexList;
    }

    @Override
    public List<Inner> deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            SerializationStrategy serStrategy = parseSerializationStrategyFlag(dis.readByte());
            List<Integer> nullIndexList = null;
            if (serStrategy == SerializationStrategy.NULL_INDEX_LIST) {
                nullIndexList = deserializeNullIndexList(dis);
            }
            final int size = dis.readInt();
            List<Inner> deserializedList = getListInstance(size);
            for (int i = 0; i < size; i++) {
                if (serStrategy == SerializationStrategy.NULL_INDEX_LIST
                        && nullIndexList.contains(i)) {
                    deserializedList.add(null);
                    continue;
                }
                int entrySize = primitiveSize == null || serStrategy == SerializationStrategy.NEGATIVE_SIZE ? dis.readInt() : primitiveSize;
                if (serStrategy == SerializationStrategy.NEGATIVE_SIZE &&
                        entrySize == Serdes.ListSerde.NEGATIVE_SIZE_VALUE) {
                    deserializedList.add(null);
                } else {
                    byte[] payload = new byte[entrySize];
                    if (dis.read(payload) == -1) {
                        throw new SerializationException("End of the stream was reached prematurely");
                    }
                    deserializedList.add(inner.deserialize(topic, payload));
                }
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
