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
import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes.ListSerde;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.serialization.Serdes.ListSerde.SerializationStrategy;

@InterfaceAudience.Public
public class ListDeserializer<Inner> implements Deserializer<List<Inner>> {

    final Logger log = LoggerFactory.getLogger(ListDeserializer.class);

    private static final Map<Class<? extends Deserializer<?>>, Integer> FIXED_LENGTH_DESERIALIZERS = Map.of(
        ShortDeserializer.class, Short.BYTES,
        IntegerDeserializer.class, Integer.BYTES,
        FloatDeserializer.class, Float.BYTES,
        LongDeserializer.class, Long.BYTES,
        DoubleDeserializer.class, Double.BYTES,
        UUIDDeserializer.class, 36
    );

    private Deserializer<Inner> inner;
    private Class<?> listClass;
    private Integer primitiveSize;

    public ListDeserializer() {}

    public <L extends List<Inner>> ListDeserializer(Class<L> listClass, Deserializer<Inner> inner) {
        if (listClass == null || inner == null) {
            log.error("Could not construct ListDeserializer as not all required parameters were present -- listClass: {}, inner: {}", listClass, inner);
            throw new IllegalArgumentException("ListDeserializer requires both \"listClass\" and \"innerDeserializer\" parameters to be provided during initialization");
        }
        this.listClass = listClass;
        this.inner = inner;
        this.primitiveSize = FIXED_LENGTH_DESERIALIZERS.get(inner.getClass());
    }

    public Deserializer<Inner> innerDeserializer() {
        return inner;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (listClass != null || inner != null) {
            log.error("Could not configure ListDeserializer as some parameters were already set -- listClass: {}, inner: {}", listClass, inner);
            throw new ConfigException("List deserializer was already initialized using a non-default constructor");
        }
        configureListClass(configs, isKey);
        configureInnerSerde(configs, isKey);
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
            primitiveSize = FIXED_LENGTH_DESERIALIZERS.get(inner.getClass());
        } catch (final ClassNotFoundException e) {
            throw new ConfigException(innerSerdePropertyName, innerSerdeClassOrName, "Deserializer's inner serde class \"" + innerSerdeClassOrName + "\" could not be found.");
        }
    }

    @SuppressWarnings("unchecked")
    private List<Inner> createListInstance(int listSize) {
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
            log.error("Failed to construct list due to ", e);
            throw new KafkaException("Could not construct a list instance of \"" + listClass.getCanonicalName() + "\"", e);
        }
    }

    private SerializationStrategy parseSerializationStrategyFlag(final int serializationStrategyFlag) throws IOException {
        if (serializationStrategyFlag < 0 || serializationStrategyFlag >= SerializationStrategy.VALUES.length) {
            throw new SerializationException("Invalid serialization strategy flag value");
        }
        return SerializationStrategy.VALUES[serializationStrategyFlag];
    }

    private List<Integer> deserializeNullIndexList(final DataInputStream dis, final int length) throws IOException {
        int nullIndexListSize = dis.readInt();
        if (nullIndexListSize < 0) {
            throw new SerializationException("Corrupted byte[]. The number of null list entries cannot be negative.");
        }
        if (nullIndexListSize > length / Integer.BYTES) {
            throw new SerializationException("Corrupted byte[]. The number of null list entries cannot be larger than overall number of elements.");
        }
        List<Integer> nullIndexList = new ArrayList<>(nullIndexListSize);
        while (nullIndexListSize != 0) {
            nullIndexList.add(dis.readInt());
            nullIndexListSize--;
        }
        return nullIndexList;
    }

    @Override
    public List<Inner> deserialize(String topic, byte[] data) {
        return deserialize(topic, new RecordHeaders(), data);
    }

    @Override
    public List<Inner> deserialize(String topic, Headers headers, byte[] data) {
        if (data == null) {
            return null;
        }
        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            SerializationStrategy serStrategy = parseSerializationStrategyFlag(dis.readByte());
            List<Integer> nullIndexList = null;
            if (serStrategy == SerializationStrategy.CONSTANT_SIZE) {
                if (primitiveSize == null) {
                    throw new SerializationException("Data is encoded as constant size entries, but configured inner deserializer is not a known fixed-size deserializer.");
                }
                // In CONSTANT_SIZE strategy, indexes of null entries are decoded from a null index list
                nullIndexList = deserializeNullIndexList(dis, data.length);
            }
            final int size = readListSize(dis, data.length);
            List<Inner> deserializedList = createListInstance(size);
            for (int i = 0; i < size; i++) {
                int entrySize = readEntrySize(dis, serStrategy, data.length);

                if (entrySize == ListSerde.NULL_ENTRY_VALUE || (nullIndexList != null && nullIndexList.contains(i))) {
                    deserializedList.add(null);
                    continue;
                }
                byte[] payload = new byte[entrySize];
                try {
                    // Use readFully (not read) so a stream truncated mid-entry is detected: read(byte[]) may
                    // return a partial count without reaching EOF, which would silently leave the tail of the
                    // payload zero-filled and hand a corrupted entry to the inner deserializer.
                    dis.readFully(payload);
                } catch (EOFException e) {
                    log.error("Ran out of bytes in serialized list");
                    log.trace("Deserialized list so far: {}", deserializedList); // avoid logging actual data above TRACE level since it may contain sensitive information
                    throw new SerializationException("End of the stream was reached prematurely");
                }
                deserializedList.add(inner.deserialize(topic, headers, payload));
            }
            return deserializedList;
        } catch (IOException e) {
            throw new KafkaException("Unable to deserialize into a List", e);
        }
    }

    private int readListSize(final DataInputStream dis, final int length) throws IOException {
        final int size = dis.readInt();
        if (size < 0) {
            throw new SerializationException("Corrupted byte[]. The number of list entries cannot be negative.");
        }
        if (size > length) {
            throw new SerializationException("Corrupted byte[]. The number of list entries cannot be larger than overall number of bytes.");
        }
        return size;
    }

    private int readEntrySize(
        final DataInputStream dis,
        final SerializationStrategy serStrategy,
        final int length
    ) throws IOException {
        if (serStrategy == SerializationStrategy.CONSTANT_SIZE) {
            return primitiveSize;
        } else {
            final int entrySize = dis.readInt();
            if (entrySize < -1) { // value `-1` is valid, encoding a null entry (-> ListSerde.NULL_ENTRY_VALUE)
                throw new SerializationException("Corrupted byte[]. A list entry cannot have negative size.");
            }
            if (entrySize > length) {
                throw new SerializationException("Corrupted byte[]. A list entry cannot be larger than the overall number of bytes.");
            }
            return entrySize;
        }
    }

    @Override
    public void close() {
        if (inner != null) {
            inner.close();
        }
    }

}
