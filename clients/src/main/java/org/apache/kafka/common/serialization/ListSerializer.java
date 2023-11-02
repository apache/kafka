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

import java.util.ArrayList;
import java.util.Iterator;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.common.serialization.Serdes.ListSerde.SerializationStrategy;

public class ListSerializer<Inner> implements Serializer<List<Inner>> {

    final Logger log = LoggerFactory.getLogger(ListSerializer.class);

    private static final List<Class<? extends Serializer<?>>> FIXED_LENGTH_SERIALIZERS = Arrays.asList(
        ShortSerializer.class,
        IntegerSerializer.class,
        FloatSerializer.class,
        LongSerializer.class,
        DoubleSerializer.class,
        UUIDSerializer.class);

    private Serializer<Inner> inner;
    private SerializationStrategy serStrategy;

    public ListSerializer() {}

    public ListSerializer(Serializer<Inner> inner) {
        if (inner == null) {
            throw new IllegalArgumentException("ListSerializer requires \"serializer\" parameter to be provided during initialization");
        }
        this.inner = inner;
        this.serStrategy = FIXED_LENGTH_SERIALIZERS.contains(inner.getClass()) ? SerializationStrategy.CONSTANT_SIZE : SerializationStrategy.VARIABLE_SIZE;
    }

    public Serializer<Inner> getInnerSerializer() {
        return inner;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (inner != null) {
            log.error("Could not configure ListSerializer as the parameter has already been set -- inner: {}", inner);
            throw new ConfigException("List serializer was already initialized using a non-default constructor");
        }
        final String innerSerdePropertyName = isKey ? CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS : CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS;
        final Object innerSerdeClassOrName = configs.get(innerSerdePropertyName);
        if (innerSerdeClassOrName == null) {
            throw new ConfigException("Not able to determine the serializer class because it was neither passed via the constructor nor set in the config.");
        }
        try {
            if (innerSerdeClassOrName instanceof String) {
                inner = Utils.newInstance((String) innerSerdeClassOrName, Serde.class).serializer();
            } else if (innerSerdeClassOrName instanceof Class) {
                inner = (Serializer<Inner>) ((Serde) Utils.newInstance((Class) innerSerdeClassOrName)).serializer();
            } else {
                throw new KafkaException("Could not create a serializer class instance using \"" + innerSerdePropertyName + "\" property.");
            }
            inner.configure(configs, isKey);
            serStrategy = FIXED_LENGTH_SERIALIZERS.contains(inner.getClass()) ? SerializationStrategy.CONSTANT_SIZE : SerializationStrategy.VARIABLE_SIZE;
        } catch (final ClassNotFoundException e) {
            throw new ConfigException(innerSerdePropertyName, innerSerdeClassOrName, "Serializer class " + innerSerdeClassOrName + " could not be found.");
        }
    }

    private void serializeNullIndexList(final DataOutputStream out, List<Inner> data) throws IOException {
        int i = 0;
        List<Integer> nullIndexList = new ArrayList<>();
        for (Iterator<Inner> it = data.listIterator(); it.hasNext(); i++) {
            if (it.next() == null) {
                nullIndexList.add(i);
            }
        }
        out.writeInt(nullIndexList.size());
        for (int nullIndex : nullIndexList) {
            out.writeInt(nullIndex);
        }
    }

    @Override
    public byte[] serialize(String topic, List<Inner> data) {
        if (data == null) {
            return null;
        }
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeByte(serStrategy.ordinal()); // write serialization strategy flag
            if (serStrategy == SerializationStrategy.CONSTANT_SIZE) {
                // In CONSTANT_SIZE strategy, indexes of null entries are encoded in a null index list
                serializeNullIndexList(out, data);
            }
            final int size = data.size();
            out.writeInt(size);
            for (Inner entry : data) {
                if (entry == null) {
                    if (serStrategy == SerializationStrategy.VARIABLE_SIZE) {
                        out.writeInt(Serdes.ListSerde.NULL_ENTRY_VALUE);
                    }
                } else {
                    final byte[] bytes = inner.serialize(topic, entry);
                    if (serStrategy == SerializationStrategy.VARIABLE_SIZE) {
                        out.writeInt(bytes.length);
                    }
                    out.write(bytes);
                }
            }
            return baos.toByteArray();
        } catch (IOException e) {
            log.error("Failed to serialize list due to", e);
            log.trace("List that could not be serialized: {}", data); // avoid logging actual data above TRACE level since it may contain sensitive information
            throw new KafkaException("Failed to serialize List", e);
        }
    }

    @Override
    public void close() {
        if (inner != null) {
            inner.close();
        }
    }

}
