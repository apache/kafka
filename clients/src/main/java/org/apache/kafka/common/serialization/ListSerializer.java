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
import org.apache.kafka.common.utils.Utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.serialization.Serdes.ListSerde.SerializationStrategy;

public class ListSerializer<Inner> implements Serializer<List<Inner>> {

    private Serializer<Inner> inner;
    private SerializationStrategy serStrategy;
    private boolean isFixedLength;

    static private List<Class<? extends Serializer<?>>> fixedLengthSerializers = Arrays.asList(
        ShortSerializer.class,
        IntegerSerializer.class,
        FloatSerializer.class,
        LongSerializer.class,
        DoubleSerializer.class,
        UUIDSerializer.class);

    public ListSerializer() {}

    public ListSerializer(Serializer<Inner> serializer) {
        this.inner = serializer;
        this.isFixedLength = serializer != null && fixedLengthSerializers.contains(serializer.getClass());
        this.serStrategy = this.isFixedLength ? SerializationStrategy.NULL_INDEX_LIST : SerializationStrategy.NEGATIVE_SIZE;
    }

    public ListSerializer(Serializer<Inner> serializer, SerializationStrategy serStrategy) {
        this(serializer);
        this.serStrategy = serStrategy;
    }

    public Serializer<Inner> getInnerSerializer() {
        return inner;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (inner == null) {
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
            } catch (final ClassNotFoundException e) {
                throw new ConfigException(innerSerdePropertyName, innerSerdeClassOrName, "Serializer class " + innerSerdeClassOrName + " could not be found.");
            }
        }
    }

    private void serializeNullIndexList(final DataOutputStream out, List<Inner> data) throws IOException {
        List<Integer> nullIndexList = IntStream.range(0, data.size())
                .filter(i -> data.get(i) == null)
                .boxed().collect(Collectors.toList());
        out.writeInt(nullIndexList.size());
        for (int i : nullIndexList) out.writeInt(i);
    }

    @Override
    public byte[] serialize(String topic, List<Inner> data) {
        if (data == null) {
            return null;
        }
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeByte(serStrategy.ordinal()); // write serialization strategy flag
            if (serStrategy == SerializationStrategy.NULL_INDEX_LIST) {
                serializeNullIndexList(out, data);
            }
            final int size = data.size();
            out.writeInt(size);
            for (Inner entry : data) {
                if (entry == null) {
                    if (serStrategy == SerializationStrategy.NEGATIVE_SIZE) {
                        out.writeInt(Serdes.ListSerde.NEGATIVE_SIZE_VALUE);
                    }
                } else {
                    final byte[] bytes = inner.serialize(topic, entry);
                    if (!isFixedLength || serStrategy == SerializationStrategy.NEGATIVE_SIZE) {
                        out.writeInt(bytes.length);
                    }
                    out.write(bytes);
                }
            }
            return baos.toByteArray();
        } catch (IOException e) {
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
