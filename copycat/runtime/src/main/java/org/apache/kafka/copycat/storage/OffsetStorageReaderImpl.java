/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.storage;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.copycat.errors.CopycatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of OffsetStorageReader. Unlike OffsetStorageWriter which is implemented
 * directly, the interface is only separate from this implementation because it needs to be
 * included in the public API package.
 */
public class OffsetStorageReaderImpl<K, V> implements OffsetStorageReader {
    private static final Logger log = LoggerFactory.getLogger(OffsetStorageReaderImpl.class);

    private final OffsetBackingStore backingStore;
    private final String namespace;
    private final Converter<K> keyConverter;
    private final Converter<V> valueConverter;
    private final Serializer<K> keySerializer;
    private final Deserializer<V> valueDeserializer;

    public OffsetStorageReaderImpl(OffsetBackingStore backingStore, String namespace,
                                   Converter<K> keyConverter, Converter<V> valueConverter,
                                   Serializer<K> keySerializer, Deserializer<V> valueDeserializer) {
        this.backingStore = backingStore;
        this.namespace = namespace;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.keySerializer = keySerializer;
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public Object getOffset(Object partition) {
        return getOffsets(Arrays.asList(partition)).get(partition);
    }

    @Override
    public Map<Object, Object> getOffsets(Collection<Object> partitions) {
        // Serialize keys so backing store can work with them
        Map<ByteBuffer, Object> serializedToOriginal = new HashMap<>(partitions.size());
        for (Object key : partitions) {
            try {
                byte[] keySerialized = keySerializer.serialize(namespace, keyConverter.fromCopycatData(key));
                ByteBuffer keyBuffer = (keySerialized != null) ? ByteBuffer.wrap(keySerialized) : null;
                serializedToOriginal.put(keyBuffer, key);
            } catch (Throwable t) {
                log.error("CRITICAL: Failed to serialize partition key when getting offsets for task with "
                        + "namespace {}. No value for this data will be returned, which may break the "
                        + "task or cause it to skip some data.", namespace, t);
            }
        }

        // Get serialized key -> serialized value from backing store
        Map<ByteBuffer, ByteBuffer> raw;
        try {
            raw = backingStore.get(namespace, serializedToOriginal.keySet(), null).get();
        } catch (Exception e) {
            log.error("Failed to fetch offsets from namespace {}: ", namespace, e);
            throw new CopycatException("Failed to fetch offsets.", e);
        }

        // Deserialize all the values and map back to the original keys
        Map<Object, Object> result = new HashMap<>(partitions.size());
        for (Map.Entry<ByteBuffer, ByteBuffer> rawEntry : raw.entrySet()) {
            try {
                // Since null could be a valid key, explicitly check whether map contains the key
                if (!serializedToOriginal.containsKey(rawEntry.getKey())) {
                    log.error("Should be able to map {} back to a requested partition-offset key, backing "
                            + "store may have returned invalid data", rawEntry.getKey());
                    continue;
                }
                Object origKey = serializedToOriginal.get(rawEntry.getKey());
                Object deserializedValue = valueConverter.toCopycatData(
                        valueDeserializer.deserialize(namespace, rawEntry.getValue().array())
                );

                result.put(origKey, deserializedValue);
            } catch (Throwable t) {
                log.error("CRITICAL: Failed to deserialize offset data when getting offsets for task with"
                        + " namespace {}. No value for this data will be returned, which may break the "
                        + "task or cause it to skip some data. This could either be due to an error in "
                        + "the connector implementation or incompatible schema.", namespace, t);
            }
        }

        return result;
    }
}
