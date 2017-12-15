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
package org.apache.kafka.streams.test;

import org.apache.kafka.common.serialization.Serializer;

/**
 * Convenience class for a test record.
 *
 * @param <K> key type
 * @param <V> value type
 * @see TestRecordFactory
 */
// TODO add annotation -- need to decide which
public class TestRecord<K, V> {
    private final String topicName;
    private final byte[] key;
    private final byte[] value;
    private final long timestamp;

    @SuppressWarnings("ConstantConditions")
    public TestRecord(final String topicName,
                      final K key,
                      final V value,
                      final long timestamp,
                      final Serializer<K> keySerializer,
                      final Serializer<V> valueSerializer) {
        this(topicName,
             serialize("key", topicName, key, keySerializer),
             serialize("value", topicName, value, valueSerializer),
             timestamp);
    }

    public TestRecord(final String topicName,
                      final byte[] key,
                      final byte[] value,
                      final long timestamp) {
        this.topicName = topicName;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String topicName() {
        return topicName;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    static <KV> byte[] serialize(final String type,
                                 final String topicName,
                                 final KV keyOrValue,
                                 final Serializer<KV> keyOrValueSerializer) {
        if (keyOrValueSerializer == null) {
            if (keyOrValue != null) {
                throw new NullPointerException(type + "Serializer cannot be null if " + type + " is not null.");
            }
            return null;
        }
        return keyOrValueSerializer.serialize(topicName, keyOrValue);
    }

}
