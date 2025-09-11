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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * An interface for converting bytes to objects.
 * A class that implements this interface is expected to have a constructor with no parameters.
 *
 * <p>This interface can be combined with {@link org.apache.kafka.common.ClusterResourceListener ClusterResourceListener}
 * to receive cluster metadata once it's available, as well as {@link org.apache.kafka.common.metrics.Monitorable Monitorable}
 * to enable the deserializer to register metrics. For the latter, the following tags are automatically added to
 * all metrics registered: {@code config} set to either {@code key.deserializer} or {@code value.deserializer},
 * and {@code class} set to the deserializer class name.
 *
 * @param <T> Type to be deserialized into.
 */
public interface Deserializer<T> extends Closeable {

    /**
     * Configure this class.
     *
     * @param configs
     *        configs in key/value pairs
     * @param isKey
     *        whether the deserializer is used for the key or the value
     */
    default void configure(Map<String, ?> configs, boolean isKey) {
        // intentionally left blank
    }

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * <p>It is recommended to deserialize a {@code null} byte array to a {@code null} object.
     *
     * @param topic
     *        topic associated with the data
     * @param data
     *        serialized bytes; may be {@code null}
     *
     * @return deserialized typed data; may be {@code null}
     */
    T deserialize(String topic, byte[] data);

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * <p>It is recommended to deserialize a {@code null} byte array to a {@code null} object.
     *
     * <p>Note that the passed in {@link Headers} may be empty, but never {@code null}.
     * The implementation is allowed to modify the passed in headers, as a side effect of deserialization.
     * It is considered best practice to not delete or modify existing headers, but rather only add new ones.
     *
     * @param topic
     *        topic associated with the data
     * @param headers
     *        headers associated with the record
     * @param data
     *        serialized bytes; may be {@code null}
     *
     * @return deserialized typed data; may be {@code null}
     */
    default T deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

    /**
     * Deserialize a record value from a {@link ByteBuffer} into a value or object.
     *
     * <p>If {@code ByteBufferDeserializer} is used by an application, the application code cannot make any assumptions
     * about the returned {@link ByteBuffer} like the position, limit, capacity, etc., or if it is backed by
     * {@link ByteBuffer#hasArray() an array or not}.
     *
     * <p>Similarly, if this method is overridden, the implementation cannot make any assumptions about the
     * passed in {@link ByteBuffer} either.
     *
     * <p>It is recommended to deserialize a {@code null} {@link ByteBuffer} to a {@code null} object.
     *
     * <p>Note that the passed in {@link Headers} may be empty, but never {@code null}.
     * The implementation is allowed to modify the passed in headers, as a side effect of deserialization.
     * It is considered best practice to not delete or modify existing headers, but rather only add new ones.
     *
     * @param topic
     *        topic associated with the data
     * @param headers
     *        headers associated with the record
     * @param data
     *        serialized ByteBuffer; may be {@code null}
     *
     * @return deserialized typed data; may be {@code null}
     */
    default T deserialize(String topic, Headers headers, ByteBuffer data) {
        return deserialize(topic, headers, Utils.toNullableArray(data));
    }

    /**
     * Close this deserializer.
     *
     * <p>This method must be idempotent as it may be called multiple times.
     */
    @Override
    default void close() {
        // intentionally left blank
    }
}
