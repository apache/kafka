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
 *
 * A class that implements this interface is expected to have a constructor with no parameters.
 * <p>
 * Implement {@link org.apache.kafka.common.ClusterResourceListener} to receive cluster metadata once it's available. Please see the class documentation for ClusterResourceListener for more information.
 * Implement {@link org.apache.kafka.common.metrics.Monitorable} to enable the deserializer to register metrics. The following tags are automatically added to
 * all metrics registered: <code>config</code> set to either <code>key.deserializer</code> or <code>value.deserializer</code>, and <code>class</code> set to the Deserializer class name.
 * @param <T> Type to be deserialized into.
 */
public interface Deserializer<T> extends Closeable {

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     * @param isKey whether is for key or value
     */
    default void configure(Map<String, ?> configs, boolean isKey) {
        // intentionally left blank
    }

    /**
     * Deserialize a record value from a byte array into a value or object.
     * @param topic topic associated with the data
     * @param data serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    T deserialize(String topic, byte[] data);

    /**
     * Deserialize a record value from a byte array into a value or object.
     * @param topic topic associated with the data
     * @param headers headers associated with the record; may be empty.
     * @param data serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
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
     * @param topic topic associated with the data
     * @param headers headers associated with the record; may be empty.
     * @param data serialized ByteBuffer; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    default T deserialize(String topic, Headers headers, ByteBuffer data) {
        return deserialize(topic, headers, Utils.toNullableArray(data));
    }

    /**
     * Close this deserializer.
     * <p>
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    default void close() {
        // intentionally left blank
    }
}
