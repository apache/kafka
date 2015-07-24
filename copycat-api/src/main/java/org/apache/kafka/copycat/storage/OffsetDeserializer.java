/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.storage;


import org.apache.kafka.copycat.data.Schema;

import java.io.Closeable;
import java.util.Map;

/**
 * Deserializer for Copycat offsets.
 * @param <T>
 */
public interface OffsetDeserializer<T> extends Closeable {

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     * @param isKey whether is for key or value
     */
    public void configure(Map<String, ?> configs, boolean isKey);

    /**
     * Deserialize an offset key or value from the specified connector.
     * @param connector connector associated with the data
     * @param data serialized bytes
     * @return deserialized typed data
     */
    public T deserializeOffset(String connector, byte[] data);

    /**
     * Deserialize an offset key or value from the specified connector using a schema.
     * @param connector connector associated with the data
     * @param data serialized bytes
     * @param schema schema to deserialize to
     * @return deserialized typed data
     */
    public T deserializeOffset(String connector, byte[] data, Schema schema);

    @Override
    public void close();
}
