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

package org.apache.kafka.connect.storage;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Map;

/**
 * The Converter interface provides support for translating between Kafka Connect's runtime data format
 * and byte[]. Internally, this likely includes an intermediate step to the format used by the serialization
 * layer (e.g. JsonNode, GenericRecord, Message).
 */
@InterfaceStability.Unstable
public interface Converter {

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     * @param isKey whether is for key or value
     */
    void configure(Map<String, ?> configs, boolean isKey);

    /**
     * Convert a Kafka Connect data object to a native object for serialization.
     * @param topic the topic associated with the data
     * @param schema the schema for the value
     * @param value the value to convert
     * @return the serialized value
     */
    byte[] fromConnectData(String topic, Schema schema, Object value);

    /**
     * Convert a native object to a Kafka Connect data object.
     * @param topic the topic associated with the data
     * @param value the value to convert
     * @return an object containing the {@link Schema} and the converted value
     */
    SchemaAndValue toConnectData(String topic, byte[] value);
}
