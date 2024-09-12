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
package org.apache.kafka.connect.storage;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Map;

/**
 * The Converter interface provides support for translating between Kafka Connect's runtime data format
 * and byte[]. Internally, this likely includes an intermediate step to the format used by the serialization
 * layer (e.g. JsonNode, GenericRecord, Message).
 * <p>Kafka Connect may discover implementations of this interface using the Java {@link java.util.ServiceLoader} mechanism.
 * To support this, implementations of this interface should also contain a service provider configuration file in
 * {@code META-INF/services/org.apache.kafka.connect.storage.Converter}.
 */
public interface Converter {

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     * @param isKey whether this converter is for a key or a value
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
     * Convert a Kafka Connect data object to a native object for serialization,
     * potentially using the supplied topic and headers in the record as necessary.
     *
     * <p>Connect uses this method directly, and for backward compatibility reasons this method
     * by default will call the {@link #fromConnectData(String, Schema, Object)} method.
     * Override this method to make use of the supplied headers.</p>
     * @param topic the topic associated with the data
     * @param headers the headers associated with the data; any changes done to the headers
     *        are applied to the message sent to the broker
     * @param schema the schema for the value
     * @param value the value to convert
     * @return the serialized value
     */
    default byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        return fromConnectData(topic, schema, value);
    }

    /**
     * Convert a native object to a Kafka Connect data object for deserialization.
     * @param topic the topic associated with the data
     * @param value the value to convert
     * @return an object containing the {@link Schema} and the converted value
     */
    SchemaAndValue toConnectData(String topic, byte[] value);

    /**
     * Convert a native object to a Kafka Connect data object for deserialization,
     * potentially using the supplied topic and headers in the record as necessary.
     *
     * <p>Connect uses this method directly, and for backward compatibility reasons this method
     * by default will call the {@link #toConnectData(String, byte[])} method.
     * Override this method to make use of the supplied headers.</p>
     * @param topic the topic associated with the data
     * @param headers the headers associated with the data
     * @param value the value to convert
     * @return an object containing the {@link Schema} and the converted value
     */
    default SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        return toConnectData(topic, value);
    }

    /**
     * Configuration specification for this converter.
     * @return the configuration specification; may not be null
     */
    default ConfigDef config() {
        return new ConfigDef();
    }
}
