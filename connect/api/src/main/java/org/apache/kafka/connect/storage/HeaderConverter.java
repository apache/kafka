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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Header;

import java.io.Closeable;

public interface HeaderConverter extends Configurable, Closeable {

    /**
     * Convert the header name and byte array value into a {@link Header} object.
     * @param topic the name of the topic for the record containing the header
     * @param headerKey the header's key; may not be null
     * @param value the header's raw value; may be null
     * @return the {@link SchemaAndValue}; may not be null
     */
    SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value);

    /**
     * Convert the {@link Header}'s {@link Header#value() value} into its byte array representation.
     * @param topic the name of the topic for the record containing the header
     * @param headerKey the header's key; may not be null
     * @param schema the schema for the header's value; may be null
     * @param value the header's value to convert; may be null
     * @return the byte array form of the Header's value; may be null if the value is null
     */
    byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value);

    /**
     * Configuration specification for this set of header converters.
     * @return the configuration specification; may not be null
     */
    ConfigDef config();
}
