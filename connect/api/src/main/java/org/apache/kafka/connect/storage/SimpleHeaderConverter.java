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
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A {@link HeaderConverter} that serializes header values as strings and that deserializes header values to the most appropriate
 * numeric, boolean, array, or map representation. Schemas are not serialized, but are inferred upon deserialization when possible.
 */
public class SimpleHeaderConverter implements HeaderConverter, Versioned {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleHeaderConverter.class);
    private static final ConfigDef CONFIG_DEF = new ConfigDef();
    private static final SchemaAndValue NULL_SCHEMA_AND_VALUE = new SchemaAndValue(null, null);
    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // do nothing
    }

    @Override
    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        if (value == null) {
            return NULL_SCHEMA_AND_VALUE;
        }
        try {
            String str = new String(value, UTF_8);
            return Values.parseString(str);
        } catch (NoSuchElementException e) {
            throw new DataException("Failed to deserialize value for header '" + headerKey + "' on topic '" + topic + "'", e);
        } catch (Throwable t) {
            LOG.warn("Failed to deserialize value for header '{}' on topic '{}', so using byte array", headerKey, topic, t);
            return new SchemaAndValue(Schema.BYTES_SCHEMA, value);
        }
    }

    @Override
    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        return Values.convertToString(schema, value).getBytes(UTF_8);
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
