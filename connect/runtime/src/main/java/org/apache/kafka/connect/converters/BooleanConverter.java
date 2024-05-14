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
package org.apache.kafka.connect.converters;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.BooleanDeserializer;
import org.apache.kafka.common.serialization.BooleanSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;

/**
 * {@link Converter} and {@link HeaderConverter} implementation that supports serializing to and
 * deserializing from Boolean values.
 * <p>
 * When converting from bytes to Kafka Connect format, the converter will always return an optional
 * BOOLEAN schema.
 */
public class BooleanConverter implements Converter, HeaderConverter, Versioned {

    private final BooleanSerializer serializer = new BooleanSerializer();
    private final BooleanDeserializer deserializer = new BooleanDeserializer();

    @Override
    public ConfigDef config() {
        return BooleanConverterConfig.configDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        BooleanConverterConfig conf = new BooleanConverterConfig(configs);
        boolean isKey = conf.type() == ConverterType.KEY;
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> conf = new HashMap<>(configs);
        conf.put(ConverterConfig.TYPE_CONFIG,
            isKey ? ConverterType.KEY.getName() : ConverterType.VALUE.getName());
        configure(conf);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema != null && schema.type() != Type.BOOLEAN)
            throw new DataException("Invalid schema type for BooleanConverter: " + schema.type().toString());

        try {
            return serializer.serialize(topic, (Boolean) value);
        } catch (ClassCastException e) {
            throw new DataException("BooleanConverter is not compatible with objects of type " + value.getClass());
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            return new SchemaAndValue(Schema.OPTIONAL_BOOLEAN_SCHEMA,
                deserializer.deserialize(topic, value));
        } catch (SerializationException e) {
            throw new DataException("Failed to deserialize boolean: ", e);
        }
    }

    @Override
    public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
        return fromConnectData(topic, schema, value);
    }

    @Override
    public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
        return toConnectData(topic, value);
    }

    @Override
    public void close() {
        Utils.closeQuietly(this.serializer, "boolean converter serializer");
        Utils.closeQuietly(this.deserializer, "boolean converter deserializer");
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
