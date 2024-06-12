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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StringConverterConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link Converter} and {@link HeaderConverter} implementation that only supports serializing to and deserializing from number values.
 * It does support handling nulls. When converting from bytes to Kafka Connect format, the converter will always return the specified
 * schema.
 * <p>
 * This implementation currently does nothing with the topic names or header keys.
 */
abstract class NumberConverter<T extends Number> implements Converter, HeaderConverter, Versioned {

    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;
    private final String typeName;
    private final Schema schema;

    /**
     * Create the converter.
     *
     * @param typeName the displayable name of the type; may not be null
     * @param schema the optional schema to be used for all deserialized forms; may not be null
     * @param serializer the serializer; may not be null
     * @param deserializer the deserializer; may not be null
     */
    protected NumberConverter(String typeName, Schema schema, Serializer<T> serializer, Deserializer<T> deserializer) {
        this.typeName = typeName;
        this.schema = schema;
        this.serializer = serializer;
        this.deserializer = deserializer;
        assert this.serializer != null;
        assert this.deserializer != null;
        assert this.typeName != null;
        assert this.schema != null;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
    @Override
    public ConfigDef config() {
        return NumberConverterConfig.configDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        NumberConverterConfig conf = new NumberConverterConfig(configs);
        boolean isKey = conf.type() == ConverterType.KEY;
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> conf = new HashMap<>(configs);
        conf.put(StringConverterConfig.TYPE_CONFIG, isKey ? ConverterType.KEY.getName() : ConverterType.VALUE.getName());
        configure(conf);
    }

    @SuppressWarnings("unchecked")
    protected T cast(Object value) {
        return (T) value;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            return serializer.serialize(topic, value == null ? null : cast(value));
        } catch (ClassCastException e) {
            throw new DataException("Failed to serialize to " + typeName + " (was " + value.getClass() + "): ", e);
        } catch (SerializationException e) {
            throw new DataException("Failed to serialize to " + typeName + ": ", e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            return new SchemaAndValue(schema, deserializer.deserialize(topic, value));
        } catch (SerializationException e) {
            throw new DataException("Failed to deserialize " + typeName + ": ", e);
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
        Utils.closeQuietly(this.serializer, "number converter serializer");
        Utils.closeQuietly(this.deserializer, "number converter deserializer");
    }
}
