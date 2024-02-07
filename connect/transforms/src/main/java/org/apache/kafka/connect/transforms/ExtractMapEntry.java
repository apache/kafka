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
package org.apache.kafka.connect.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireSchema;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class ExtractMapEntry<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Extract the specified entry from a map when schema present. "
            + "Any null values are passed through unmodified."
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName()
            + "</code>) " + "or value (<code>" + Value.class.getName() + "</code>).";

    private static final String ENTRY_CONFIG = "entry";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    ENTRY_CONFIG, ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Map entry key to extract.");

    private static final String PURPOSE = "map entry extraction";

    private String entryKey;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        entryKey = config.getString(ENTRY_CONFIG);
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        requireSchema(schema, PURPOSE);
        if (schema.type()!=Schema.Type.MAP) {
            throw new DataException("Map schema required for [" + PURPOSE + "]");
        }
        Object entryKey = cast(this.entryKey, schema.keySchema().type());
        final Map<String, ?> value = requireMapOrNull(operatingValue(record), PURPOSE);
        return newRecord(record, schema.valueSchema(), value == null ? null : value.get(entryKey));
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected Object cast(String entryKey, Schema.Type targetType) {
        switch (targetType) {
            case INT8:
                return Byte.parseByte(entryKey);
            case INT16:
                return Short.parseShort(entryKey);
            case INT32:
                return Integer.parseInt(entryKey);
            case INT64:
                return Long.parseLong(entryKey);
            case STRING:
                return entryKey;
            default:
                throw new DataException("Map schema with integer or string key required for [" + PURPOSE + "]");
        }
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ExtractMapEntry<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends ExtractMapEntry<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

}
