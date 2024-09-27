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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireSchema;

public abstract class SetSchemaMetadata<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    private static final Logger log = LoggerFactory.getLogger(SetSchemaMetadata.class);

    public static final String OVERVIEW_DOC =
            "Set the schema name, version or both on the record's key (<code>" + Key.class.getName() + "</code>)"
                    + " or value (<code>" + Value.class.getName() + "</code>) schema.";

    private interface ConfigName {
        String SCHEMA_NAME = "schema.name";
        String SCHEMA_VERSION = "schema.version";
        String REPLACE_NULL_WITH_DEFAULT = "replace.null.with.default";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SCHEMA_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Schema name to set.")
            .define(ConfigName.SCHEMA_VERSION, ConfigDef.Type.INT, null, ConfigDef.Importance.HIGH, "Schema version to set.")
            .define(ConfigName.REPLACE_NULL_WITH_DEFAULT, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                    "Whether to replace fields that have a default value and that are null to the default value. When set to true, the default value is used, otherwise null is used.");

    private String schemaName;
    private Integer schemaVersion;
    private boolean replaceNullWithDefault;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        schemaName = config.getString(ConfigName.SCHEMA_NAME);
        schemaVersion = config.getInt(ConfigName.SCHEMA_VERSION);
        replaceNullWithDefault = config.getBoolean(ConfigName.REPLACE_NULL_WITH_DEFAULT);

        if (schemaName == null && schemaVersion == null) {
            throw new ConfigException("Neither schema name nor version configured");
        }
    }

    @Override
    public R apply(R record) {
        final Object value = operatingValue(record);
        final Schema schema = operatingSchema(record);
        if (value == null && schema == null) {
            return record;
        }
        requireSchema(schema, "updating schema metadata");
        final boolean isArray = schema.type() == Schema.Type.ARRAY;
        final boolean isMap = schema.type() == Schema.Type.MAP;
        final boolean isStruct = value instanceof Struct;
        final Schema updatedSchema = new ConnectSchema(
                schema.type(),
                schema.isOptional(),
                schema.defaultValue(),
                schemaName != null ? schemaName : schema.name(),
                schemaVersion != null ? schemaVersion : schema.version(),
                schema.doc(),
                schema.parameters(),
                isStruct ? schema.fields() : null,
                isMap ? schema.keySchema() : null,
                isMap || isArray ? schema.valueSchema() : null
        );
        log.trace("Applying SetSchemaMetadata SMT. Original schema: {}, updated schema: {}",
            schema, updatedSchema);
        return newRecord(record, updatedSchema);
    }

    private Object getFieldValue(Struct value, Field field) {
        if (replaceNullWithDefault) {
            return value.get(field);
        }
        return value.getWithoutDefault(field.name());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema);

    /**
     * Set the schema name, version or both on the record's key schema.
     */
    public static class Key<R extends ConnectRecord<R>> extends SetSchemaMetadata<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema) {
            Object updatedKey = updateSchemaIn(record.key(), updatedSchema);
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedKey, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    /**
     * Set the schema name, version or both on the record's value schema.
     */
    public static class Value<R extends ConnectRecord<R>> extends SetSchemaMetadata<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema) {
            Object updatedValue = updateSchemaIn(record.value(), updatedSchema);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

    /**
     * Utility to check the supplied key or value for references to the old Schema,
     * and if so to return an updated key or value object that references the new Schema.
     * Note that this method assumes that the new Schema may have a different name and/or version,
     * but has fields that exactly match those of the old Schema.
     * <p>
     * Currently only {@link Struct} objects have references to the {@link Schema}.
     *
     * @param keyOrValue    the key or value object; may be null
     * @param updatedSchema the updated schema that has been potentially renamed
     * @return the original key or value object if it does not reference the old schema, or
     * a copy of the key or value object with updated references to the new schema.
     */
    protected Object updateSchemaIn(Object keyOrValue, Schema updatedSchema) {
        if (keyOrValue instanceof Struct) {
            Struct origStruct = (Struct) keyOrValue;
            Struct newStruct = new Struct(updatedSchema);
            for (Field field : updatedSchema.fields()) {
                // assume both schemas have exact same fields with same names and schemas ...
                newStruct.put(field, getFieldValue(origStruct, field));
            }
            return newStruct;
        }
        return keyOrValue;
    }
}
