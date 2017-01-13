/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

public abstract class SetSchemaMetadata<R extends ConnectRecord<R>> implements Transformation<R> {

    public interface ConfigName {
        String SCHEMA_NAME = "schema.name";
        String SCHEMA_VERSION = "schema.version";
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SCHEMA_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Schema name to set.")
            .define(ConfigName.SCHEMA_VERSION, ConfigDef.Type.INT, null, ConfigDef.Importance.HIGH, "Schema version to set.");

    private String schemaName;
    private Integer schemaVersion;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        schemaName = config.getString(ConfigName.SCHEMA_NAME);
        schemaVersion = config.getInt(ConfigName.SCHEMA_VERSION);
    }

    @Override
    public R apply(R record) {
        if (schemaName == null && schemaVersion == null) return record; // no-op
        final Schema schema = operatingSchema(record);
        if (schema == null) {
            throw new DataException("Cannot update metadata on null Schema");
        }
        final boolean isArray = schema.type() == Schema.Type.ARRAY;
        final boolean isMap = schema.type() == Schema.Type.MAP;
        final Schema updatedSchema = new ConnectSchema(
                schema.type(),
                schema.isOptional(),
                schema.defaultValue(),
                schemaName != null ? schemaName : schema.name(),
                schemaVersion != null ? schemaVersion : schema.version(),
                schema.doc(),
                schema.parameters(),
                schema.fields(),
                isMap ? schema.keySchema() : null,
                isMap || isArray ? schema.valueSchema() : null
        );
        return updatedRecord(record, updatedSchema);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract R updatedRecord(R record, Schema updatedSchema);

    /**
     * Set the schema name, version or both on the record's key schema.
     */
    public static class Key<R extends ConnectRecord<R>> extends SetSchemaMetadata<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected R updatedRecord(R record, Schema updatedSchema) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, record.key(), record.valueSchema(), record.value(), record.timestamp());
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
        protected R updatedRecord(R record, Schema updatedSchema) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, record.value(), record.timestamp());
        }
    }

}