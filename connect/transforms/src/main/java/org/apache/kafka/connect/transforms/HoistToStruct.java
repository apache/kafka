/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUpdateCache;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

abstract class HoistToStruct<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("field", ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM,
                    "Field name for the single field that will be created in the resulting Struct.");

    private static final class SchemaUpdateCacheEntry {
        final Schema base;
        final Schema updated;

        private SchemaUpdateCacheEntry(Schema base, Schema updated) {
            this.base = base;
            this.updated = updated;
        }
    }

    private final SchemaUpdateCache schemaUpdateCache = new SchemaUpdateCache();

    private String fieldName;

    @Override
    public void init(Map<String, Object> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString("field");

        schemaUpdateCache.init();
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        final Object value = operatingValue(record);

        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            updatedSchema = SchemaBuilder.struct().field(fieldName, schema).build();
            schemaUpdateCache.put(schema, updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema).put(fieldName, value);

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public void close() {
        schemaUpdateCache.close();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

}
