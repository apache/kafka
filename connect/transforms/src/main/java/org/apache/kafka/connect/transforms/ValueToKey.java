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

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class ValueToKey<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public static final String OVERVIEW_DOC = "Replace the record key with a new key formed from a subset of fields in the record value.";

    public static final String FIELDS_CONFIG = "fields";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH,
                    "Field names on the record value to extract as the record key.");

    private static final String PURPOSE = "copying fields from value to key";

    private List<String> fields;

    private Cache<Schema, Schema> valueToKeySchemaCache;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fields = config.getList(FIELDS_CONFIG);
        valueToKeySchemaCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(record.value(), PURPOSE);
        final Map<String, Object> key = new HashMap<>(fields.size());
        for (String field : fields) {
            key.put(field, value.get(field));
        }
        return record.newRecord(record.topic(), record.kafkaPartition(), null, key, record.valueSchema(), record.value(), record.timestamp());
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);

        Schema keySchema = valueToKeySchemaCache.get(value.schema());
        if (keySchema == null) {
            final SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
            for (String field : fields) {
                final Field fieldFromValue = value.schema().field(field);
                if (fieldFromValue == null) {
                    throw new DataException("Field does not exist: " + field);
                }
                keySchemaBuilder.field(field, fieldFromValue.schema());
            }
            keySchema = keySchemaBuilder.build();
            valueToKeySchemaCache.put(value.schema(), keySchema);
        }

        final Struct key = new Struct(keySchema);
        for (String field : fields) {
            key.put(field, value.get(field));
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), keySchema, key, value.schema(), value, record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        valueToKeySchemaCache = null;
    }

}
