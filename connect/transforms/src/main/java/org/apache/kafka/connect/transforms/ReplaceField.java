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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ReplaceField<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public static final String OVERVIEW_DOC = "Filter or rename fields."
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
            + "or value (<code>" + Value.class.getName() + "</code>).";

    interface ConfigName {
        String EXCLUDE = "exclude";
        String INCLUDE = "include";

        String RENAMES = "renames";
        String REPLACE_NULL_WITH_DEFAULT_CONFIG = "replace.null.with.default";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.EXCLUDE, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM,
                    "Fields to exclude. This takes precedence over the fields to include.")
            .define(ConfigName.INCLUDE, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM,
                    "Fields to include. If specified, only these fields will be used.")
            .define(ConfigName.RENAMES, ConfigDef.Type.LIST, Collections.emptyList(),
                ConfigDef.LambdaValidator.with(
                    (name, value) -> {
                        @SuppressWarnings("unchecked")
                        List<String> valueList = (List<String>) value;
                        parseRenameMappings(valueList);
                    },
                    () -> "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>"),
                ConfigDef.Importance.MEDIUM, "Field rename mappings.")
            .define(ConfigName.REPLACE_NULL_WITH_DEFAULT_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                    "Whether to replace fields that have a default value and that are null to the default value. When set to true, the default value is used, otherwise null is used.");

    private static final String PURPOSE = "field replacement";

    private Set<String> exclude;
    private Set<String> include;
    private Map<String, String> renames;
    private Map<String, String> reverseRenames;
    private boolean replaceNullWithDefault;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);

        exclude = new HashSet<>(config.getList(ConfigName.EXCLUDE));
        include = new HashSet<>(config.getList(ConfigName.INCLUDE));
        renames = parseRenameMappings(config.getList(ConfigName.RENAMES));
        reverseRenames = invert(renames);
        replaceNullWithDefault = config.getBoolean(ConfigName.REPLACE_NULL_WITH_DEFAULT_CONFIG);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    static Map<String, String> parseRenameMappings(List<String> mappings) {
        final Map<String, String> m = new HashMap<>();
        for (String mapping : mappings) {
            final String[] parts = mapping.split(":");
            if (parts.length != 2) {
                throw new ConfigException(ConfigName.RENAMES, mappings, "Invalid rename mapping: " + mapping);
            }
            m.put(parts[0], parts[1]);
        }
        return m;
    }

    static Map<String, String> invert(Map<String, String> source) {
        final Map<String, String> m = new HashMap<>();
        for (Map.Entry<String, String> e : source.entrySet()) {
            m.put(e.getValue(), e.getKey());
        }
        return m;
    }

    boolean filter(String fieldName) {
        return !exclude.contains(fieldName) && (include.isEmpty() || include.contains(fieldName));
    }

    String renamed(String fieldName) {
        final String mapping = renames.get(fieldName);
        return mapping == null ? fieldName : mapping;
    }

    String reverseRenamed(String fieldName) {
        final String mapping = reverseRenames.get(fieldName);
        return mapping == null ? fieldName : mapping;
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value.size());

        for (Map.Entry<String, Object> e : value.entrySet()) {
            final String fieldName = e.getKey();
            if (filter(fieldName)) {
                final Object fieldValue = e.getValue();
                updatedValue.put(renamed(fieldName), fieldValue);
            }
        }

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : updatedSchema.fields()) {
            final Object fieldValue = replaceNullWithDefault ? value.get(reverseRenamed(field.name())) : value.getWithoutDefault(reverseRenamed(field.name()));
            updatedValue.put(field.name(), fieldValue);
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            if (filter(field.name())) {
                builder.field(renamed(field.name()), field.schema());
            }
        }
        return builder.build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ReplaceField<R> {

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

    public static class Value<R extends ConnectRecord<R>> extends ReplaceField<R> {

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
