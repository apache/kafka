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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ParseStructByRegex<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Generate key/value Struct objects supported by ordered Regex Group"
        + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
        + "or value (<code>" + Value.class.getName() + "</code>).";

    private interface ConfigName {
        String REGEX = "regex";
        String MAPPING_KEY = "mapping";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(ConfigName.REGEX, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new GroupRegexValidator(), ConfigDef.Importance.MEDIUM,
            "String Regex Group Pattern.")
        .define(ConfigName.MAPPING_KEY, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM,
            "Ordered Regex Group Mapping Keys");

    private static final String PURPOSE = "Parse Struct by regex group mapping";

    private String pattern;
    private List<String> fieldKeys;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> configs) {
        final AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs, false);
        pattern = config.getString(ConfigName.REGEX);
        fieldKeys = config.getList(ConfigName.MAPPING_KEY);

        if (fieldKeys.isEmpty()) {
            throw new ConfigException("pattern must not be empty");
        }
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        Map<String, Object> resultMap;

        String inputTargetStr;
        if (operatingValue(record) instanceof String) {
            inputTargetStr = (String) operatingValue(record);
            resultMap = parseRegexGroupToStringMap(inputTargetStr);
        } else {
            throw new DataException("only plain string value support with ParseStructByRegex");
        }

        return newRecord(record, null, resultMap);
    }

    private R applyWithSchema(R record) {
        Schema updatedSchema;
        Struct updatedStruct;

        String inputTargetStr;
        if (operatingValue(record) instanceof String) {
            inputTargetStr = (String) operatingValue(record);
            Map<String, Object> newEntryMap = parseRegexGroupToStringMap(inputTargetStr);
            updatedSchema = newSchema(newEntryMap);
            updatedStruct = newStruct(newEntryMap, updatedSchema);
        } else {
            throw new DataException("only plain string value support with ParseStructByRegex");
        }

        return newRecord(record, updatedSchema, updatedStruct);
    }

    private Map<String, Object> parseRegexGroupToStringMap(String inputTargetStr) {
        Map<String, Object> newEntryMap = new HashMap<>();

        if (inputTargetStr == null) {
            return newEntryMap;
        }

        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(inputTargetStr);
        if (m.find()) {
            if (m.groupCount() != fieldKeys.size()) {
                throw new DataException("Unexpected different count data between mapping names and regex group : " + inputTargetStr);
            }
            for (int i = 0; i < m.groupCount(); i++) {
                String value = m.group(i + 1);
                newEntryMap.put(fieldKeys.get(i), value);
            }
        } else {
            throw new DataException("Unexpected no match data with regex : " + inputTargetStr);
        }
        return newEntryMap;
    }

    private Struct newStruct(Map<String, Object> newEntryMap, Schema updatedSchema) {
        final Struct updatedValue = new Struct(updatedSchema);
        for (Map.Entry<String, Object> entry : newEntryMap.entrySet()) {
            updatedValue.put(entry.getKey(), entry.getValue());
        }

        return updatedValue;
    }

    private Schema newSchema(Map<String, Object> newEntryMap) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        for (Map.Entry<String, Object> entry : newEntryMap.entrySet()) {
            builder.field(entry.getKey(), Schema.STRING_SCHEMA);
        }
        Schema schema = builder.build();
        schemaUpdateCache.put(schema, schema);
        return schema;
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

    public static class Key<R extends ConnectRecord<R>> extends ParseStructByRegex<R> {

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

    public static class Value<R extends ConnectRecord<R>> extends ParseStructByRegex<R> {

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

    private static class GroupRegexValidator implements ConfigDef.Validator {

        @Override
        public void ensureValid(String name, Object value) {
            try {
                Pattern p = Pattern.compile((String) value);
                if (p.matcher("dummy").groupCount() < 1) {
                    throw new ConfigException(name, value, "Regex contain at least one group syntax Required  ex) ^a(.*)c");
                }

            } catch (Exception e) {
                throw new ConfigException(name, value, "Invalid Regex");
            }
        }
    }
}
