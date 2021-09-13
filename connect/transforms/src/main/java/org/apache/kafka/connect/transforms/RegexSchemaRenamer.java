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


import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegexSchemaRenamer<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(RegexSchemaRenamer.class);

    public static final String OVERVIEW_DOC =
                    "<p/>regex schema renamer</p>";

    public static final String REGEX_CONFIG = "regex";
    public static final String REPLACEMENT_CONFIG = "replacement";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(REGEX_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public void ensureValid(String name, Object valueObject) {
                        String value = (String) valueObject;
                        if (value == null || value.isEmpty()) {
                            throw new ConfigException("Must specify regex to match test.");
                        }
                        parseRegex(value);
                    }

                    @Override
                    public String toString() {
                        return "Regex for schema matching e.g. '.*\\.([^.]*)\\.(Value|Key)'";
                    }
            },
            ConfigDef.Importance.HIGH, "Replacement")
            .define(REPLACEMENT_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
                @SuppressWarnings("unchecked")
                @Override
                public void ensureValid(String name, Object valueObject) {
                    String value = (String) valueObject;
                    if (value == null || value.isEmpty()) {
                        throw new ConfigException("Must specify replacement e.g. 'com.company.schema.$1.$2'");
                    }
                    parseRegex(value);
                }

                @Override
                public String toString() {
                    return "Replacement string";
                }
            },
        ConfigDef.Importance.HIGH, "Regex to match against schema");

    private static final String PURPOSE = "rename schemas";

    private Pattern regex;
    private String replacement;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        regex = parseRegex(config.getString(REGEX_CONFIG));
        replacement = config.getString(REPLACEMENT_CONFIG);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (!hasSchema(record)) {
            return record;
        }

        return applyWithSchema(record);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }
    
    boolean hasSchema(R record) {
        Schema key = record.keySchema();
        Schema value = record.valueSchema();
        return (key != null && Type.STRUCT.equals(key.type()))  || (value != null && Type.STRUCT.equals(value.type()));
    }


    private R applyWithSchema(R record) {
        RenamedSchema renamedKeySchema = renameSchema(record.keySchema());
        RenamedSchema renamedValueSchema = renameSchema(record.valueSchema());
        if (renamedKeySchema.isRenamed || renamedValueSchema.isRenamed) { 
            Object key = updateSchemaIn(record.key(), renamedKeySchema.schema);
            Object value = updateSchemaIn(record.value(), renamedValueSchema.schema);
            R renamedRecord = record.newRecord(record.topic(), record.kafkaPartition(), renamedKeySchema.schema, key, renamedValueSchema.schema, value, record.timestamp());
            return renamedRecord;
        }
        return record;
    }
    
    static Object updateSchemaIn(Object keyOrValue, Schema updatedSchema) {
        if (keyOrValue instanceof Struct) {
            Struct origStruct = (Struct) keyOrValue;
            Struct newStruct = new Struct(updatedSchema);
            for (Field field : updatedSchema.fields()) {
                newStruct.put(field, origStruct.get(field));
            }
            return newStruct;
        }
        return keyOrValue;
    }
    
    RenamedSchema renameSchema(Schema schema) {
        String newName = newSchemaName(schema);
        if (newName != null) {
            Schema renamed = schemaUpdateCache.get(schema);
            if (renamed == null) {
                renamed = renameSchema(schema, newName);
                schemaUpdateCache.put(schema, renamed);
            }

            return new RenamedSchema(true, renamed);
        }
        return new RenamedSchema(false, schema);
    }
    
    String newSchemaName(Schema schema) {
        if (schema == null)
            return null;
        String name = schema.name(); 
        if (name == null)
            return null;
        if (regex.matcher(name).find())
            return regex.matcher(name).replaceFirst(replacement);
        return name;
    }
    
    Schema renameSchema(Schema schema, String newName) {
        if (schema == null) {
            return schema;
        }
        SchemaBuilder builder = new SchemaBuilder(schema.type());
        builder.name(newName);
        builder.version(schema.version());
        builder.doc(schema.doc());
        for (Field f: schema.fields()) {
            builder.field(f.name(), f.schema());
        }

        final Map<String, String> params = schema.parameters();
        if (params != null) {
            builder.parameters(params);
        }
        
        
        return builder.build();
    }

    private static Pattern parseRegex(String regex) {
        try {
            return Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new ConfigException("Invalid regex expression", e);
        }
    }    
    
    public class RenamedSchema {
        public final Boolean isRenamed;
        public final Schema schema;

        public RenamedSchema(Boolean isRenamed, Schema schema) {
            this.isRenamed = isRenamed;
            this.schema = schema;
        }
    }

}
