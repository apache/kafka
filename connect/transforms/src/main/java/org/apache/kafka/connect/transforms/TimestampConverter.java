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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

public abstract class TimestampConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Convert timestamps between different formats such as Unix epoch, strings, and Connect Date/Timestamp types."
                    + "Applies to individual fields or to the entire value."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + TimestampConverter.Key.class.getName() + "</code>) "
                    + "or value (<code>" + TimestampConverter.Value.class.getName() + "</code>).";

    public static final String FIELD_CONFIG = "field";
    private static final String FIELD_DEFAULT = "";

    public static final String TARGET_TYPE_CONFIG = "target.type";

    public static final String FORMAT_CONFIG = "format";
    private static final String FORMAT_DEFAULT = "";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, FIELD_DEFAULT, ConfigDef.Importance.HIGH,
                    "The field containing the timestamp, or empty if the entire value is a timestamp")
            .define(TARGET_TYPE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "The desired timestamp representation: string, unix, Date, Time, or Timestamp")
            .define(FORMAT_CONFIG, ConfigDef.Type.STRING, FORMAT_DEFAULT, ConfigDef.Importance.MEDIUM,
                    "A SimpleDateFormat-compatible format for the timestamp. Used to generate the output when type=string "
                            + "or used to parse the input if the input is a string.");

    private static final String PURPOSE = "converting timestamp formats";

    private static final String TYPE_STRING = "string";
    private static final String TYPE_UNIX = "unix";
    private static final String TYPE_DATE = "Date";
    private static final String TYPE_TIME = "Time";
    private static final String TYPE_TIMESTAMP = "Timestamp";
    private static final Set<String> VALID_TYPES = new HashSet<>(Arrays.asList(TYPE_STRING, TYPE_UNIX, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP));

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    public static final Schema OPTIONAL_DATE_SCHEMA = org.apache.kafka.connect.data.Date.builder().optional().schema();
    public static final Schema OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().schema();
    public static final Schema OPTIONAL_TIME_SCHEMA = Time.builder().optional().schema();

    private interface TimestampTranslator {
        /**
         * Convert from the type-specific format to the universal java.util.Date format
         */
        Date toRaw(Config config, Object orig);

        /**
         * Get the schema for this format.
         */
        Schema typeSchema(boolean isOptional);

        /**
         * Convert from the universal java.util.Date format to the type-specific format
         */
        Object toType(Config config, Date orig);
    }

    private static final Map<String, TimestampTranslator> TRANSLATORS = new HashMap<>();
    static {
        TRANSLATORS.put(TYPE_STRING, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof String))
                    throw new DataException("Expected string timestamp to be a String, but found " + orig.getClass());
                try {
                    return config.format.parse((String) orig);
                } catch (ParseException e) {
                    throw new DataException("Could not parse timestamp: value (" + orig + ") does not match pattern ("
                            + config.format.toPattern() + ")", e);
                }
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
            }

            @Override
            public String toType(Config config, Date orig) {
                synchronized (config.format) {
                    return config.format.format(orig);
                }
            }
        });

        TRANSLATORS.put(TYPE_UNIX, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Long))
                    throw new DataException("Expected Unix timestamp to be a Long, but found " + orig.getClass());
                return Timestamp.toLogical(Timestamp.SCHEMA, (Long) orig);
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
            }

            @Override
            public Long toType(Config config, Date orig) {
                return Timestamp.fromLogical(Timestamp.SCHEMA, orig);
            }
        });

        TRANSLATORS.put(TYPE_DATE, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Date))
                    throw new DataException("Expected Date to be a java.util.Date, but found " + orig.getClass());
                // Already represented as a java.util.Date and Connect Dates are a subset of valid java.util.Date values
                return (Date) orig;
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? OPTIONAL_DATE_SCHEMA : org.apache.kafka.connect.data.Date.SCHEMA;
            }

            @Override
            public Date toType(Config config, Date orig) {
                Calendar result = Calendar.getInstance(UTC);
                result.setTime(orig);
                result.set(Calendar.HOUR_OF_DAY, 0);
                result.set(Calendar.MINUTE, 0);
                result.set(Calendar.SECOND, 0);
                result.set(Calendar.MILLISECOND, 0);
                return result.getTime();
            }
        });

        TRANSLATORS.put(TYPE_TIME, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Date))
                    throw new DataException("Expected Time to be a java.util.Date, but found " + orig.getClass());
                // Already represented as a java.util.Date and Connect Times are a subset of valid java.util.Date values
                return (Date) orig;
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? OPTIONAL_TIME_SCHEMA : Time.SCHEMA;
            }

            @Override
            public Date toType(Config config, Date orig) {
                Calendar origCalendar = Calendar.getInstance(UTC);
                origCalendar.setTime(orig);
                Calendar result = Calendar.getInstance(UTC);
                result.setTimeInMillis(0L);
                result.set(Calendar.HOUR_OF_DAY, origCalendar.get(Calendar.HOUR_OF_DAY));
                result.set(Calendar.MINUTE, origCalendar.get(Calendar.MINUTE));
                result.set(Calendar.SECOND, origCalendar.get(Calendar.SECOND));
                result.set(Calendar.MILLISECOND, origCalendar.get(Calendar.MILLISECOND));
                return result.getTime();
            }
        });

        TRANSLATORS.put(TYPE_TIMESTAMP, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Date))
                    throw new DataException("Expected Timestamp to be a java.util.Date, but found " + orig.getClass());
                return (Date) orig;
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? OPTIONAL_TIMESTAMP_SCHEMA : Timestamp.SCHEMA;
            }

            @Override
            public Date toType(Config config, Date orig) {
                return orig;
            }
        });
    }

    // This is a bit unusual, but allows the transformation config to be passed to static anonymous classes to customize
    // their behavior
    private static class Config {
        Config(String field, String type, SimpleDateFormat format) {
            this.field = field;
            this.type = type;
            this.format = format;
        }
        String field;
        String type;
        SimpleDateFormat format;
    }
    private Config config;
    private Cache<Schema, Schema> schemaUpdateCache;


    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig simpleConfig = new SimpleConfig(CONFIG_DEF, configs);
        final String field = simpleConfig.getString(FIELD_CONFIG);
        final String type = simpleConfig.getString(TARGET_TYPE_CONFIG);
        String formatPattern = simpleConfig.getString(FORMAT_CONFIG);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));

        if (!VALID_TYPES.contains(type)) {
            throw new ConfigException("Unknown timestamp type in TimestampConverter: " + type + ". Valid values are "
                    + Utils.join(VALID_TYPES, ", ") + ".");
        }
        if (type.equals(TYPE_STRING) && Utils.isBlank(formatPattern)) {
            throw new ConfigException("TimestampConverter requires format option to be specified when using string timestamps");
        }
        SimpleDateFormat format = null;
        if (!Utils.isBlank(formatPattern)) {
            try {
                format = new SimpleDateFormat(formatPattern);
                format.setTimeZone(UTC);
            } catch (IllegalArgumentException e) {
                throw new ConfigException("TimestampConverter requires a SimpleDateFormat-compatible pattern for string timestamps: "
                        + formatPattern, e);
            }
        }
        config = new Config(field, type, format);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    public static class Key<R extends ConnectRecord<R>> extends TimestampConverter<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends TimestampConverter<R> {
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

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    private R applyWithSchema(R record) {
        final Schema schema = operatingSchema(record);
        if (config.field.isEmpty()) {
            Object value = operatingValue(record);
            // New schema is determined by the requested target timestamp type
            Schema updatedSchema = TRANSLATORS.get(config.type).typeSchema(schema.isOptional());
            return newRecord(record, updatedSchema, convertTimestamp(value, timestampTypeFromSchema(schema)));
        } else {
            final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
            Schema updatedSchema = schemaUpdateCache.get(schema);
            if (updatedSchema == null) {
                SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
                for (Field field : schema.fields()) {
                    if (field.name().equals(config.field)) {
                        builder.field(field.name(), TRANSLATORS.get(config.type).typeSchema(field.schema().isOptional()));
                    } else {
                        builder.field(field.name(), field.schema());
                    }
                }
                if (schema.isOptional())
                    builder.optional();
                if (schema.defaultValue() != null) {
                    Struct updatedDefaultValue = applyValueWithSchema((Struct) schema.defaultValue(), builder);
                    builder.defaultValue(updatedDefaultValue);
                }

                updatedSchema = builder.build();
                schemaUpdateCache.put(schema, updatedSchema);
            }

            Struct updatedValue = applyValueWithSchema(value, updatedSchema);
            return newRecord(record, updatedSchema, updatedValue);
        }
    }

    private Struct applyValueWithSchema(Struct value, Schema updatedSchema) {
        if (value == null) {
            return null;
        }
        Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object updatedFieldValue;
            if (field.name().equals(config.field)) {
                updatedFieldValue = convertTimestamp(value.get(field), timestampTypeFromSchema(field.schema()));
            } else {
                updatedFieldValue = value.get(field);
            }
            updatedValue.put(field.name(), updatedFieldValue);
        }
        return updatedValue;
    }

    private R applySchemaless(R record) {
        Object rawValue = operatingValue(record);
        if (rawValue == null || config.field.isEmpty()) {
            return newRecord(record, null, convertTimestamp(rawValue));
        } else {
            final Map<String, Object> value = requireMap(rawValue, PURPOSE);
            final HashMap<String, Object> updatedValue = new HashMap<>(value);
            updatedValue.put(config.field, convertTimestamp(value.get(config.field)));
            return newRecord(record, null, updatedValue);
        }
    }

    /**
     * Determine the type/format of the timestamp based on the schema
     */
    private String timestampTypeFromSchema(Schema schema) {
        if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
            return TYPE_TIMESTAMP;
        } else if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())) {
            return TYPE_DATE;
        } else if (Time.LOGICAL_NAME.equals(schema.name())) {
            return TYPE_TIME;
        } else if (schema.type().equals(Schema.Type.STRING)) {
            // If not otherwise specified, string == user-specified string format for timestamps
            return TYPE_STRING;
        } else if (schema.type().equals(Schema.Type.INT64)) {
            // If not otherwise specified, long == unix time
            return TYPE_UNIX;
        }
        throw new ConnectException("Schema " + schema + " does not correspond to a known timestamp type format");
    }

    /**
     * Infer the type/format of the timestamp based on the raw Java type
     */
    private String inferTimestampType(Object timestamp) {
        // Note that we can't infer all types, e.g. Date/Time/Timestamp all have the same runtime representation as a
        // java.util.Date
        if (timestamp instanceof Date) {
            return TYPE_TIMESTAMP;
        } else if (timestamp instanceof Long) {
            return TYPE_UNIX;
        } else if (timestamp instanceof String) {
            return TYPE_STRING;
        }
        throw new DataException("TimestampConverter does not support " + timestamp.getClass() + " objects as timestamps");
    }

    /**
     * Convert the given timestamp to the target timestamp format.
     * @param timestamp the input timestamp, may be null
     * @param timestampFormat the format of the timestamp, or null if the format should be inferred
     * @return the converted timestamp
     */
    private Object convertTimestamp(Object timestamp, String timestampFormat) {
        if (timestamp == null) {
            return null;
        }
        if (timestampFormat == null) {
            timestampFormat = inferTimestampType(timestamp);
        }

        TimestampTranslator sourceTranslator = TRANSLATORS.get(timestampFormat);
        if (sourceTranslator == null) {
            throw new ConnectException("Unsupported timestamp type: " + timestampFormat);
        }
        Date rawTimestamp = sourceTranslator.toRaw(config, timestamp);

        TimestampTranslator targetTranslator = TRANSLATORS.get(config.type);
        if (targetTranslator == null) {
            throw new ConnectException("Unsupported timestamp type: " + config.type);
        }
        return targetTranslator.toType(config, rawTimestamp);
    }

    private Object convertTimestamp(Object timestamp) {
        return convertTimestamp(timestamp, null);
    }
}
