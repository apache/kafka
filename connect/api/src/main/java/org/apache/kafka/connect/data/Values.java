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
package org.apache.kafka.connect.data;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.CharacterIterator;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * Utility for converting from one Connect value to a different form. This is useful when the caller expects a value of a particular type
 * but is uncertain whether the actual value is one that isn't directly that type but can be converted into that type.
 *
 * <p>For example, a caller might expects a particular {@link org.apache.kafka.connect.header.Header} to contain an {@link Type#INT64}
 * value, when in fact that header contains a string representation of a 32-bit integer. Here, the caller can use the methods in this
 * class to convert the value to the desired type:
 * <pre>
 *     Header header = ...
 *     long value = Values.convertToLong(header.schema(), header.value());
 * </pre>
 *
 * <p>This class is able to convert any value to a string representation as well as parse those string representations back into most of
 * the types. The only exception is {@link Struct} values that require a schema and thus cannot be parsed from a simple string.
 */
public class Values {

    private static final Logger LOG = LoggerFactory.getLogger(Values.class);

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final SchemaAndValue NULL_SCHEMA_AND_VALUE = new SchemaAndValue(null, null);
    private static final SchemaAndValue TRUE_SCHEMA_AND_VALUE = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, Boolean.TRUE);
    private static final SchemaAndValue FALSE_SCHEMA_AND_VALUE = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, Boolean.FALSE);
    private static final Schema ARRAY_SELECTOR_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
    private static final Schema MAP_SELECTOR_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
    private static final Schema STRUCT_SELECTOR_SCHEMA = SchemaBuilder.struct().build();
    private static final String TRUE_LITERAL = Boolean.TRUE.toString();
    private static final String FALSE_LITERAL = Boolean.FALSE.toString();
    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;
    private static final String NULL_VALUE = "null";
    static final String ISO_8601_DATE_FORMAT_PATTERN = "yyyy-MM-dd";
    static final String ISO_8601_TIME_FORMAT_PATTERN = "HH:mm:ss.SSS'Z'";
    static final String ISO_8601_TIMESTAMP_FORMAT_PATTERN = ISO_8601_DATE_FORMAT_PATTERN + "'T'" + ISO_8601_TIME_FORMAT_PATTERN;
    private static final Set<String> TEMPORAL_LOGICAL_TYPE_NAMES =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(Time.LOGICAL_NAME,
                            Timestamp.LOGICAL_NAME,
                            Date.LOGICAL_NAME
                            )
                    )
            );

    private static final String QUOTE_DELIMITER = "\"";
    private static final String COMMA_DELIMITER = ",";
    private static final String ENTRY_DELIMITER = ":";
    private static final String ARRAY_BEGIN_DELIMITER = "[";
    private static final String ARRAY_END_DELIMITER = "]";
    private static final String MAP_BEGIN_DELIMITER = "{";
    private static final String MAP_END_DELIMITER = "}";
    private static final int ISO_8601_DATE_LENGTH = ISO_8601_DATE_FORMAT_PATTERN.length();
    private static final int ISO_8601_TIME_LENGTH = ISO_8601_TIME_FORMAT_PATTERN.length() - 2; // subtract single quotes
    private static final int ISO_8601_TIMESTAMP_LENGTH = ISO_8601_TIMESTAMP_FORMAT_PATTERN.length() - 4; // subtract single quotes

    private static final Pattern TWO_BACKSLASHES = Pattern.compile("\\\\");

    private static final Pattern DOUBLEQOUTE = Pattern.compile("\"");

    /**
     * Convert the specified value to an {@link Type#BOOLEAN} value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a boolean.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a boolean, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a boolean
     */
    public static Boolean convertToBoolean(Schema schema, Object value) throws DataException {
        return (Boolean) convertTo(Schema.OPTIONAL_BOOLEAN_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Type#INT8} byte value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a byte.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a byte, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a byte
     */
    public static Byte convertToByte(Schema schema, Object value) throws DataException {
        return (Byte) convertTo(Schema.OPTIONAL_INT8_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Type#INT16} short value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a short.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a short, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a short
     */
    public static Short convertToShort(Schema schema, Object value) throws DataException {
        return (Short) convertTo(Schema.OPTIONAL_INT16_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Type#INT32} int value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to an integer.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as an integer, or null if the supplied value was null
     * @throws DataException if the value could not be converted to an integer
     */
    public static Integer convertToInteger(Schema schema, Object value) throws DataException {
        return (Integer) convertTo(Schema.OPTIONAL_INT32_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Type#INT64} long value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a long.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a long, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a long
     */
    public static Long convertToLong(Schema schema, Object value) throws DataException {
        return (Long) convertTo(Schema.OPTIONAL_INT64_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Type#FLOAT32} float value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a floating point number.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a float, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a float
     */
    public static Float convertToFloat(Schema schema, Object value) throws DataException {
        return (Float) convertTo(Schema.OPTIONAL_FLOAT32_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Type#FLOAT64} double value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a floating point number.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a double, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a double
     */
    public static Double convertToDouble(Schema schema, Object value) throws DataException {
        return (Double) convertTo(Schema.OPTIONAL_FLOAT64_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Type#STRING} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a string, or null if the supplied value was null
     */
    public static String convertToString(Schema schema, Object value) {
        return (String) convertTo(Schema.OPTIONAL_STRING_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Type#ARRAY} value. If the value is a string representation of an array, this method
     * will parse the string and its elements to infer the schemas for those elements. Thus, this method supports
     * arrays of other primitives and structured types. If the value is already an array (or list), this method simply casts and
     * returns it.
     *
     * <p>This method currently does not use the schema, though it may be used in the future.</p>
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a list, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a list value
     */
    public static List<?> convertToList(Schema schema, Object value) {
        return (List<?>) convertTo(ARRAY_SELECTOR_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Type#MAP} value. If the value is a string representation of a map, this method
     * will parse the string and its entries to infer the schemas for those entries. Thus, this method supports
     * maps with primitives and structured keys and values. If the value is already a map, this method simply casts and returns it.
     *
     * <p>This method currently does not use the schema, though it may be used in the future.</p>
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a map, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a map value
     */
    public static Map<?, ?> convertToMap(Schema schema, Object value) {
        return (Map<?, ?>) convertTo(MAP_SELECTOR_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Type#STRUCT} value. Structs cannot be converted from other types, so this method returns
     * a struct only if the supplied value is a struct. If not a struct, this method throws an exception.
     *
     * <p>This method currently does not use the schema, though it may be used in the future.</p>
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a struct, or null if the supplied value was null
     * @throws DataException if the value is not a struct
     */
    public static Struct convertToStruct(Schema schema, Object value) {
        return (Struct) convertTo(STRUCT_SELECTOR_SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Time#SCHEMA time} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a time, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a time value
     */
    public static java.util.Date convertToTime(Schema schema, Object value) {
        return (java.util.Date) convertTo(Time.SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Date#SCHEMA date} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a date, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a date value
     */
    public static java.util.Date convertToDate(Schema schema, Object value) {
        return (java.util.Date) convertTo(Date.SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Timestamp#SCHEMA timestamp} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a timestamp, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a timestamp value
     */
    public static java.util.Date convertToTimestamp(Schema schema, Object value) {
        return (java.util.Date) convertTo(Timestamp.SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to an {@link Decimal decimal} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a decimal, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a decimal value
     */
    public static BigDecimal convertToDecimal(Schema schema, Object value, int scale) {
        return (BigDecimal) convertTo(Decimal.schema(scale), schema, value);
    }

    /**
     * If possible infer a schema for the given value.
     *
     * @param value the value whose schema is to be inferred; may be null
     * @return the inferred schema, or null if the value is null or no schema could be inferred
     */
    public static Schema inferSchema(Object value) {
        if (value instanceof String) {
            return Schema.STRING_SCHEMA;
        }
        if (value instanceof Boolean) {
            return Schema.BOOLEAN_SCHEMA;
        }
        if (value instanceof Byte) {
            return Schema.INT8_SCHEMA;
        }
        if (value instanceof Short) {
            return Schema.INT16_SCHEMA;
        }
        if (value instanceof Integer) {
            return Schema.INT32_SCHEMA;
        }
        if (value instanceof Long) {
            return Schema.INT64_SCHEMA;
        }
        if (value instanceof Float) {
            return Schema.FLOAT32_SCHEMA;
        }
        if (value instanceof Double) {
            return Schema.FLOAT64_SCHEMA;
        }
        if (value instanceof byte[] || value instanceof ByteBuffer) {
            return Schema.BYTES_SCHEMA;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            if (list.isEmpty()) {
                return null;
            }
            SchemaDetector detector = new SchemaDetector();
            for (Object element : list) {
                if (!detector.canDetect(element)) {
                    return null;
                }
            }
            return SchemaBuilder.array(detector.schema()).build();
        }
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            if (map.isEmpty()) {
                return null;
            }
            SchemaDetector keyDetector = new SchemaDetector();
            SchemaDetector valueDetector = new SchemaDetector();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!keyDetector.canDetect(entry.getKey()) || !valueDetector.canDetect(entry.getValue())) {
                    return null;
                }
            }
            return SchemaBuilder.map(keyDetector.schema(), valueDetector.schema()).build();
        }
        if (value instanceof Struct) {
            return ((Struct) value).schema();
        }
        return null;
    }


    /**
     * Parse the specified string representation of a value into its schema and value.
     *
     * @param value the string form of the value
     * @return the schema and value; never null, but whose schema and value may be null
     * @see #convertToString
     */
    public static SchemaAndValue parseString(String value) {
        if (value == null) {
            return NULL_SCHEMA_AND_VALUE;
        }
        if (value.isEmpty()) {
            return new SchemaAndValue(Schema.STRING_SCHEMA, value);
        }
        Parser parser = new Parser(value);
        return parse(parser, false);
    }

    /**
     * Convert the value to the desired type.
     *
     * @param toSchema   the schema for the desired type; may not be null
     * @param fromSchema the schema for the supplied value; may be null if not known
     * @return the converted value; never null
     * @throws DataException if the value could not be converted to the desired type
     */
    protected static Object convertTo(Schema toSchema, Schema fromSchema, Object value) throws DataException {
        if (value == null) {
            if (toSchema.isOptional()) {
                return null;
            }
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        }
        switch (toSchema.type()) {
            case BYTES:
                if (Decimal.LOGICAL_NAME.equals(toSchema.name())) {
                    if (value instanceof ByteBuffer) {
                        value = Utils.toArray((ByteBuffer) value);
                    }
                    if (value instanceof byte[]) {
                        return Decimal.toLogical(toSchema, (byte[]) value);
                    }
                    if (value instanceof BigDecimal) {
                        return value;
                    }
                    if (value instanceof Number) {
                        // Not already a decimal, so treat it as a double ...
                        double converted = ((Number) value).doubleValue();
                        return BigDecimal.valueOf(converted);
                    }
                    if (value instanceof String) {
                        return new BigDecimal(value.toString()).doubleValue();
                    }
                }
                if (value instanceof ByteBuffer) {
                    return Utils.toArray((ByteBuffer) value);
                }
                if (value instanceof byte[]) {
                    return value;
                }
                if (value instanceof BigDecimal) {
                    return Decimal.fromLogical(toSchema, (BigDecimal) value);
                }
                break;
            case STRING:
                StringBuilder sb = new StringBuilder();
                append(sb, value, false);
                return sb.toString();
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                if (value instanceof String) {
                    SchemaAndValue parsed = parseString(value.toString());
                    if (parsed.value() instanceof Boolean) {
                        return parsed.value();
                    }
                }
                return asLong(value, fromSchema, null) == 0L ? Boolean.FALSE : Boolean.TRUE;
            case INT8:
                if (value instanceof Byte) {
                    return value;
                }
                return (byte) asLong(value, fromSchema, null);
            case INT16:
                if (value instanceof Short) {
                    return value;
                }
                return (short) asLong(value, fromSchema, null);
            case INT32:
                if (Date.LOGICAL_NAME.equals(toSchema.name())) {
                    if (value instanceof String) {
                        SchemaAndValue parsed = parseString(value.toString());
                        value = parsed.value();
                    }
                    if (value instanceof java.util.Date) {
                        if (fromSchema != null) {
                            String fromSchemaName = fromSchema.name();
                            if (Date.LOGICAL_NAME.equals(fromSchemaName)) {
                                return value;
                            }
                            if (Timestamp.LOGICAL_NAME.equals(fromSchemaName)) {
                                // Just get the number of days from this timestamp
                                long millis = ((java.util.Date) value).getTime();
                                int days = (int) (millis / MILLIS_PER_DAY); // truncates
                                return Date.toLogical(toSchema, days);
                            }
                        } else {
                            // There is no fromSchema, so no conversion is needed
                            return value;
                        }
                    }
                    long numeric = asLong(value, fromSchema, null);
                    return Date.toLogical(toSchema, (int) numeric);
                }
                if (Time.LOGICAL_NAME.equals(toSchema.name())) {
                    if (value instanceof String) {
                        SchemaAndValue parsed = parseString(value.toString());
                        value = parsed.value();
                    }
                    if (value instanceof java.util.Date) {
                        if (fromSchema != null) {
                            String fromSchemaName = fromSchema.name();
                            if (Time.LOGICAL_NAME.equals(fromSchemaName)) {
                                return value;
                            }
                            if (Timestamp.LOGICAL_NAME.equals(fromSchemaName)) {
                                // Just get the time portion of this timestamp
                                Calendar calendar = Calendar.getInstance(UTC);
                                calendar.setTime((java.util.Date) value);
                                calendar.set(Calendar.YEAR, 1970);
                                calendar.set(Calendar.MONTH, 0); // Months are zero-based
                                calendar.set(Calendar.DAY_OF_MONTH, 1);
                                return Time.toLogical(toSchema, (int) calendar.getTimeInMillis());
                            }
                        } else {
                            // There is no fromSchema, so no conversion is needed
                            return value;
                        }
                    }
                    long numeric = asLong(value, fromSchema, null);
                    return Time.toLogical(toSchema, (int) numeric);
                }
                if (value instanceof Integer) {
                    return value;
                }
                return (int) asLong(value, fromSchema, null);
            case INT64:
                if (Timestamp.LOGICAL_NAME.equals(toSchema.name())) {
                    if (value instanceof String) {
                        SchemaAndValue parsed = parseString(value.toString());
                        value = parsed.value();
                    }
                    if (value instanceof java.util.Date) {
                        java.util.Date date = (java.util.Date) value;
                        if (fromSchema != null) {
                            String fromSchemaName = fromSchema.name();
                            if (Date.LOGICAL_NAME.equals(fromSchemaName)) {
                                int days = Date.fromLogical(fromSchema, date);
                                long millis = days * MILLIS_PER_DAY;
                                return Timestamp.toLogical(toSchema, millis);
                            }
                            if (Time.LOGICAL_NAME.equals(fromSchemaName)) {
                                long millis = Time.fromLogical(fromSchema, date);
                                return Timestamp.toLogical(toSchema, millis);
                            }
                            if (Timestamp.LOGICAL_NAME.equals(fromSchemaName)) {
                                return value;
                            }
                        } else {
                            // There is no fromSchema, so no conversion is needed
                            return value;
                        }
                    }
                    long numeric = asLong(value, fromSchema, null);
                    return Timestamp.toLogical(toSchema, numeric);
                }
                if (value instanceof Long) {
                    return value;
                }
                return asLong(value, fromSchema, null);
            case FLOAT32:
                if (value instanceof Float) {
                    return value;
                }
                return (float) asDouble(value, fromSchema, null);
            case FLOAT64:
                if (value instanceof Double) {
                    return value;
                }
                return asDouble(value, fromSchema, null);
            case ARRAY:
                if (value instanceof String) {
                    SchemaAndValue schemaAndValue = parseString(value.toString());
                    value = schemaAndValue.value();
                }
                if (value instanceof List) {
                    return value;
                }
                break;
            case MAP:
                if (value instanceof String) {
                    SchemaAndValue schemaAndValue = parseString(value.toString());
                    value = schemaAndValue.value();
                }
                if (value instanceof Map) {
                    return value;
                }
                break;
            case STRUCT:
                if (value instanceof Struct) {
                    Struct struct = (Struct) value;
                    return struct;
                }
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
    }

    /**
     * Convert the specified value to the desired scalar value type.
     *
     * @param value      the value to be converted; may not be null
     * @param fromSchema the schema for the current value type; may not be null
     * @param error      any previous error that should be included in an exception message; may be null
     * @return the long value after conversion; never null
     * @throws DataException if the value could not be converted to a long
     */
    protected static long asLong(Object value, Schema fromSchema, Throwable error) {
        try {
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.longValue();
            }
            if (value instanceof String) {
                return new BigDecimal(value.toString()).longValue();
            }
        } catch (NumberFormatException e) {
            error = e;
            // fall through
        }
        if (fromSchema != null) {
            String schemaName = fromSchema.name();
            if (value instanceof java.util.Date) {
                if (Date.LOGICAL_NAME.equals(schemaName)) {
                    return Date.fromLogical(fromSchema, (java.util.Date) value);
                }
                if (Time.LOGICAL_NAME.equals(schemaName)) {
                    return Time.fromLogical(fromSchema, (java.util.Date) value);
                }
                if (Timestamp.LOGICAL_NAME.equals(schemaName)) {
                    return Timestamp.fromLogical(fromSchema, (java.util.Date) value);
                }
            }
            throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + fromSchema, error);
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to a number", error);
    }

    /**
     * Convert the specified value with the desired floating point type.
     *
     * @param value  the value to be converted; may not be null
     * @param schema the schema for the current value type; may not be null
     * @param error  any previous error that should be included in an exception message; may be null
     * @return the double value after conversion; never null
     * @throws DataException if the value could not be converted to a double
     */
    protected static double asDouble(Object value, Schema schema, Throwable error) {
        try {
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.doubleValue();
            }
            if (value instanceof String) {
                return new BigDecimal(value.toString()).doubleValue();
            }
        } catch (NumberFormatException e) {
            error = e;
            // fall through
        }
        return asLong(value, schema, error);
    }

    protected static void append(StringBuilder sb, Object value, boolean embedded) {
        if (value == null) {
            sb.append(NULL_VALUE);
        } else if (value instanceof Number) {
            sb.append(value);
        } else if (value instanceof Boolean) {
            sb.append(value);
        } else if (value instanceof String) {
            if (embedded) {
                String escaped = escape((String) value);
                sb.append('"').append(escaped).append('"');
            } else {
                sb.append(value);
            }
        } else if (value instanceof byte[]) {
            value = Base64.getEncoder().encodeToString((byte[]) value);
            if (embedded) {
                sb.append('"').append(value).append('"');
            } else {
                sb.append(value);
            }
        } else if (value instanceof ByteBuffer) {
            byte[] bytes = Utils.readBytes((ByteBuffer) value);
            append(sb, bytes, embedded);
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            sb.append('[');
            appendIterable(sb, list.iterator());
            sb.append(']');
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            sb.append('{');
            appendIterable(sb, map.entrySet().iterator());
            sb.append('}');
        } else if (value instanceof Struct) {
            Struct struct = (Struct) value;
            Schema schema = struct.schema();
            boolean first = true;
            sb.append('{');
            for (Field field : schema.fields()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(',');
                }
                append(sb, field.name(), true);
                sb.append(':');
                append(sb, struct.get(field), true);
            }
            sb.append('}');
        } else if (value instanceof Map.Entry) {
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) value;
            append(sb, entry.getKey(), true);
            sb.append(':');
            append(sb, entry.getValue(), true);
        } else if (value instanceof java.util.Date) {
            java.util.Date dateValue = (java.util.Date) value;
            String formatted = dateFormatFor(dateValue).format(dateValue);
            sb.append(formatted);
        } else {
            throw new DataException("Failed to serialize unexpected value type " + value.getClass().getName() + ": " + value);
        }
    }

    protected static void appendIterable(StringBuilder sb, Iterator<?> iter) {
        if (iter.hasNext()) {
            append(sb, iter.next(), true);
            while (iter.hasNext()) {
                sb.append(',');
                append(sb, iter.next(), true);
            }
        }
    }

    protected static String escape(String value) {
        String replace1 = TWO_BACKSLASHES.matcher(value).replaceAll("\\\\\\\\");
        return DOUBLEQOUTE.matcher(replace1).replaceAll("\\\\\"");
    }

    public static DateFormat dateFormatFor(java.util.Date value) {
        if (value.getTime() < MILLIS_PER_DAY) {
            return new SimpleDateFormat(ISO_8601_TIME_FORMAT_PATTERN);
        }
        if (value.getTime() % MILLIS_PER_DAY == 0) {
            return new SimpleDateFormat(ISO_8601_DATE_FORMAT_PATTERN);
        }
        return new SimpleDateFormat(ISO_8601_TIMESTAMP_FORMAT_PATTERN);
    }

    protected static boolean canParseSingleTokenLiteral(Parser parser, boolean embedded, String tokenLiteral) {
        int startPosition = parser.mark();
        // If the next token is what we expect, then either...
        if (parser.canConsume(tokenLiteral)) {
            //   ...we're reading an embedded value, in which case the next token will be handled appropriately
            //      by the caller if it's something like an end delimiter for a map or array, or a comma to
            //      separate multiple embedded values...
            //   ...or it's being parsed as part of a top-level string, in which case, any other tokens should
            //      cause use to stop parsing this single-token literal as such and instead just treat it like
            //      a string. For example, the top-level string "true}" will be tokenized as the tokens "true" and
            //      "}", but should ultimately be parsed as just the string "true}" instead of the boolean true.
            if (embedded || !parser.hasNext()) {
                return true;
            }
        }
        parser.rewindTo(startPosition);
        return false;
    }

    protected static SchemaAndValue parse(Parser parser, boolean embedded) throws NoSuchElementException {
        if (!parser.hasNext()) {
            return null;
        }
        if (embedded) {
            if (parser.canConsume(QUOTE_DELIMITER)) {
                StringBuilder sb = new StringBuilder();
                while (parser.hasNext()) {
                    if (parser.canConsume(QUOTE_DELIMITER)) {
                        break;
                    }
                    sb.append(parser.next());
                }
                String content = sb.toString();
                // We can parse string literals as temporal logical types, but all others
                // are treated as strings
                SchemaAndValue parsed = parseString(content);
                if (parsed != null && TEMPORAL_LOGICAL_TYPE_NAMES.contains(parsed.schema().name())) {
                    return parsed;
                }
                return new SchemaAndValue(Schema.STRING_SCHEMA, content);
            }
        }

        if (canParseSingleTokenLiteral(parser, embedded, NULL_VALUE)) {
            return null;
        }
        if (canParseSingleTokenLiteral(parser, embedded, TRUE_LITERAL)) {
            return TRUE_SCHEMA_AND_VALUE;
        }
        if (canParseSingleTokenLiteral(parser, embedded, FALSE_LITERAL)) {
            return FALSE_SCHEMA_AND_VALUE;
        }

        int startPosition = parser.mark();

        try {
            if (parser.canConsume(ARRAY_BEGIN_DELIMITER)) {
                List<Object> result = new ArrayList<>();
                Schema elementSchema = null;
                while (parser.hasNext()) {
                    if (parser.canConsume(ARRAY_END_DELIMITER)) {
                        Schema listSchema;
                        if (elementSchema != null) {
                            listSchema = SchemaBuilder.array(elementSchema).schema();
                            result = alignListEntriesWithSchema(listSchema, result);
                        } else {
                            // Every value is null
                            listSchema = SchemaBuilder.arrayOfNull().build();
                        }
                        return new SchemaAndValue(listSchema, result);
                    }

                    if (parser.canConsume(COMMA_DELIMITER)) {
                        throw new DataException("Unable to parse an empty array element: " + parser.original());
                    }
                    SchemaAndValue element = parse(parser, true);
                    elementSchema = commonSchemaFor(elementSchema, element);
                    result.add(element != null ? element.value() : null);

                    int currentPosition = parser.mark();
                    if (parser.canConsume(ARRAY_END_DELIMITER)) {
                        parser.rewindTo(currentPosition);
                    } else if (!parser.canConsume(COMMA_DELIMITER)) {
                        throw new DataException("Array elements missing '" + COMMA_DELIMITER + "' delimiter");
                    }
                }

                // Missing either a comma or an end delimiter
                if (COMMA_DELIMITER.equals(parser.previous())) {
                    throw new DataException("Array is missing element after ',': " + parser.original());
                }
                throw new DataException("Array is missing terminating ']': " + parser.original());
            }

            if (parser.canConsume(MAP_BEGIN_DELIMITER)) {
                Map<Object, Object> result = new LinkedHashMap<>();
                Schema keySchema = null;
                Schema valueSchema = null;
                while (parser.hasNext()) {
                    if (parser.canConsume(MAP_END_DELIMITER)) {
                        Schema mapSchema;
                        if (keySchema != null && valueSchema != null) {
                            mapSchema = SchemaBuilder.map(keySchema, valueSchema).build();
                            result = alignMapKeysAndValuesWithSchema(mapSchema, result);
                        } else if (keySchema != null) {
                            mapSchema = SchemaBuilder.mapWithNullValues(keySchema);
                            result = alignMapKeysWithSchema(mapSchema, result);
                        } else {
                            mapSchema = SchemaBuilder.mapOfNull().build();
                        }
                        return new SchemaAndValue(mapSchema, result);
                    }

                    if (parser.canConsume(COMMA_DELIMITER)) {
                        throw new DataException("Unable to parse a map entry with no key or value: " + parser.original());
                    }
                    SchemaAndValue key = parse(parser, true);
                    if (key == null || key.value() == null) {
                        throw new DataException("Map entry may not have a null key: " + parser.original());
                    }

                    if (!parser.canConsume(ENTRY_DELIMITER)) {
                        throw new DataException("Map entry is missing '" + ENTRY_DELIMITER
                                                + "' at " + parser.position()
                                                + " in " + parser.original());
                    }
                    SchemaAndValue value = parse(parser, true);
                    Object entryValue = value != null ? value.value() : null;
                    result.put(key.value(), entryValue);

                    parser.canConsume(COMMA_DELIMITER);
                    keySchema = commonSchemaFor(keySchema, key);
                    valueSchema = commonSchemaFor(valueSchema, value);
                }
                // Missing either a comma or an end delimiter
                if (COMMA_DELIMITER.equals(parser.previous())) {
                    throw new DataException("Map is missing element after ',': " + parser.original());
                }
                throw new DataException("Map is missing terminating '}': " + parser.original());
            }
        } catch (DataException e) {
            LOG.trace("Unable to parse the value as a map or an array; reverting to string", e);
            parser.rewindTo(startPosition);
        }

        String token = parser.next();
        if (token.trim().isEmpty()) {
            return new SchemaAndValue(Schema.STRING_SCHEMA, token);
        }
        token = token.trim();

        char firstChar = token.charAt(0);
        boolean firstCharIsDigit = Character.isDigit(firstChar);

        // Temporal types are more restrictive, so try them first
        if (firstCharIsDigit) {
            // The time and timestamp literals may be split into 5 tokens since an unescaped colon
            // is a delimiter. Check these first since the first of these tokens is a simple numeric
            int position = parser.mark();
            String remainder = parser.next(4);
            if (remainder != null) {
                String timeOrTimestampStr = token + remainder;
                SchemaAndValue temporal = parseAsTemporal(timeOrTimestampStr);
                if (temporal != null) {
                    return temporal;
                }
            }
            // No match was found using the 5 tokens, so rewind and see if the current token has a date, time, or timestamp
            parser.rewindTo(position);
            SchemaAndValue temporal = parseAsTemporal(token);
            if (temporal != null) {
                return temporal;
            }
        }
        if (firstCharIsDigit || firstChar == '+' || firstChar == '-') {
            try {
                // Try to parse as a number ...
                BigDecimal decimal = new BigDecimal(token);
                try {
                    return new SchemaAndValue(Schema.INT8_SCHEMA, decimal.byteValueExact());
                } catch (ArithmeticException e) {
                    // continue
                }
                try {
                    return new SchemaAndValue(Schema.INT16_SCHEMA, decimal.shortValueExact());
                } catch (ArithmeticException e) {
                    // continue
                }
                try {
                    return new SchemaAndValue(Schema.INT32_SCHEMA, decimal.intValueExact());
                } catch (ArithmeticException e) {
                    // continue
                }
                try {
                    return new SchemaAndValue(Schema.INT64_SCHEMA, decimal.longValueExact());
                } catch (ArithmeticException e) {
                    // continue
                }
                float fValue = decimal.floatValue();
                if (fValue != Float.NEGATIVE_INFINITY && fValue != Float.POSITIVE_INFINITY
                    && decimal.scale() != 0) {
                    return new SchemaAndValue(Schema.FLOAT32_SCHEMA, fValue);
                }
                double dValue = decimal.doubleValue();
                if (dValue != Double.NEGATIVE_INFINITY && dValue != Double.POSITIVE_INFINITY
                    && decimal.scale() != 0) {
                    return new SchemaAndValue(Schema.FLOAT64_SCHEMA, dValue);
                }
                Schema schema = Decimal.schema(decimal.scale());
                return new SchemaAndValue(schema, decimal);
            } catch (NumberFormatException e) {
                // can't parse as a number
            }
        }
        if (embedded) {
            throw new DataException("Failed to parse embedded value");
        }
        // At this point, the only thing this non-embedded value can be is a string.
        return new SchemaAndValue(Schema.STRING_SCHEMA, parser.original());
    }

    private static SchemaAndValue parseAsTemporal(String token) {
        if (token == null) {
            return null;
        }
        // If the colons were escaped, we'll see the escape chars and need to remove them
        token = token.replace("\\:", ":");
        int tokenLength = token.length();
        if (tokenLength == ISO_8601_TIME_LENGTH) {
            try {
                return new SchemaAndValue(Time.SCHEMA, new SimpleDateFormat(ISO_8601_TIME_FORMAT_PATTERN).parse(token));
            } catch (ParseException e) {
              // not a valid date
            }
        } else if (tokenLength == ISO_8601_TIMESTAMP_LENGTH) {
            try {
                return new SchemaAndValue(Timestamp.SCHEMA, new SimpleDateFormat(ISO_8601_TIMESTAMP_FORMAT_PATTERN).parse(token));
            } catch (ParseException e) {
              // not a valid date
            }
        } else if (tokenLength == ISO_8601_DATE_LENGTH) {
            try {
                return new SchemaAndValue(Date.SCHEMA, new SimpleDateFormat(ISO_8601_DATE_FORMAT_PATTERN).parse(token));
            } catch (ParseException e) {
                // not a valid date
            }
        }
        return null;
    }

    protected static Schema commonSchemaFor(Schema previous, SchemaAndValue latest) {
        if (latest == null) {
            return previous;
        }
        if (previous == null) {
            return latest.schema();
        }
        Schema newSchema = latest.schema();
        Type previousType = previous.type();
        Type newType = newSchema.type();
        if (previousType != newType) {
            switch (previous.type()) {
                case INT8:
                    if (newType == Type.INT16 || newType == Type.INT32 || newType == Type.INT64 || newType == Type.FLOAT32 || newType ==
                                                                                                                              Type.FLOAT64) {
                        return newSchema;
                    }
                    break;
                case INT16:
                    if (newType == Type.INT8) {
                        return previous;
                    }
                    if (newType == Type.INT32 || newType == Type.INT64 || newType == Type.FLOAT32 || newType == Type.FLOAT64) {
                        return newSchema;
                    }
                    break;
                case INT32:
                    if (newType == Type.INT8 || newType == Type.INT16) {
                        return previous;
                    }
                    if (newType == Type.INT64 || newType == Type.FLOAT32 || newType == Type.FLOAT64) {
                        return newSchema;
                    }
                    break;
                case INT64:
                    if (newType == Type.INT8 || newType == Type.INT16 || newType == Type.INT32) {
                        return previous;
                    }
                    if (newType == Type.FLOAT32 || newType == Type.FLOAT64) {
                        return newSchema;
                    }
                    break;
                case FLOAT32:
                    if (newType == Type.INT8 || newType == Type.INT16 || newType == Type.INT32 || newType == Type.INT64) {
                        return previous;
                    }
                    if (newType == Type.FLOAT64) {
                        return newSchema;
                    }
                    break;
                case FLOAT64:
                    if (newType == Type.INT8 || newType == Type.INT16 || newType == Type.INT32 || newType == Type.INT64 || newType ==
                                                                                                                           Type.FLOAT32) {
                        return previous;
                    }
                    break;
            }
            return null;
        }
        if (previous.isOptional() == newSchema.isOptional()) {
            // Use the optional one
            return previous.isOptional() ? previous : newSchema;
        }
        if (!previous.equals(newSchema)) {
            return null;
        }
        return previous;
    }

    protected static List<Object> alignListEntriesWithSchema(Schema schema, List<Object> input) {
        Schema valueSchema = schema.valueSchema();
        List<Object> result = new ArrayList<>();
        for (Object value : input) {
            Object newValue = convertTo(valueSchema, null, value);
            result.add(newValue);
        }
        return result;
    }

    protected static Map<Object, Object> alignMapKeysAndValuesWithSchema(Schema mapSchema, Map<Object, Object> input) {
        Schema keySchema = mapSchema.keySchema();
        Schema valueSchema = mapSchema.valueSchema();
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : input.entrySet()) {
            Object newKey = convertTo(keySchema, null, entry.getKey());
            Object newValue = convertTo(valueSchema, null, entry.getValue());
            result.put(newKey, newValue);
        }
        return result;
    }

    protected static Map<Object, Object> alignMapKeysWithSchema(Schema mapSchema, Map<Object, Object> input) {
        Schema keySchema = mapSchema.keySchema();
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : input.entrySet()) {
            Object newKey = convertTo(keySchema, null, entry.getKey());
            result.put(newKey, entry.getValue());
        }
        return result;
    }

    protected static class SchemaDetector {
        private Type knownType = null;
        private boolean optional = false;

        public SchemaDetector() {
        }

        public boolean canDetect(Object value) {
            if (value == null) {
                optional = true;
                return true;
            }
            Schema schema = inferSchema(value);
            if (schema == null) {
                return false;
            }
            if (knownType == null) {
                knownType = schema.type();
            } else if (knownType != schema.type()) {
                return false;
            }
            return true;
        }

        public Schema schema() {
            SchemaBuilder builder = SchemaBuilder.type(knownType);
            if (optional) {
                builder.optional();
            }
            return builder.schema();
        }
    }

    protected static class Parser {
        private final String original;
        private final CharacterIterator iter;
        private String nextToken = null;
        private String previousToken = null;

        public Parser(String original) {
            this.original = original;
            this.iter = new StringCharacterIterator(this.original);
        }

        public int position() {
            return iter.getIndex();
        }

        public int mark() {
            return iter.getIndex() - (nextToken != null ? nextToken.length() : 0);
        }

        public void rewindTo(int position) {
            iter.setIndex(position);
            nextToken = null;
            previousToken = null;
        }

        public String original() {
            return original;
        }

        public boolean hasNext() {
            return nextToken != null || canConsumeNextToken();
        }

        protected boolean canConsumeNextToken() {
            return iter.getEndIndex() > iter.getIndex();
        }

        public String next() {
            if (nextToken != null) {
                previousToken = nextToken;
                nextToken = null;
            } else {
                previousToken = consumeNextToken();
            }
            return previousToken;
        }

        public String next(int n) {
            int current = mark();
            int start = mark();
            for (int i = 0; i != n; ++i) {
                if (!hasNext()) {
                    rewindTo(start);
                    return null;
                }
                next();
            }
            return original.substring(current, position());
        }

        private String consumeNextToken() throws NoSuchElementException {
            boolean escaped = false;
            int start = iter.getIndex();
            char c = iter.current();
            while (canConsumeNextToken()) {
                switch (c) {
                    case '\\':
                        escaped = !escaped;
                        break;
                    case ':':
                    case ',':
                    case '{':
                    case '}':
                    case '[':
                    case ']':
                    case '\"':
                        if (!escaped) {
                            if (start < iter.getIndex()) {
                                // Return the previous token
                                return original.substring(start, iter.getIndex());
                            }
                            // Consume and return this delimiter as a token
                            iter.next();
                            return original.substring(start, start + 1);
                        }
                        // escaped, so continue
                        escaped = false;
                        break;
                    default:
                        // If escaped, then we don't care what was escaped
                        escaped = false;
                        break;
                }
                c = iter.next();
            }
            return original.substring(start, iter.getIndex());
        }

        public String previous() {
            return previousToken;
        }

        public boolean canConsume(String expected) {
            return canConsume(expected, true);
        }

        public boolean canConsume(String expected, boolean ignoreLeadingAndTrailingWhitespace) {
            if (isNext(expected, ignoreLeadingAndTrailingWhitespace)) {
                // consume this token ...
                nextToken = null;
                return true;
            }
            return false;
        }

        protected boolean isNext(String expected, boolean ignoreLeadingAndTrailingWhitespace) {
            if (nextToken == null) {
                if (!hasNext()) {
                    return false;
                }
                // There's another token, so consume it
                nextToken = consumeNextToken();
            }
            if (ignoreLeadingAndTrailingWhitespace) {
                while (nextToken.trim().isEmpty() && canConsumeNextToken()) {
                    nextToken = consumeNextToken();
                }
            }
            return ignoreLeadingAndTrailingWhitespace
                ? nextToken.trim().equals(expected)
                : nextToken.equals(expected);
        }
    }
}
