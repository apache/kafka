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

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.CharacterIterator;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * Utility for converting from one Connect value to a different form. This is useful when the caller expects a value of a particular type
 * but is uncertain whether the actual value is one that isn't directly that type but can be converted into that type.
 *
 * <p>For example, a caller might expect a particular {@link org.apache.kafka.connect.header.Header} to contain a {@link Type#INT64}
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

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final SchemaAndValue NULL_SCHEMA_AND_VALUE = new SchemaAndValue(null, null);
    private static final Schema ARRAY_SELECTOR_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
    private static final Schema MAP_SELECTOR_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
    private static final Schema STRUCT_SELECTOR_SCHEMA = SchemaBuilder.struct().build();
    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;
    private static final String NULL_VALUE = "null";
    static final String ISO_8601_DATE_FORMAT_PATTERN = "yyyy-MM-dd";
    static final String ISO_8601_TIME_FORMAT_PATTERN = "HH:mm:ss.SSS'Z'";
    static final String ISO_8601_TIMESTAMP_FORMAT_PATTERN = ISO_8601_DATE_FORMAT_PATTERN + "'T'" + ISO_8601_TIME_FORMAT_PATTERN;

    private static final Pattern TWO_BACKSLASHES = Pattern.compile("\\\\");

    private static final Pattern DOUBLE_QUOTE = Pattern.compile("\"");

    /**
     * Convert the specified value to a {@link Type#BOOLEAN} value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a boolean.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a boolean, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a boolean
     */
    public static Boolean convertToBoolean(Schema schema, Object value) throws DataException {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            SchemaAndValue parsed = parseString(value.toString());
            if (parsed.value() instanceof Boolean) {
                return (Boolean) parsed.value();
            }
        }
        return asLong(value, schema, null) == 0L ? Boolean.FALSE : Boolean.TRUE;
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
        if (value == null) {
            return null;
        } else if (value instanceof Byte) {
            return (Byte) value;
        }
        return (byte) asLong(value, schema, null);
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
        if (value == null) {
            return null;
        } else if (value instanceof Short) {
            return (Short) value;
        }
        return (short) asLong(value, schema, null);
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
        if (value == null) {
            return null;
        } else if (value instanceof Integer) {
            return (Integer) value;
        }
        return (int) asLong(value, schema, null);
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
        if (value == null) {
            return null;
        } else if (value instanceof Long) {
            return (Long) value;
        }
        return asLong(value, schema, null);
    }

    /**
     * Convert the specified value to a {@link Type#FLOAT32} float value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a floating point number.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a float, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a float
     */
    public static Float convertToFloat(Schema schema, Object value) throws DataException {
        if (value == null) {
            return null;
        } else if (value instanceof Float) {
            return (Float) value;
        }
        return (float) asDouble(value, schema, null);
    }

    /**
     * Convert the specified value to a {@link Type#FLOAT64} double value. The supplied schema is required if the value is a logical
     * type when the schema contains critical information that might be necessary for converting to a floating point number.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a double, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a double
     */
    public static Double convertToDouble(Schema schema, Object value) throws DataException {
        if (value == null) {
            return null;
        } else if (value instanceof Double) {
            return (Double) value;
        }
        return asDouble(value, schema, null);
    }

    /**
     * Convert the specified value to a {@link Type#STRING} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a string, or null if the supplied value was null
     */
    public static String convertToString(Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        append(sb, value, false);
        return sb.toString();
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
        return convertToArray(ARRAY_SELECTOR_SCHEMA, value);
    }

    /**
     * Convert the specified value to a {@link Type#MAP} value. If the value is a string representation of a map, this method
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
        return convertToMapInternal(MAP_SELECTOR_SCHEMA, value);
    }

    /**
     * Convert the specified value to a {@link Type#STRUCT} value. Structs cannot be converted from other types, so this method returns
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
        return convertToStructInternal(STRUCT_SELECTOR_SCHEMA, value);
    }

    /**
     * Convert the specified value to a {@link Time#SCHEMA time} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a time, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a time value
     */
    public static java.util.Date convertToTime(Schema schema, Object value) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        }
        return convertToTime(Time.SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to a {@link Date#SCHEMA date} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a date, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a date value
     */
    public static java.util.Date convertToDate(Schema schema, Object value) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        }
        return convertToDate(Date.SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to a {@link Timestamp#SCHEMA timestamp} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a timestamp, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a timestamp value
     */
    public static java.util.Date convertToTimestamp(Schema schema, Object value) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        }
        return convertToTimestamp(Timestamp.SCHEMA, schema, value);
    }

    /**
     * Convert the specified value to a {@link Decimal decimal} value.
     * Not supplying a schema may limit the ability to convert to the desired type.
     *
     * @param schema the schema for the value; may be null
     * @param value  the value to be converted; may be null
     * @return the representation as a decimal, or null if the supplied value was null
     * @throws DataException if the value cannot be converted to a decimal value
     */
    public static BigDecimal convertToDecimal(Schema schema, Object value, int scale) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        }
        return convertToDecimal(Decimal.schema(scale), value);
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
        } else if (value instanceof Boolean) {
            return Schema.BOOLEAN_SCHEMA;
        } else if (value instanceof Byte) {
            return Schema.INT8_SCHEMA;
        } else if (value instanceof Short) {
            return Schema.INT16_SCHEMA;
        } else if (value instanceof Integer) {
            return Schema.INT32_SCHEMA;
        } else if (value instanceof Long) {
            return Schema.INT64_SCHEMA;
        } else if (value instanceof Float) {
            return Schema.FLOAT32_SCHEMA;
        } else if (value instanceof Double) {
            return Schema.FLOAT64_SCHEMA;
        } else if (value instanceof byte[] || value instanceof ByteBuffer) {
            return Schema.BYTES_SCHEMA;
        } else if (value instanceof List) {
            return inferListSchema((List<?>) value);
        } else if (value instanceof Map) {
            return inferMapSchema((Map<?, ?>) value);
        } else if (value instanceof Struct) {
            return ((Struct) value).schema();
        }
        return null;
    }

    private static Schema inferListSchema(List<?> list) {
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

    private static Schema inferMapSchema(Map<?, ?> map) {
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
        ValueParser parser = new ValueParser(new Parser(value));
        return parser.parse(false);
    }

    /**
     * Convert the value to the desired type.
     *
     * @param toSchema   the schema for the desired type; may not be null
     * @param fromSchema the schema for the supplied value; may be null if not known
     * @return the converted value; null if the passed-in schema was optional, and the input value was null.
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
                return convertMaybeLogicalBytes(toSchema, value);
            case STRING:
                return convertToString(fromSchema, value);
            case BOOLEAN:
                return convertToBoolean(fromSchema, value);
            case INT8:
                return convertToByte(fromSchema, value);
            case INT16:
                return convertToShort(fromSchema, value);
            case INT32:
                return convertMaybeLogicalInteger(toSchema, fromSchema, value);
            case INT64:
                return convertMaybeLogicalLong(toSchema, fromSchema, value);
            case FLOAT32:
                return convertToFloat(fromSchema, value);
            case FLOAT64:
                return convertToDouble(fromSchema, value);
            case ARRAY:
                return convertToArray(toSchema, value);
            case MAP:
                return convertToMapInternal(toSchema, value);
            case STRUCT:
                return convertToStructInternal(toSchema, value);
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
    }

    private static Serializable convertMaybeLogicalBytes(Schema toSchema, Object value) {
        if (Decimal.LOGICAL_NAME.equals(toSchema.name())) {
            return convertToDecimal(toSchema, value);
        }
        return convertToBytes(toSchema, value);
    }

    private static BigDecimal convertToDecimal(Schema toSchema, Object value) {
        if (value instanceof ByteBuffer) {
            value = Utils.toArray((ByteBuffer) value);
        }
        if (value instanceof byte[]) {
            return Decimal.toLogical(toSchema, (byte[]) value);
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            // Not already a decimal, so treat it as a double ...
            double converted = ((Number) value).doubleValue();
            return BigDecimal.valueOf(converted);
        }
        if (value instanceof String) {
            return new BigDecimal(value.toString());
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
    }

    private static byte[] convertToBytes(Schema toSchema, Object value) {
        if (value instanceof ByteBuffer) {
            return Utils.toArray((ByteBuffer) value);
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof BigDecimal) {
            return Decimal.fromLogical(toSchema, (BigDecimal) value);
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
    }

    private static Serializable convertMaybeLogicalInteger(Schema toSchema, Schema fromSchema, Object value) {
        if (Date.LOGICAL_NAME.equals(toSchema.name())) {
            return convertToDate(toSchema, fromSchema, value);
        }
        if (Time.LOGICAL_NAME.equals(toSchema.name())) {
            return convertToTime(toSchema, fromSchema, value);
        }
        return convertToInteger(fromSchema, value);
    }

    private static java.util.Date convertToDate(Schema toSchema, Schema fromSchema, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            SchemaAndValue parsed = parseString(value.toString());
            value = parsed.value();
        }
        if (value instanceof java.util.Date) {
            if (fromSchema != null) {
                String fromSchemaName = fromSchema.name();
                if (Date.LOGICAL_NAME.equals(fromSchemaName)) {
                    return (java.util.Date) value;
                }
                if (Timestamp.LOGICAL_NAME.equals(fromSchemaName)) {
                    // Just get the number of days from this timestamp
                    long millis = ((java.util.Date) value).getTime();
                    int days = (int) (millis / MILLIS_PER_DAY); // truncates
                    return Date.toLogical(toSchema, days);
                }
            } else {
                // There is no fromSchema, so no conversion is needed
                return (java.util.Date) value;
            }
        }
        long numeric = asLong(value, fromSchema, null);
        return Date.toLogical(toSchema, (int) numeric);
    }

    private static java.util.Date convertToTime(Schema toSchema, Schema fromSchema, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            SchemaAndValue parsed = parseString(value.toString());
            value = parsed.value();
        }
        if (value instanceof java.util.Date) {
            if (fromSchema != null) {
                String fromSchemaName = fromSchema.name();
                if (Time.LOGICAL_NAME.equals(fromSchemaName)) {
                    return (java.util.Date) value;
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
                return (java.util.Date) value;
            }
        }
        long numeric = asLong(value, fromSchema, null);
        return Time.toLogical(toSchema, (int) numeric);
    }

    private static Serializable convertMaybeLogicalLong(Schema toSchema, Schema fromSchema, Object value) {
        if (Timestamp.LOGICAL_NAME.equals(toSchema.name())) {
            return convertToTimestamp(toSchema, fromSchema, value);
        }
        return convertToLong(fromSchema, value);
    }

    private static java.util.Date convertToTimestamp(Schema toSchema, Schema fromSchema, Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            SchemaAndValue parsed = parseString(value.toString());
            value = parsed.value();
        }
        if (value instanceof java.util.Date date) {
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
                    return date;
                }
            } else {
                // There is no fromSchema, so no conversion is needed
                return date;
            }
        }
        long numeric = asLong(value, fromSchema, null);
        return Timestamp.toLogical(toSchema, numeric);
    }

    private static List<?> convertToArray(Schema toSchema, Object value) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        } else if (value instanceof String) {
            SchemaAndValue schemaAndValue = parseString(value.toString());
            value = schemaAndValue.value();
        }
        if (value instanceof List) {
            return (List<?>) value;
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
    }

    private static Map<?, ?> convertToMapInternal(Schema toSchema, Object value) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        } else if (value instanceof String) {
            SchemaAndValue schemaAndValue = parseString(value.toString());
            value = schemaAndValue.value();
        }
        if (value instanceof Map) {
            return (Map<?, ?>) value;
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + toSchema);
    }

    private static Struct convertToStructInternal(Schema toSchema, Object value) {
        if (value == null) {
            throw new DataException("Unable to convert a null value to a schema that requires a value");
        } else if (value instanceof Struct) {
            return (Struct) value;
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
            if (value instanceof Number number) {
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
            if (value instanceof Number number) {
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
        } else if (value instanceof List<?> list) {
            sb.append('[');
            appendIterable(sb, list.iterator());
            sb.append(']');
        } else if (value instanceof Map<?, ?> map) {
            sb.append('{');
            appendIterable(sb, map.entrySet().iterator());
            sb.append('}');
        } else if (value instanceof Struct struct) {
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
        } else if (value instanceof Map.Entry<?, ?> entry) {
            append(sb, entry.getKey(), true);
            sb.append(':');
            append(sb, entry.getValue(), true);
        } else if (value instanceof java.util.Date dateValue) {
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
        return DOUBLE_QUOTE.matcher(replace1).replaceAll("\\\\\"");
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

    private static class ValueParser {

        private static final Logger log = LoggerFactory.getLogger(ValueParser.class);
        private static final SchemaAndValue TRUE_SCHEMA_AND_VALUE = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, Boolean.TRUE);
        private static final SchemaAndValue FALSE_SCHEMA_AND_VALUE = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, Boolean.FALSE);
        private static final String TRUE_LITERAL = Boolean.TRUE.toString();
        private static final String FALSE_LITERAL = Boolean.FALSE.toString();
        private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
        private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);
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

        private final Parser parser;

        private ValueParser(Parser parser) {
            this.parser = parser;
        }

        private boolean canParseSingleTokenLiteral(boolean embedded, String tokenLiteral) {
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

        public SchemaAndValue parse(boolean embedded) throws NoSuchElementException {
            if (!parser.hasNext()) {
                return null;
            } else if (embedded && parser.canConsume(QUOTE_DELIMITER)) {
                return parseQuotedString();
            } else if (canParseSingleTokenLiteral(embedded, NULL_VALUE)) {
                return null;
            } else if (canParseSingleTokenLiteral(embedded, TRUE_LITERAL)) {
                return TRUE_SCHEMA_AND_VALUE;
            } else if (canParseSingleTokenLiteral(embedded, FALSE_LITERAL)) {
                return FALSE_SCHEMA_AND_VALUE;
            }

            int startPosition = parser.mark();

            try {
                if (parser.canConsume(ARRAY_BEGIN_DELIMITER)) {
                    return parseArray();
                } else if (parser.canConsume(MAP_BEGIN_DELIMITER)) {
                    return parseMap();
                }
            } catch (DataException e) {
                log.trace("Unable to parse the value as a map or an array; reverting to string", e);
                parser.rewindTo(startPosition);
            }

            String token = parser.next();
            if (Utils.isBlank(token)) {
                return new SchemaAndValue(Schema.STRING_SCHEMA, token);
            } else {
                return parseNextToken(embedded, token.trim());
            }
        }

        private SchemaAndValue parseNextToken(boolean embedded, String token) {
            char firstChar = token.charAt(0);
            boolean firstCharIsDigit = Character.isDigit(firstChar);

            // Temporal types are more restrictive, so try them first
            if (firstCharIsDigit) {
                SchemaAndValue temporal = parseMultipleTokensAsTemporal(token);
                if (temporal != null) {
                    return temporal;
                }
            }
            if (firstCharIsDigit || firstChar == '+' || firstChar == '-') {
                try {
                    return parseAsNumber(token);
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

        private SchemaAndValue parseQuotedString() {
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
            SchemaAndValue parsed = parseAsTemporal(content);
            if (parsed != null) {
                return parsed;
            }
            return new SchemaAndValue(Schema.STRING_SCHEMA, content);
        }

        private SchemaAndValue parseArray() {
            List<Object> result = new ArrayList<>();
            SchemaMerger elementSchema = new SchemaMerger();
            while (parser.hasNext()) {
                if (parser.canConsume(ARRAY_END_DELIMITER)) {
                    Schema listSchema;
                    if (elementSchema.hasCommonSchema()) {
                        listSchema = SchemaBuilder.array(elementSchema.schema()).schema();
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
                SchemaAndValue element = parse(true);
                elementSchema.merge(element);
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

        private SchemaAndValue parseMap() {
            Map<Object, Object> result = new LinkedHashMap<>();
            SchemaMerger keySchema = new SchemaMerger();
            SchemaMerger valueSchema = new SchemaMerger();
            while (parser.hasNext()) {
                if (parser.canConsume(MAP_END_DELIMITER)) {
                    Schema mapSchema;
                    if (keySchema.hasCommonSchema() && valueSchema.hasCommonSchema()) {
                        mapSchema = SchemaBuilder.map(keySchema.schema(), valueSchema.schema()).build();
                        result = alignMapKeysAndValuesWithSchema(mapSchema, result);
                    } else if (keySchema.hasCommonSchema()) {
                        mapSchema = SchemaBuilder.mapWithNullValues(keySchema.schema());
                        result = alignMapKeysWithSchema(mapSchema, result);
                    } else {
                        mapSchema = SchemaBuilder.mapOfNull().build();
                    }
                    return new SchemaAndValue(mapSchema, result);
                }

                if (parser.canConsume(COMMA_DELIMITER)) {
                    throw new DataException("Unable to parse a map entry with no key or value: " + parser.original());
                }
                SchemaAndValue key = parse(true);
                if (key == null || key.value() == null) {
                    throw new DataException("Map entry may not have a null key: " + parser.original());
                } else if (!parser.canConsume(ENTRY_DELIMITER)) {
                    throw new DataException("Map entry is missing '" + ENTRY_DELIMITER
                            + "' at " + parser.position()
                            + " in " + parser.original());
                }
                SchemaAndValue value = parse(true);
                Object entryValue = value != null ? value.value() : null;
                result.put(key.value(), entryValue);

                parser.canConsume(COMMA_DELIMITER);
                keySchema.merge(key);
                valueSchema.merge(value);
            }
            // Missing either a comma or an end delimiter
            if (COMMA_DELIMITER.equals(parser.previous())) {
                throw new DataException("Map is missing element after ',': " + parser.original());
            }
            throw new DataException("Map is missing terminating '}': " + parser.original());
        }

        private SchemaAndValue parseMultipleTokensAsTemporal(String token) {
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
            return parseAsTemporal(token);
        }


        private static SchemaAndValue parseAsNumber(String token) {
            // Try to parse as a number ...
            BigDecimal decimal = new BigDecimal(token);
            SchemaAndValue exactDecimal = parseAsExactDecimal(decimal);
            float fValue = decimal.floatValue();
            double dValue = decimal.doubleValue();
            if (exactDecimal != null) {
                return exactDecimal;
            } else if (fValue != Float.NEGATIVE_INFINITY && fValue != Float.POSITIVE_INFINITY
                    && decimal.scale() != 0) {
                return new SchemaAndValue(Schema.FLOAT32_SCHEMA, fValue);
            } else if (dValue != Double.NEGATIVE_INFINITY && dValue != Double.POSITIVE_INFINITY
                    && decimal.scale() != 0) {
                return new SchemaAndValue(Schema.FLOAT64_SCHEMA, dValue);
            } else {
                Schema schema = Decimal.schema(decimal.scale());
                return new SchemaAndValue(schema, decimal);
            }
        }

        private static boolean isWholeNumber(BigDecimal bd) {
            return bd.signum() == 0 || bd.scale() <= 0 || bd.stripTrailingZeros().scale() <= 0;
        }

        private static final BigDecimal BIGGER_THAN_LONG = new BigDecimal("1e19");

        private static SchemaAndValue parseAsExactDecimal(BigDecimal decimal) {
            BigDecimal abs = decimal.abs();
            if (abs.compareTo(BIGGER_THAN_LONG) > 0 || (abs.compareTo(BigDecimal.ONE) < 0 && abs.compareTo(BigDecimal.ZERO) != 0)) {
                return null;
            }
            if (isWholeNumber(decimal)) {
                BigInteger num = decimal.toBigIntegerExact();
                if (num.compareTo(LONG_MIN) < 0 || num.compareTo(LONG_MAX) > 0) {
                    return null;
                }
                long integral = num.longValue();
                byte int8 = (byte) integral;
                short int16 = (short) integral;
                int int32 = (int) integral;
                if (int8 == integral) {
                    return new SchemaAndValue(Schema.INT8_SCHEMA, int8);
                } else if (int16 == integral) {
                    return new SchemaAndValue(Schema.INT16_SCHEMA, int16);
                } else if (int32 == integral) {
                    return new SchemaAndValue(Schema.INT32_SCHEMA, int32);
                } else {
                    return new SchemaAndValue(Schema.INT64_SCHEMA, integral);
                }
            }
            return null;
        }

        private static SchemaAndValue parseAsTemporal(String token) {
            if (token == null) {
                return null;
            }
            // If the colons were escaped, we'll see the escape chars and need to remove them
            token = token.replace("\\:", ":");
            int tokenLength = token.length();
            if (tokenLength == ISO_8601_TIME_LENGTH) {
                return parseAsTemporalType(token, Time.SCHEMA, ISO_8601_TIME_FORMAT_PATTERN);
            } else if (tokenLength == ISO_8601_TIMESTAMP_LENGTH) {
                return parseAsTemporalType(token, Timestamp.SCHEMA, ISO_8601_TIMESTAMP_FORMAT_PATTERN);
            } else if (tokenLength == ISO_8601_DATE_LENGTH) {
                return parseAsTemporalType(token, Date.SCHEMA, ISO_8601_DATE_FORMAT_PATTERN);
            } else {
                return null;
            }
        }

        private static SchemaAndValue parseAsTemporalType(String token, Schema schema, String pattern) {
            ParsePosition pos = new ParsePosition(0);
            java.util.Date result = new SimpleDateFormat(pattern).parse(token, pos);
            if (pos.getIndex() != 0) {
                return new SchemaAndValue(schema, result);
            }
            return null;
        }
    }

    /**
     * Utility for merging various optional primitive numeric schemas into a common schema.
     * If a non-numeric type appears (including logical numeric types), no common schema will be inferred.
     * This class is not thread-safe and should only be accessed by one thread.
     */
    private static class SchemaMerger {
        /**
         * Schema which applies to all of the values passed to {@link #merge(SchemaAndValue)}
         * Null if no non-null schemas have been seen, or if the values seen do not have a common schema
         */
        private Schema common = null;
        /**
         * Flag to determine the meaning of the null sentinel in {@link #common}
         * If true, null means "any optional type", as no non-null values have appeared.
         * If false, null means "no common type", as one or more non-null values had mutually exclusive schemas.
         */
        private boolean compatible = true;

        protected void merge(SchemaAndValue latest) {
            if (latest != null && latest.schema() != null && compatible) {
                if (common == null) {
                    // This null means any type is valid, so choose the new schema.
                    common = latest.schema();
                } else {
                    // There is a previous type restriction, so merge the new schema into the old one.
                    common = mergeSchemas(common, latest.schema());
                    // If there isn't a common schema any longer, then give up on finding further compatible schemas.
                    compatible = common != null;
                }
            }
        }

        protected boolean hasCommonSchema() {
            return common != null;
        }

        protected Schema schema() {
            return common;
        }
    }

    /**
     * Merge two schemas to a common schema which can represent values from both input schemas.
     * @param previous One Schema, non-null
     * @param newSchema Another schema, non-null
     * @return A schema that is a superset of both input schemas, or null if no common schema is found.
     */
    private static Schema mergeSchemas(Schema previous, Schema newSchema) {
        Type previousType = previous.type();
        Type newType = newSchema.type();
        if (previousType != newType) {
            switch (previous.type()) {
                case INT8:
                    return commonSchemaForInt8(newSchema, newType);
                case INT16:
                    return commonSchemaForInt16(previous, newSchema, newType);
                case INT32:
                    return commonSchemaForInt32(previous, newSchema, newType);
                case INT64:
                    return commonSchemaForInt64(previous, newSchema, newType);
                case FLOAT32:
                    return commonSchemaForFloat32(previous, newSchema, newType);
                case FLOAT64:
                    return commonSchemaForFloat64(previous, newType);
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

    private static Schema commonSchemaForInt8(Schema newSchema, Type newType) {
        if (newType == Type.INT16 || newType == Type.INT32 || newType == Type.INT64
                || newType == Type.FLOAT32 || newType == Type.FLOAT64) {
            return newSchema;
        }
        return null;
    }

    private static Schema commonSchemaForInt16(Schema previous, Schema newSchema, Type newType) {
        if (newType == Type.INT8) {
            return previous;
        } else if (newType == Type.INT32 || newType == Type.INT64
                || newType == Type.FLOAT32 || newType == Type.FLOAT64) {
            return newSchema;
        }
        return null;
    }

    private static Schema commonSchemaForInt32(Schema previous, Schema newSchema, Type newType) {
        if (newType == Type.INT8 || newType == Type.INT16) {
            return previous;
        } else if (newType == Type.INT64 || newType == Type.FLOAT32 || newType == Type.FLOAT64) {
            return newSchema;
        }
        return null;
    }

    private static Schema commonSchemaForInt64(Schema previous, Schema newSchema, Type newType) {
        if (newType == Type.INT8 || newType == Type.INT16 || newType == Type.INT32) {
            return previous;
        } else if (newType == Type.FLOAT32 || newType == Type.FLOAT64) {
            return newSchema;
        }
        return null;
    }

    private static Schema commonSchemaForFloat32(Schema previous, Schema newSchema, Type newType) {
        if (newType == Type.INT8 || newType == Type.INT16 || newType == Type.INT32 || newType == Type.INT64) {
            return previous;
        } else if (newType == Type.FLOAT64) {
            return newSchema;
        }
        return null;
    }

    private static Schema commonSchemaForFloat64(Schema previous, Type newType) {
        if (newType == Type.INT8 || newType == Type.INT16 || newType == Type.INT32 || newType == Type.INT64
                || newType == Type.FLOAT32) {
            return previous;
        }
        return null;
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
                while (Utils.isBlank(nextToken) && canConsumeNextToken()) {
                    nextToken = consumeNextToken();
                }
            }
            return ignoreLeadingAndTrailingWhitespace
                ? nextToken.trim().equals(expected)
                : nextToken.equals(expected);
        }
    }
}
