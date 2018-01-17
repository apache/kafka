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

import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.CharacterIterator;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Utility for converting values into a different form.
 */
public class Values {

    private static final Logger LOGGER = LoggerFactory.getLogger(Values.class);

    private static final SchemaAndValue NULL_SCHEMA_AND_VALUE = new SchemaAndValue(null, null);
    private static final SchemaAndValue TRUE_SCHEMA_AND_VALUE = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, Boolean.TRUE);
    private static final SchemaAndValue FALSE_SCHEMA_AND_VALUE = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, Boolean.FALSE);
    private static final Schema ARRAY_SELECTOR_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
    private static final Schema MAP_SELECTOR_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
    private static final Schema STRUCT_SELECTOR_SCHEMA = SchemaBuilder.struct().build();
    private static final String TRUE_LITERAL = Boolean.TRUE.toString();
    private static final String FALSE_LITERAL = Boolean.TRUE.toString();
    private static final Charset UTF_8 = StandardCharsets.UTF_8;
    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;
    private static final String NULL_VALUE = "null";
    private static final String ISO_8601_DATE_FORMAT_PATTERN = "YYYY-MM-DD";
    private static final String ISO_8601_TIME_FORMAT_PATTERN = "HH:mm:ss.SSS'Z'";
    private static final String ISO_8601_TIMESTAMP_FORMAT_PATTERN = ISO_8601_DATE_FORMAT_PATTERN + "'T'" + ISO_8601_TIME_FORMAT_PATTERN;

    private static final String QUOTE_DELIMITER = "\"";
    private static final String COMMA_DELIMITER = ",";
    private static final String ENTRY_DELIMITER = ":";
    private static final String ARRAY_BEGIN_DELIMITER = "[";
    private static final String ARRAY_END_DELIMITER = "]";
    private static final String MAP_BEGIN_DELIMITER = "{";
    private static final String MAP_END_DELIMITER = "}";
    private static final int IS_8601_DATE_LENGTH = ISO_8601_DATE_FORMAT_PATTERN.length();
    private static final int IS_8601_TIME_LENGTH = ISO_8601_TIME_FORMAT_PATTERN.length() - 2; // subtract single quotes
    private static final int IS_8601_TIMESTAMP_LENGTH = ISO_8601_TIMESTAMP_FORMAT_PATTERN.length() - 4; // subtract single quotes

    /**
     * Convert the specified value to an {@link Type#BOOLEAN} value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a boolean, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a boolean
     */
    public static Boolean convertToBoolean(Object value) throws DataException {
        return (Boolean) convertTo(Schema.OPTIONAL_BOOLEAN_SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Type#INT8} byte value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a byte, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a byte
     */
    public static Byte convertToByte(Object value) throws DataException {
        return (Byte) convertTo(Schema.OPTIONAL_INT8_SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Type#INT16} short value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a short, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a short
     */
    public static Short convertToShort(Object value) throws DataException {
        return (Short) convertTo(Schema.OPTIONAL_INT16_SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Type#INT32} int value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as an integer, or null if the supplied value was null
     * @throws DataException if the value could not be converted to an integer
     */
    public static Integer convertToInteger(Object value) throws DataException {
        return (Integer) convertTo(Schema.OPTIONAL_INT32_SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Type#INT64} long value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a long, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a long
     */
    public static Long convertToLong(Object value) throws DataException {
        return (Long) convertTo(Schema.OPTIONAL_INT64_SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Type#FLOAT32} float value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a float, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a float
     */
    public static Float convertToFloat(Object value) throws DataException {
        return (Float) convertTo(Schema.OPTIONAL_FLOAT32_SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Type#FLOAT64} double value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a double, or null if the supplied value was null
     * @throws DataException if the value could not be converted to a double
     */
    public static Double convertToDouble(Object value) throws DataException {
        return (Double) convertTo(Schema.OPTIONAL_FLOAT64_SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Type#STRING} value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a string, or null if the supplied value was null
     */
    public static String convertToString(Object value) {
        return (String) convertTo(Schema.OPTIONAL_STRING_SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Type#STRING} value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a list, or null if the supplied value was null
     */
    public static List<?> convertToList(Object value) {
        return (List<?>) convertTo(ARRAY_SELECTOR_SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Type#STRING} value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a list, or null if the supplied value was null
     */
    public static Map<?, ?> convertToMap(Object value) {
        return (Map<?, ?>) convertTo(MAP_SELECTOR_SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Type#STRING} value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a list, or null if the supplied value was null
     */
    public static Struct convertToStruct(Object value) {
        return (Struct) convertTo(STRUCT_SELECTOR_SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Time#SCHEMA time} value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a time, or null if the supplied value was null
     */
    public static java.util.Date convertToTime(Object value) {
        return (java.util.Date) convertTo(Time.SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Date#SCHEMA date} value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a date, or null if the supplied value was null
     */
    public static java.util.Date convertToDate(Object value) {
        return (java.util.Date) convertTo(Date.SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Timestamp#SCHEMA timestamp} value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a timestamp, or null if the supplied value was null
     */
    public static java.util.Date convertToTimestamp(Object value) {
        return (java.util.Date) convertTo(Timestamp.SCHEMA, value);
    }

    /**
     * Convert the specified value to an {@link Timestamp#SCHEMA timestamp} value.
     *
     * @param value the value to be converted; may be null
     * @return the representation as a timestamp, or null if the supplied value was null
     */
    public static BigDecimal convertToDecimal(Object value, int scale) {
        return (BigDecimal) convertTo(Decimal.schema(scale), value);
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
        if (value instanceof byte[]) {
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
     * @see #convertToString(Object)
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
     * @param toSchema the schema for the desired type; may not be null
     * @return the converted value; never null
     * @throws DataException if the value could not be converted to the desired type
     */
    protected static Object convertTo(Schema toSchema, Object value) throws DataException {
        if (value == null) {
            if (toSchema.isOptional()) {
                return null;
            }
            throw new DataException("Unable to convert a null value to a non-null value");
        }
        switch (toSchema.type()) {
            case BYTES:
                if (Decimal.LOGICAL_NAME.equals(toSchema.name())) {
                    if (value instanceof byte[]) {
                        return Decimal.toLogical(toSchema, (byte[]) value);
                    }
                    if (value instanceof BigDecimal) {
                        return value;
                    }
                    if (value instanceof Number) {
                        // Not already a decimal, so treat it as a double ...
                        double converted = ((Number) value).doubleValue();
                        return new BigDecimal(converted);
                    }
                    if (value instanceof String) {
                        return new BigDecimal(value.toString()).doubleValue();
                    }
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
                return asLong(value, toSchema, null) == 1L ? Boolean.TRUE : Boolean.FALSE;
            case INT8:
                if (value instanceof Byte) {
                    return value;
                }
                return (byte) asLong(value, toSchema, null);
            case INT16:
                if (value instanceof Short) {
                    return value;
                }
                return (short) asLong(value, toSchema, null);
            case INT32:
                if (Date.LOGICAL_NAME.equals(toSchema.name())) {
                    if (value instanceof String) {
                        SchemaAndValue parsed = parseString(value.toString());
                        value = parsed.value();
                    }
                    if (value instanceof java.util.Date) {
                        return new SchemaAndValue(toSchema, value);
                    }
                }
                if (Time.LOGICAL_NAME.equals(toSchema.name())) {
                    if (value instanceof String) {
                        SchemaAndValue parsed = parseString(value.toString());
                        value = parsed.value();
                    }
                    if (value instanceof java.util.Date) {
                        return new SchemaAndValue(toSchema, value);
                    }
                }
                if (value instanceof Integer) {
                    return value;
                }
                return (int) asLong(value, toSchema, null);
            case INT64:
                if (Timestamp.LOGICAL_NAME.equals(toSchema.name())) {
                    if (value instanceof String) {
                        SchemaAndValue parsed = parseString(value.toString());
                        value = parsed.value();
                    }
                    if (value instanceof java.util.Date) {
                        return new SchemaAndValue(toSchema, value);
                    }
                }
                if (value instanceof Long) {
                    return value;
                }
                return asLong(value, toSchema, null);
            case FLOAT32:
                if (value instanceof Float) {
                    return value;
                }
                return (float) asDouble(value, toSchema, null);
            case FLOAT64:
                if (value instanceof Double) {
                    return value;
                }
                return asDouble(value, toSchema, null);
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
     * @param value  the value to be converted; may not be null
     * @param schema the schema for the desired type; may not be null
     * @param error  any previous error that should be included in an exception message; may be null
     * @return the long value after conversion; never null
     * @throws DataException if the value could not be converted to a long
     */
    protected static long asLong(Object value, Schema schema, Throwable error) {
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
        if (value instanceof java.util.Date) {
            if (schema == null) {
                return ((java.util.Date) value).getTime();
            }
            if (Date.LOGICAL_NAME.equals(schema.name())) {
                return Date.fromLogical(schema, (java.util.Date) value);
            }
            if (Time.LOGICAL_NAME.equals(schema.name())) {
                return Time.fromLogical(schema, (java.util.Date) value);
            }
            if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
                return Timestamp.fromLogical(schema, (java.util.Date) value);
            }
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + schema, error);
    }

    /**
     * Convert the specified value with the desired floating point type.
     *
     * @param value  the value to be converted; may not be null
     * @param schema the schema for the desired type; may not be null
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
            value = new String((byte[]) value, UTF_8);
            if (embedded) {
                sb.append('"').append(value).append('"');
            } else {
                sb.append(value);
            }
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
        return value.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"");
    }

    protected static DateFormat dateFormatFor(java.util.Date value) {
        if (value.getTime() < MILLIS_PER_DAY) {
            return new SimpleDateFormat(ISO_8601_TIME_FORMAT_PATTERN);
        }
        if (value.getTime() % MILLIS_PER_DAY == 0) {
            return new SimpleDateFormat(ISO_8601_DATE_FORMAT_PATTERN);
        }
        return new SimpleDateFormat(ISO_8601_TIMESTAMP_FORMAT_PATTERN);
    }

    protected static SchemaAndValue parse(Parser parser, boolean embedded) throws NoSuchElementException {
        if (!parser.hasNext()) {
            return null;
        }
        if (embedded) {
            if (parser.canConsume(NULL_VALUE)) {
                return null;
            }
            if (parser.canConsume(QUOTE_DELIMITER)) {
                StringBuilder sb = new StringBuilder();
                while (parser.hasNext()) {
                    if (parser.canConsume(QUOTE_DELIMITER)) {
                        break;
                    }
                    sb.append(parser.next());
                }
                return new SchemaAndValue(Schema.STRING_SCHEMA, sb.toString());
            }
        }
        if (parser.canConsume(TRUE_LITERAL)) {
            return TRUE_SCHEMA_AND_VALUE;
        }
        if (parser.canConsume(FALSE_LITERAL)) {
            return FALSE_SCHEMA_AND_VALUE;
        }
        int startPosition = parser.mark();
        try {
            if (parser.canConsume(ARRAY_BEGIN_DELIMITER)) {
                List<Object> result = new ArrayList<>();
                Schema elementSchema = null;
                while (parser.hasNext()) {
                    if (parser.canConsume(ARRAY_END_DELIMITER)) {
                        Schema listSchema = elementSchema == null ? null : SchemaBuilder.array(elementSchema).schema();
                        result = alignListEntriesWithSchema(listSchema, result);
                        return new SchemaAndValue(listSchema, result);
                    }
                    SchemaAndValue element = parse(parser, true);
                    elementSchema = commonSchemaFor(elementSchema, element);
                    result.add(element.value());
                    parser.canConsume(COMMA_DELIMITER);
                }
                // Missing either a comma or an end delimiter
                if (COMMA_DELIMITER.equals(parser.previous())) {
                    throw new DataException("Malformed array: missing element after ','");
                }
                throw new DataException("Malformed array: missing terminating ']'");
            }

            if (parser.canConsume(MAP_BEGIN_DELIMITER)) {
                Map<Object, Object> result = new LinkedHashMap<>();
                Schema keySchema = null;
                Schema valueSchema = null;
                while (parser.hasNext()) {
                    if (parser.canConsume(MAP_END_DELIMITER)) {
                        Schema mapSchema =
                                keySchema == null || valueSchema == null ? null : SchemaBuilder.map(keySchema, valueSchema).schema();
                        result = alignMapKeysAndValuesWithSchema(mapSchema, result);
                        return new SchemaAndValue(mapSchema, result);
                    }
                    SchemaAndValue key = parse(parser, true);
                    if (key == null || key.value() == null) {
                        throw new DataException("Malformed map entry: null key");
                    }
                    if (!parser.canConsume(ENTRY_DELIMITER)) {
                        throw new DataException("Malformed map entry: missing '='");
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
                    throw new DataException("Malformed map: missing element after ','");
                }
                throw new DataException("Malformed array: missing terminating ']'");
            }
        } catch (DataException e) {
            LOGGER.debug("Unable to parse the value as a map; reverting to string", e);
            parser.rewindTo(startPosition);
        }
        String token = parser.next().trim();
        assert !token.isEmpty(); // original can be empty string but is handled right away; no way for token to be empty here
        char firstChar = token.charAt(0);
        boolean firstCharIsDigit = Character.isDigit(firstChar);
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
                double dValue = decimal.doubleValue();
                if (dValue != Double.NEGATIVE_INFINITY && dValue != Double.POSITIVE_INFINITY) {
                    return new SchemaAndValue(Schema.FLOAT64_SCHEMA, dValue);
                }
                Schema schema = Decimal.schema(decimal.scale());
                return new SchemaAndValue(schema, decimal);
            } catch (NumberFormatException e) {
                // can't parse as a number
            }
        }
        if (firstCharIsDigit) {
            // Check for a date, time, or timestamp ...
            int tokenLength = token.length();
            if (tokenLength == IS_8601_DATE_LENGTH) {
                try {
                    return new SchemaAndValue(Date.SCHEMA, new SimpleDateFormat(ISO_8601_DATE_FORMAT_PATTERN).parse(token));
                } catch (ParseException e) {
                    // not a valid date
                }
            } else if (tokenLength == IS_8601_TIME_LENGTH) {
                try {
                    return new SchemaAndValue(Time.SCHEMA, new SimpleDateFormat(ISO_8601_TIME_FORMAT_PATTERN).parse(token));
                } catch (ParseException e) {
                    // not a valid date
                }
            } else if (tokenLength == IS_8601_TIMESTAMP_LENGTH) {
                try {
                    return new SchemaAndValue(Time.SCHEMA, new SimpleDateFormat(ISO_8601_TIMESTAMP_FORMAT_PATTERN).parse(token));
                } catch (ParseException e) {
                    // not a valid date
                }
            }
        }
        // At this point, the only thing this can be is a string. Embedded strings were processed above,
        // so this is not embedded and we can use the original string...
        return new SchemaAndValue(Schema.STRING_SCHEMA, parser.original());
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
        if (schema == null) {
            return input;
        }
        Schema valueSchema = schema.valueSchema();
        if (!isProjectable(valueSchema)) {
            return input;
        }
        List<Object> result = new ArrayList<>();
        for (Object value : input) {
            Object newValue = project(valueSchema, value);
            result.add(newValue);
        }
        return result;
    }

    protected static Map<Object, Object> alignMapKeysAndValuesWithSchema(Schema mapSchema, Map<Object, Object> input) {
        if (mapSchema == null) {
            return input;
        }
        Schema keySchema = mapSchema.keySchema();
        Schema valueSchema = mapSchema.valueSchema();
        if (!isProjectable(keySchema) && !isProjectable(valueSchema)) {
            return input;
        }
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : input.entrySet()) {
            Object newKey = project(keySchema, entry.getKey());
            Object newValue = project(valueSchema, entry.getValue());
            result.put(newKey, newValue);
        }
        return result;
    }

    protected static boolean isProjectable(Schema schema) {
        switch (schema.type()) {
            case BYTES:
                if (Decimal.LOGICAL_NAME.equals(schema.name())) {
                    return true;
                }
                return false;
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
                return true;
            default:
                return false;
        }
    }

    protected static Object project(Schema desiredSchema, Object value) {
        if (value instanceof Number) {
            Number number = (Number) value;
            switch (desiredSchema.type()) {
                case BYTES:
                    if (Decimal.LOGICAL_NAME.equals(desiredSchema.name())) {
                        if (number instanceof BigDecimal) {
                            return number;
                        }
                        if (number instanceof Double || number instanceof Float) {
                            return new BigDecimal(number.doubleValue());
                        }
                        return new BigDecimal(number.longValue());
                    }
                    return value;
                case INT8:
                    return number.byteValue();
                case INT16:
                    return number.shortValue();
                case INT32:
                    if (Date.SCHEMA.equals(desiredSchema) || Time.SCHEMA.equals(desiredSchema)) {
                        return new java.util.Date(number.longValue());
                    }
                    return number.intValue();
                case INT64:
                    if (Timestamp.SCHEMA.equals(desiredSchema)) {
                        return new java.util.Date(number.longValue());
                    }
                    return number.longValue();
                case FLOAT32:
                    return number.floatValue();
                case FLOAT64:
                    return number.doubleValue();
                default:
                    break;
            }
        }
        if (value instanceof java.util.Date) {
            java.util.Date date = (java.util.Date) value;
            switch (desiredSchema.type()) {
                case BYTES:
                    if (Decimal.LOGICAL_NAME.equals(desiredSchema.name())) {
                        return new BigDecimal(date.getTime());
                    }
                    return value;
                case INT64:
                    return date.getTime();
                case FLOAT64:
                    return (double) date.getTime();
                default:
                    break;
            }
        }
        return value;
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

        private String consumeNextToken() throws NoSuchElementException {
            boolean escaped = false;
            int start = iter.getIndex();
            char c = iter.current();
            while (c != CharacterIterator.DONE) {
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
                nextToken = nextToken.trim();
                while (nextToken.isEmpty() && canConsumeNextToken()) {
                    nextToken = consumeNextToken().trim();
                }
            }
            return nextToken.equals(expected);
        }
    }
}
