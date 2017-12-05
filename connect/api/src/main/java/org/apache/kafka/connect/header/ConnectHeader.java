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
package org.apache.kafka.connect.header;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link Header} implementation.
 */
class ConnectHeader implements Header {

    private static final SchemaAndValue NULL_SCHEMA_AND_VALUE = new SchemaAndValue(null, null);

    private static final String TRUE_WORD_LITERAL = "true";
    private static final String TRUE_LETTER_LITERAL = "t";
    private static final String FALSE_WORD_LITERAL = "false";
    private static final String FALSE_LETTER_LITERAL = "f";

    private final String key;
    private final SchemaAndValue schemaAndValue;

    protected ConnectHeader(String key, SchemaAndValue schemaAndValue) {
        Objects.requireNonNull(key, "Null header keys are not permitted");
        this.key = key;
        this.schemaAndValue = schemaAndValue != null ? schemaAndValue : NULL_SCHEMA_AND_VALUE;
        assert this.schemaAndValue != null;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public SchemaAndValue schemaAndValue() {
        return schemaAndValue;
    }

    @Override
    public Object valueAsObject() {
        return schemaAndValue.value();
    }

    @Override
    public Schema schema() {
        return schemaAndValue.schema();
    }

    @Override
    public Header rename(String key) {
        Objects.requireNonNull(key, "Null header keys are not permitted");
        return new ConnectHeader(key, schemaAndValue);
    }

    @Override
    public Header with(Schema schema, Object value) {
        return with(new SchemaAndValue(schema, value));
    }

    public Header with(SchemaAndValue schemaAndValue) {
        return new ConnectHeader(key, schemaAndValue);
    }

    @Override
    public byte[] valueAsBytes() throws DataException {
        return (byte[]) valueAsType(Type.BYTES, true);
    }

    @Override
    public boolean valueAsBoolean() throws DataException {
        return (boolean) valueAsType(Type.BOOLEAN, false);
    }

    @Override
    public byte valueAsByte() throws DataException {
        return (byte) valueAsType(Type.INT8, false);
    }

    @Override
    public short valueAsShort() throws DataException {
        return (short) valueAsType(Type.INT16, false);
    }

    @Override
    public int valueAsInt() throws DataException {
        return (int) valueAsType(Type.INT32, false);
    }

    @Override
    public long valueAsLong() throws DataException {
        return (long) valueAsType(Type.INT64, false);
    }

    @Override
    public float valueAsFloat() throws DataException {
        return (float) valueAsType(Type.FLOAT32, false);
    }

    @Override
    public double valueAsDouble() throws DataException {
        return (double) valueAsType(Type.FLOAT64, false);
    }

    @Override
    public String valueAsString() {
        return (String) valueAsType(Type.STRING, true);
    }

    @Override
    public <T> List<T> valueAsList() throws DataException {
        return (List<T>) valueAsType(Type.ARRAY, true);
    }

    @Override
    public <K, V> Map<K, V> valueAsMap() throws DataException {
        return (Map<K, V>) valueAsType(Type.MAP, true);
    }

    @Override
    public Struct valueAsStruct() throws DataException {
        return (Struct) valueAsType(Type.STRUCT, true);
    }

    @Override
    public BigDecimal valueAsDecimal(int scale, RoundingMode roundingMode) throws DataException {
        Object value = valueAsObject();
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof BigDecimal) {
                BigDecimal decimal = (BigDecimal) value;
                if (decimal.scale() != scale) {
                    if (roundingMode == null) {
                        roundingMode = RoundingMode.HALF_EVEN;
                    }
                    decimal = decimal.setScale(scale, roundingMode);
                }
                return decimal;
            }
            if (value instanceof Double || value instanceof Float) {
                return BigDecimal.valueOf(((Number) value).doubleValue()).setScale(scale);
            }
            if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
                return BigDecimal.valueOf(((Number) value).longValue()).setScale(scale);
            }
            if (value instanceof Boolean) {
                return Boolean.TRUE.equals(value) ? BigDecimal.ONE : BigDecimal.ZERO;
            }
            if (value instanceof String) {
                return new BigDecimal(value.toString());
            }
        } catch (NumberFormatException e) {
            throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + Decimal.LOGICAL_NAME, e);
        }
        if (schema() != null && schema().type() == Type.BYTES) {
            return Decimal.toLogical(Decimal.schema(scale), valueAsBytes());
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + Decimal.LOGICAL_NAME);
    }

    @Override
    public java.util.Date valueAsDate() throws DataException {
        Object value = valueAsObject();
        if (value == null) {
            return null;
        }
        if (value instanceof java.util.Date) {
            return (java.util.Date) value;
        }
        return Date.toLogical(Date.SCHEMA, valueAsInt());
    }

    @Override
    public java.util.Date valueAsTime() throws DataException {
        Object value = valueAsObject();
        if (value == null) {
            return null;
        }
        if (value instanceof java.util.Date) {
            return (java.util.Date) value;
        }
        return Time.toLogical(Time.SCHEMA, valueAsInt());
    }

    @Override
    public java.util.Date valueAsTimestamp() throws DataException {
        Object value = valueAsObject();
        if (value == null) {
            return null;
        }
        if (value instanceof java.util.Date) {
            return (java.util.Date) value;
        }
        return Timestamp.toLogical(Timestamp.SCHEMA, valueAsLong());
    }

    @Override
    public Object valueAsType(Type type) throws DataException {
        return valueAsType(type, true);
    }

    /**
     * Convert the header value to the desired type.
     *
     * @param type            the desired type; may not be null
     * @param allowNullResult true if this conversion can return a null value, or false otherwise
     * @return the converted value; never null
     * @throws DataException if the value could not be converted to the desired type
     */
    protected Object valueAsType(Type type, boolean allowNullResult) throws DataException {
        Object value = valueAsObject();
        if (value == null) {
            if (allowNullResult) {
                return null;
            }
            throw new DataException("Unable to convert a null value to a primitive");
        }
        switch (type) {
            case BYTES:
                if (value instanceof byte[]) {
                    return value;
                }
                break;
            case STRING:
                return value.toString();
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                if (value instanceof String) {
                    String str = value.toString().trim();
                    if (TRUE_WORD_LITERAL.equalsIgnoreCase(str) || TRUE_LETTER_LITERAL.equalsIgnoreCase(str)) {
                        return Boolean.TRUE;
                    }
                    if (FALSE_WORD_LITERAL.equalsIgnoreCase(str) || FALSE_LETTER_LITERAL.equalsIgnoreCase(str)) {
                        return Boolean.FALSE;
                    }
                }
                return asLong(value, type, null) == 1L ? Boolean.TRUE : Boolean.FALSE;
            case INT8:
                if (value instanceof Byte) {
                    return value;
                }
                return (byte) asLong(value, type, null);
            case INT16:
                if (value instanceof Short) {
                    return value;
                }
                return (short) asLong(value, type, null);
            case INT32:
                if (value instanceof Integer) {
                    return value;
                }
                return (int) asLong(value, type, null);
            case INT64:
                if (value instanceof Long) {
                    return value;
                }
                return asLong(value, type, null);
            case FLOAT32:
                if (value instanceof Float) {
                    return value;
                }
                return (float) asDouble(value, type, null);
            case FLOAT64:
                if (value instanceof Double) {
                    return value;
                }
                return asDouble(value, type, null);
            case ARRAY:
                if (value instanceof List) {
                    return value;
                }
                break;
            case MAP:
                if (value instanceof Map) {
                    return value;
                }
                break;
            case STRUCT:
                if (value instanceof Struct) {
                    return value;
                }
        }
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + type);
    }

    /**
     * Convert the specified value to the desired scalar value type.
     *
     * @param value the value to be converted; may not be null
     * @param type  the desired type; may not be null
     * @param error any previous error that should be included in an exception message; may be null
     * @return the long value after conversion; never null
     * @throws DataException if the value could not be converted to a long
     */
    protected long asLong(Object value, Type type, Throwable error) {
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
            Schema schema = schema();
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
        throw new DataException("Unable to convert " + value + " (" + value.getClass() + ") to " + type, error);
    }

    /**
     * Convert the specified value with the desired floating point type.
     *
     * @param value the value to be converted; may not be null
     * @param type  the desired type; may not be null
     * @param error any previous error that should be included in an exception message; may be null
     * @return the double value after conversion; never null
     * @throws DataException if the value could not be converted to a double
     */
    protected double asDouble(Object value, Type type, Throwable error) {
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
        return asLong(value, type, error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, schemaAndValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof Header) {
            Header that = (Header) obj;
            return Objects.equals(this.key, that.key()) && Objects.equals(this.schemaAndValue, that.schemaAndValue());
        }
        return false;
    }

    @Override
    public String toString() {
        return "ConnectHeader(key=" + key + ", value=" + valueAsString() + ", schema=" + schema() + ")";
    }
}
