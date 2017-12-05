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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class ConnectHeaderTest {

    private static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_DAYS;
    private static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_MILLIS;
    private static final double TOLERANCE = 0.00001d;

    static {
        EPOCH_PLUS_TEN_THOUSAND_DAYS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH_PLUS_TEN_THOUSAND_DAYS.setTimeZone(TimeZone.getTimeZone("UTC"));
        EPOCH_PLUS_TEN_THOUSAND_DAYS.add(Calendar.DATE, 10000);

        EPOCH_PLUS_TEN_THOUSAND_MILLIS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH_PLUS_TEN_THOUSAND_MILLIS.setTimeZone(TimeZone.getTimeZone("UTC"));
        EPOCH_PLUS_TEN_THOUSAND_MILLIS.add(Calendar.MILLISECOND, 10000);
    }

    private String key;
    private ConnectHeader header;

    @Before
    public void beforeEach() {
        key = "key";
        withString("value");
    }

    protected void withValue(Schema schema, Object value) {
        header = new ConnectHeader(key, new SchemaAndValue(schema, value));
    }

    protected void withString(String value) {
        withValue(Schema.STRING_SCHEMA, value);
    }

    @Test
    public void shouldConvertFromStringToBoolean() {
        withString("1");
        assertEquals(true, header.valueAsBoolean());
        assertEquals(true, header.valueAsType(Type.BOOLEAN));

        withString("t");
        assertEquals(true, header.valueAsBoolean());
        assertEquals(true, header.valueAsType(Type.BOOLEAN));

        withString("true");
        assertEquals(true, header.valueAsBoolean());
        assertEquals(true, header.valueAsType(Type.BOOLEAN));

        withString("  trUE  ");
        assertEquals(true, header.valueAsBoolean());
        assertEquals(true, header.valueAsType(Type.BOOLEAN));

        withString("  FalSE  ");
        assertEquals(false, header.valueAsBoolean());
        assertEquals(false, header.valueAsType(Type.BOOLEAN));

        withString("false");
        assertEquals(false, header.valueAsBoolean());
        assertEquals(false, header.valueAsType(Type.BOOLEAN));

        withString("f");
        assertEquals(false, header.valueAsBoolean());
        assertEquals(false, header.valueAsType(Type.BOOLEAN));

        withString("10");
        assertEquals(false, header.valueAsBoolean());
        assertEquals(false, header.valueAsType(Type.BOOLEAN));
    }

    @Test
    public void shouldConvertFromStringToScalar() {
        withString("10");
        assertEquals((byte) 10, header.valueAsByte());
        assertEquals((short) 10, header.valueAsShort());
        assertEquals(10, header.valueAsInt());
        assertEquals(10L, header.valueAsLong());
        assertEquals(10.0f, header.valueAsFloat(), TOLERANCE);
        assertEquals(10.0d, header.valueAsDouble(), TOLERANCE);

        assertEquals((byte) 10, header.valueAsType(Type.INT8));
        assertEquals((short) 10, header.valueAsType(Type.INT16));
        assertEquals(10, header.valueAsType(Type.INT32));
        assertEquals(10L, header.valueAsType(Type.INT64));
        assertEquals(10.0f, (float) header.valueAsType(Type.FLOAT32), TOLERANCE);
        assertEquals(10.0d, (double) header.valueAsType(Type.FLOAT64), TOLERANCE);

        withString("10.9");
        assertEquals((byte) 10, header.valueAsByte());
        assertEquals((short) 10, header.valueAsShort());
        assertEquals(10, header.valueAsInt());
        assertEquals(10L, header.valueAsLong());
        assertEquals(10.9f, header.valueAsFloat(), TOLERANCE);
        assertEquals(10.9d, header.valueAsDouble(), TOLERANCE);

        assertEquals((byte) 10, header.valueAsType(Type.INT8));
        assertEquals((short) 10, header.valueAsType(Type.INT16));
        assertEquals(10, header.valueAsType(Type.INT32));
        assertEquals(10L, header.valueAsType(Type.INT64));
        assertEquals(10.9f, (float) header.valueAsType(Type.FLOAT32), TOLERANCE);
        assertEquals(10.9d, (double) header.valueAsType(Type.FLOAT64), TOLERANCE);
    }

    @Test(expected = DataException.class)
    public void shouldNotConvertNullValueToByte() {
        withValue(null, null);
        header.valueAsByte();
    }

    @Test(expected = DataException.class)
    public void shouldNotConvertNullValueToShort() {
        withValue(null, null);
        header.valueAsShort();
    }

    @Test(expected = DataException.class)
    public void shouldNotConvertNullValueToInt() {
        withValue(null, null);
        header.valueAsInt();
    }

    @Test(expected = DataException.class)
    public void shouldNotConvertNullValueToLong() {
        withValue(null, null);
        header.valueAsLong();
    }

    @Test(expected = DataException.class)
    public void shouldNotConvertNullValueToDouble() {
        withValue(null, null);
        header.valueAsDouble();
    }

    @Test(expected = DataException.class)
    public void shouldNotConvertNullValueToFloat() {
        withValue(null, null);
        header.valueAsFloat();
    }

    @Test(expected = DataException.class)
    public void shouldNotConvertNullValueToBoolean() {
        withValue(null, null);
        header.valueAsBoolean();
    }

    @Test
    public void shouldConvertNullValueToEveryTypeReturningObject() {
        withValue(null, null);
        for (Type type : Type.values()) {
            assertNull(header.valueAsType(type));
        }
    }

    @Test
    public void shouldNotConvertListToAnythingButList() {
        Schema schema = SchemaBuilder.array(Schema.STRING_SCHEMA);
        List<String> list = new ArrayList<>();
        assertValueAsTypeOnlyForMatchingOrStringType(schema, list);
        assertSame(list, header.valueAsList());

        list = Collections.singletonList("value");
        assertValueAsTypeOnlyForMatchingOrStringType(schema, list);
        assertSame(list, header.valueAsList());
    }

    @Test
    public void shouldNotConvertMapToAnythingButMap() {
        Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA);
        Map<String, Integer> map = new HashMap<String, Integer>();
        assertValueAsTypeOnlyForMatchingOrStringType(schema, map);
        assertSame(map, header.valueAsMap());

        map = Collections.singletonMap("value", 10);
        assertValueAsTypeOnlyForMatchingOrStringType(schema, map);
        assertSame(map, header.valueAsMap());
    }

    @Test
    public void shouldNotConvertStructToAnythingButStruct() {
        Schema schema = SchemaBuilder.struct(); // empty
        Struct struct = new Struct(schema);
        assertValueAsTypeOnlyForMatchingOrStringType(schema, struct);

        schema = SchemaBuilder.struct().field("foo", Schema.OPTIONAL_BOOLEAN_SCHEMA).field("bar", Schema.STRING_SCHEMA).schema();
        struct = new Struct(schema).put("foo", true).put("bar", "v");
        assertValueAsTypeOnlyForMatchingOrStringType(schema, struct);
        assertSame(struct, header.valueAsStruct());
    }

    @Test
    public void shouldConvertToDecimal() {
        withValue(null, null);
        assertEquals(null, header.valueAsDecimal(4, null));

        withValue(Schema.INT8_SCHEMA, (byte) 10);
        assertEquals(10, header.valueAsDecimal(4, null).intValueExact());

        withValue(Schema.INT16_SCHEMA, (short) 10);
        assertEquals(10, header.valueAsDecimal(4, null).intValueExact());

        withValue(Schema.INT32_SCHEMA, 300);
        assertEquals(300, header.valueAsDecimal(4, null).intValueExact());

        withValue(Schema.INT64_SCHEMA, 300L);
        assertEquals(300L, header.valueAsDecimal(4, null).longValueExact());

        withValue(Schema.BOOLEAN_SCHEMA, Boolean.TRUE);
        assertEquals(1, header.valueAsDecimal(4, null).intValueExact());
        withValue(Schema.BOOLEAN_SCHEMA, Boolean.FALSE);
        assertEquals(0, header.valueAsDecimal(4, null).intValueExact());

        BigDecimal value = new BigDecimal("3.038573478e+3");
        withValue(Schema.STRING_SCHEMA, value);
        assertEquals(value, header.valueAsDecimal(value.scale(), null));
    }

    @Test
    public void shouldConvertToDate() {
        withValue(null, null);
        assertEquals(null, header.valueAsDate());

        java.util.Date dateObj = EPOCH_PLUS_TEN_THOUSAND_DAYS.getTime();
        int days = Date.fromLogical(Date.SCHEMA, dateObj);
        assertEquals(10000, days);
        withValue(Schema.INT32_SCHEMA, days);
        assertEquals(dateObj, header.valueAsDate());
    }

    @Test
    public void shouldConvertToTime() {
        withValue(null, null);
        assertEquals(null, header.valueAsTime());

        java.util.Date dateObj = EPOCH_PLUS_TEN_THOUSAND_MILLIS.getTime();
        int millis = Time.fromLogical(Time.SCHEMA, dateObj);
        assertEquals(10000, millis);
        withValue(Schema.INT32_SCHEMA, millis);
        assertEquals(dateObj, header.valueAsTime());
    }

    @Test
    public void shouldConvertToTimestamp() {
        withValue(null, null);
        assertEquals(null, header.valueAsTimestamp());

        java.util.Date dateObj = EPOCH_PLUS_TEN_THOUSAND_MILLIS.getTime();
        long millis = Timestamp.fromLogical(Timestamp.SCHEMA, dateObj);
        assertEquals(10000, millis);
        withValue(Schema.INT32_SCHEMA, millis);
        assertEquals(dateObj, header.valueAsTimestamp());
    }

    protected void assertValueAsTypeOnlyForMatchingOrStringType(Schema schema, Object value) {
        withValue(schema, value);
        for (Type type : Type.values()) {
            if (type == schema.type()) {
                assertSame(value, header.valueAsType(type));
            } else if (type == Type.STRING) {
                assertEquals(value.toString(), header.valueAsType(type));
            } else {
                try {
                    header.valueAsType(type);
                    fail("Should not have allowed converting a header " + schema.type() + " value to " + type);
                } catch (DataException e) {
                    // expected
                }
            }
        }
    }
}