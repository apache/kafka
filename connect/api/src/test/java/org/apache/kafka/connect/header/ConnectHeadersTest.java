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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Headers.HeaderTransform;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConnectHeadersTest {

    private static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_DAYS;
    private static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_MILLIS;

    static {
        EPOCH_PLUS_TEN_THOUSAND_DAYS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH_PLUS_TEN_THOUSAND_DAYS.setTimeZone(TimeZone.getTimeZone("UTC"));
        EPOCH_PLUS_TEN_THOUSAND_DAYS.add(Calendar.DATE, 10000);

        EPOCH_PLUS_TEN_THOUSAND_MILLIS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH_PLUS_TEN_THOUSAND_MILLIS.setTimeZone(TimeZone.getTimeZone("UTC"));
        EPOCH_PLUS_TEN_THOUSAND_MILLIS.add(Calendar.MILLISECOND, 10000);
    }

    private ConnectHeaders headers;
    private Iterator<Header> iter;
    private String key;
    private String other;

    @Before
    public void beforeEach() {
        headers = new ConnectHeaders();
        key = "k1";
        other = "other key";
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullKey() {
        headers.add(null, "value", Schema.STRING_SCHEMA);
    }

    protected void populate(Headers headers) {
        headers.addBoolean(key, true);
        headers.addInt(key, 0);
        headers.addString(other, "other value");
        headers.addString(key, null);
        headers.addString(key, "third");
    }

    @Test
    public void shouldBeEquals() {
        Headers other = new ConnectHeaders();
        assertEquals(headers, other);
        assertEquals(headers.hashCode(), other.hashCode());

        populate(headers);
        assertNotEquals(headers, other);
        assertNotEquals(headers.hashCode(), other.hashCode());

        populate(other);
        assertEquals(headers, other);
        assertEquals(headers.hashCode(), other.hashCode());

        headers.addString("wow", "some value");
        assertNotEquals(headers, other);
    }

    @Test
    public void shouldHaveToString() {
        // empty
        assertNotNull(headers.toString());

        // not empty
        populate(headers);
        assertNotNull(headers.toString());
    }

    @Test
    public void shouldRetainLatestWhenEmpty() {
        headers.retainLatest(other);
        headers.retainLatest(key);
        headers.retainLatest();
        assertTrue(headers.isEmpty());
    }

    @Test
    public void shouldAddMultipleHeadersWithSameKeyAndRetainLatest() {
        populate(headers);

        Header header = headers.lastWithName(key);
        assertHeader(header, key, Schema.STRING_SCHEMA, "third");

        iter = headers.allWithName(key);
        assertNextHeader(iter, key, Schema.BOOLEAN_SCHEMA, true);
        assertNextHeader(iter, key, Schema.INT32_SCHEMA, 0);
        assertNextHeader(iter, key, Schema.OPTIONAL_STRING_SCHEMA, null);
        assertNextHeader(iter, key, Schema.STRING_SCHEMA, "third");
        assertNoNextHeader(iter);

        iter = headers.allWithName(other);
        assertOnlyNextHeader(iter, other, Schema.STRING_SCHEMA, "other value");

        headers.retainLatest(other);
        assertOnlySingleHeader(other, Schema.STRING_SCHEMA, "other value");

        headers.retainLatest(key);
        assertOnlySingleHeader(key, Schema.STRING_SCHEMA, "third");

        headers.retainLatest();
        assertOnlySingleHeader(other, Schema.STRING_SCHEMA, "other value");
        assertOnlySingleHeader(key, Schema.STRING_SCHEMA, "third");
    }

    @Test
    public void shouldAddHeadersWithPrimitiveValues() {
        String key = "k1";
        headers.addBoolean(key, true);
        headers.addByte(key, (byte) 0);
        headers.addShort(key, (short) 0);
        headers.addInt(key, 0);
        headers.addLong(key, 0);
        headers.addFloat(key, 1.0f);
        headers.addDouble(key, 1.0d);
        headers.addString(key, null);
        headers.addString(key, "third");
    }

    @Test
    public void shouldAddHeadersWithNullObjectValuesWithOptionalSchema() {
        addHeader("k1", Schema.BOOLEAN_SCHEMA, true);
        addHeader("k2", Schema.STRING_SCHEMA, "hello");
        addHeader("k3", Schema.OPTIONAL_STRING_SCHEMA, null);
    }

    @Test
    public void shouldNotAddHeadersWithNullObjectValuesWithNonOptionalSchema() {
        attemptAndFailToAddHeader("k1", Schema.BOOLEAN_SCHEMA, null);
        attemptAndFailToAddHeader("k2", Schema.STRING_SCHEMA, null);
    }

    @Test
    public void shouldNotAddHeadersWithObjectValuesAndMismatchedSchema() {
        attemptAndFailToAddHeader("k1", Schema.BOOLEAN_SCHEMA, "wrong");
        attemptAndFailToAddHeader("k2", Schema.OPTIONAL_STRING_SCHEMA, 0L);
    }

    @Test
    public void shouldRemoveAllHeadersWithSameKeyWhenEmpty() {
        headers.remove(key);
        assertNoHeaderWithKey(key);
    }

    @Test
    public void shouldRemoveAllHeadersWithSameKey() {
        populate(headers);

        iter = headers.allWithName(key);
        assertContainsHeader(key, Schema.BOOLEAN_SCHEMA, true);
        assertContainsHeader(key, Schema.INT32_SCHEMA, 0);
        assertContainsHeader(key, Schema.STRING_SCHEMA, "third");
        assertOnlySingleHeader(other, Schema.STRING_SCHEMA, "other value");

        headers.remove(key);
        assertNoHeaderWithKey(key);
        assertOnlySingleHeader(other, Schema.STRING_SCHEMA, "other value");
    }

    @Test
    public void shouldRemoveAllHeaders() {
        populate(headers);

        iter = headers.allWithName(key);
        assertContainsHeader(key, Schema.BOOLEAN_SCHEMA, true);
        assertContainsHeader(key, Schema.INT32_SCHEMA, 0);
        assertContainsHeader(key, Schema.STRING_SCHEMA, "third");
        assertOnlySingleHeader(other, Schema.STRING_SCHEMA, "other value");

        headers.clear();
        assertNoHeaderWithKey(key);
        assertNoHeaderWithKey(other);
        assertEquals(0, headers.size());
        assertTrue(headers.isEmpty());
    }

    @Test
    public void shouldTransformHeadersWhenEmpty() {
        headers.apply(appendToKey("-suffix"));
        headers.apply(key, appendToKey("-suffix"));
        assertTrue(headers.isEmpty());
    }

    @Test
    public void shouldTransformHeaders() {
        populate(headers);

        iter = headers.allWithName(key);
        assertNextHeader(iter, key, Schema.BOOLEAN_SCHEMA, true);
        assertNextHeader(iter, key, Schema.INT32_SCHEMA, 0);
        assertNextHeader(iter, key, Schema.OPTIONAL_STRING_SCHEMA, null);
        assertNextHeader(iter, key, Schema.STRING_SCHEMA, "third");
        assertNoNextHeader(iter);

        iter = headers.allWithName(other);
        assertOnlyNextHeader(iter, other, Schema.STRING_SCHEMA, "other value");

        // Transform the headers
        assertEquals(5, headers.size());
        headers.apply(appendToKey("-suffix"));
        assertEquals(5, headers.size());

        assertNoHeaderWithKey(key);
        assertNoHeaderWithKey(other);

        String altKey = key + "-suffix";
        iter = headers.allWithName(altKey);
        assertNextHeader(iter, altKey, Schema.BOOLEAN_SCHEMA, true);
        assertNextHeader(iter, altKey, Schema.INT32_SCHEMA, 0);
        assertNextHeader(iter, altKey, Schema.OPTIONAL_STRING_SCHEMA, null);
        assertNextHeader(iter, altKey, Schema.STRING_SCHEMA, "third");
        assertNoNextHeader(iter);

        iter = headers.allWithName(other + "-suffix");
        assertOnlyNextHeader(iter, other + "-suffix", Schema.STRING_SCHEMA, "other value");
    }

    @Test
    public void shouldTransformHeadersWithKey() {
        populate(headers);

        iter = headers.allWithName(key);
        assertNextHeader(iter, key, Schema.BOOLEAN_SCHEMA, true);
        assertNextHeader(iter, key, Schema.INT32_SCHEMA, 0);
        assertNextHeader(iter, key, Schema.OPTIONAL_STRING_SCHEMA, null);
        assertNextHeader(iter, key, Schema.STRING_SCHEMA, "third");
        assertNoNextHeader(iter);

        iter = headers.allWithName(other);
        assertOnlyNextHeader(iter, other, Schema.STRING_SCHEMA, "other value");

        // Transform the headers
        assertEquals(5, headers.size());
        headers.apply(key, appendToKey("-suffix"));
        assertEquals(5, headers.size());

        assertNoHeaderWithKey(key);

        String altKey = key + "-suffix";
        iter = headers.allWithName(altKey);
        assertNextHeader(iter, altKey, Schema.BOOLEAN_SCHEMA, true);
        assertNextHeader(iter, altKey, Schema.INT32_SCHEMA, 0);
        assertNextHeader(iter, altKey, Schema.OPTIONAL_STRING_SCHEMA, null);
        assertNextHeader(iter, altKey, Schema.STRING_SCHEMA, "third");
        assertNoNextHeader(iter);

        iter = headers.allWithName(other);
        assertOnlyNextHeader(iter, other, Schema.STRING_SCHEMA, "other value");
    }

    @Test
    public void shouldTransformAndRemoveHeaders() {
        populate(headers);

        iter = headers.allWithName(key);
        assertNextHeader(iter, key, Schema.BOOLEAN_SCHEMA, true);
        assertNextHeader(iter, key, Schema.INT32_SCHEMA, 0);
        assertNextHeader(iter, key, Schema.OPTIONAL_STRING_SCHEMA, null);
        assertNextHeader(iter, key, Schema.STRING_SCHEMA, "third");
        assertNoNextHeader(iter);

        iter = headers.allWithName(other);
        assertOnlyNextHeader(iter, other, Schema.STRING_SCHEMA, "other value");

        // Transform the headers
        assertEquals(5, headers.size());
        headers.apply(key, removeHeadersOfType(Type.STRING));
        assertEquals(3, headers.size());

        iter = headers.allWithName(key);
        assertNextHeader(iter, key, Schema.BOOLEAN_SCHEMA, true);
        assertNextHeader(iter, key, Schema.INT32_SCHEMA, 0);
        assertNoNextHeader(iter);

        assertHeader(headers.lastWithName(key), key, Schema.INT32_SCHEMA, 0);

        iter = headers.allWithName(other);
        assertOnlyNextHeader(iter, other, Schema.STRING_SCHEMA, "other value");

        // Transform the headers
        assertEquals(3, headers.size());
        headers.apply(removeHeadersOfType(Type.STRING));
        assertEquals(2, headers.size());

        assertNoHeaderWithKey(other);

        iter = headers.allWithName(key);
        assertNextHeader(iter, key, Schema.BOOLEAN_SCHEMA, true);
        assertNextHeader(iter, key, Schema.INT32_SCHEMA, 0);
        assertNoNextHeader(iter);
    }

    protected HeaderTransform appendToKey(final String suffix) {
        return new HeaderTransform() {
            @Override
            public Header apply(Header header) {
                return header.rename(header.key() + suffix);
            }
        };
    }

    protected HeaderTransform removeHeadersOfType(final Type type) {
        return new HeaderTransform() {
            @Override
            public Header apply(Header header) {
                Schema schema = header.schema();
                if (schema != null && schema.type() == type) {
                    return null;
                }
                return header;
            }
        };
    }

    @Test
    public void shouldValidateBuildInTypes() {
        assertSchemaMatches(Schema.OPTIONAL_BOOLEAN_SCHEMA, null);
        assertSchemaMatches(Schema.OPTIONAL_BYTES_SCHEMA, null);
        assertSchemaMatches(Schema.OPTIONAL_INT8_SCHEMA, null);
        assertSchemaMatches(Schema.OPTIONAL_INT16_SCHEMA, null);
        assertSchemaMatches(Schema.OPTIONAL_INT32_SCHEMA, null);
        assertSchemaMatches(Schema.OPTIONAL_INT64_SCHEMA, null);
        assertSchemaMatches(Schema.OPTIONAL_FLOAT32_SCHEMA, null);
        assertSchemaMatches(Schema.OPTIONAL_FLOAT64_SCHEMA, null);
        assertSchemaMatches(Schema.OPTIONAL_STRING_SCHEMA, null);
        assertSchemaMatches(Schema.BOOLEAN_SCHEMA, true);
        assertSchemaMatches(Schema.BYTES_SCHEMA, new byte[]{});
        assertSchemaMatches(Schema.INT8_SCHEMA, (byte) 0);
        assertSchemaMatches(Schema.INT16_SCHEMA, (short) 0);
        assertSchemaMatches(Schema.INT32_SCHEMA, 0);
        assertSchemaMatches(Schema.INT64_SCHEMA, 0L);
        assertSchemaMatches(Schema.FLOAT32_SCHEMA, 1.0f);
        assertSchemaMatches(Schema.FLOAT64_SCHEMA, 1.0d);
        assertSchemaMatches(Schema.STRING_SCHEMA, "value");
        assertSchemaMatches(SchemaBuilder.array(Schema.STRING_SCHEMA), new ArrayList<String>());
        assertSchemaMatches(SchemaBuilder.array(Schema.STRING_SCHEMA), Collections.singletonList("value"));
        assertSchemaMatches(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA), new HashMap<String, Integer>());
        assertSchemaMatches(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA), Collections.singletonMap("a", 0));
        Schema emptyStructSchema = SchemaBuilder.struct();
        assertSchemaMatches(emptyStructSchema, new Struct(emptyStructSchema));
        Schema structSchema = SchemaBuilder.struct().field("foo", Schema.OPTIONAL_BOOLEAN_SCHEMA).field("bar", Schema.STRING_SCHEMA)
                                           .schema();
        assertSchemaMatches(structSchema, new Struct(structSchema).put("foo", true).put("bar", "v"));
    }

    @Test
    public void shouldValidateLogicalTypes() {
        assertSchemaMatches(Decimal.schema(3), new BigDecimal(100.00));
        assertSchemaMatches(Time.SCHEMA, new java.util.Date());
        assertSchemaMatches(Date.SCHEMA, new java.util.Date());
        assertSchemaMatches(Timestamp.SCHEMA, new java.util.Date());
    }

    @Test
    public void shouldNotValidateNullValuesWithBuiltInTypes() {
        assertSchemaDoesNotMatch(Schema.BOOLEAN_SCHEMA, null);
        assertSchemaDoesNotMatch(Schema.BYTES_SCHEMA, null);
        assertSchemaDoesNotMatch(Schema.INT8_SCHEMA, null);
        assertSchemaDoesNotMatch(Schema.INT16_SCHEMA, null);
        assertSchemaDoesNotMatch(Schema.INT32_SCHEMA, null);
        assertSchemaDoesNotMatch(Schema.INT64_SCHEMA, null);
        assertSchemaDoesNotMatch(Schema.FLOAT32_SCHEMA, null);
        assertSchemaDoesNotMatch(Schema.FLOAT64_SCHEMA, null);
        assertSchemaDoesNotMatch(Schema.STRING_SCHEMA, null);
        assertSchemaDoesNotMatch(SchemaBuilder.array(Schema.STRING_SCHEMA), null);
        assertSchemaDoesNotMatch(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA), null);
        assertSchemaDoesNotMatch(SchemaBuilder.struct(), null);
    }

    @Test
    public void shouldNotValidateMismatchedValuesWithBuiltInTypes() {
        assertSchemaDoesNotMatch(Schema.BOOLEAN_SCHEMA, 0L);
        assertSchemaDoesNotMatch(Schema.BYTES_SCHEMA, "oops");
        assertSchemaDoesNotMatch(Schema.INT8_SCHEMA, 1.0f);
        assertSchemaDoesNotMatch(Schema.INT16_SCHEMA, 1.0f);
        assertSchemaDoesNotMatch(Schema.INT32_SCHEMA, 0L);
        assertSchemaDoesNotMatch(Schema.INT64_SCHEMA, 1.0f);
        assertSchemaDoesNotMatch(Schema.FLOAT32_SCHEMA, 1L);
        assertSchemaDoesNotMatch(Schema.FLOAT64_SCHEMA, 1L);
        assertSchemaDoesNotMatch(Schema.STRING_SCHEMA, true);
        assertSchemaDoesNotMatch(SchemaBuilder.array(Schema.STRING_SCHEMA), "value");
        assertSchemaDoesNotMatch(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA), "value");
        assertSchemaDoesNotMatch(SchemaBuilder.struct(), new ArrayList<String>());
    }

    @Test
    public void shouldAddDate() {
        java.util.Date dateObj = EPOCH_PLUS_TEN_THOUSAND_DAYS.getTime();
        int days = Date.fromLogical(Date.SCHEMA, dateObj);
        headers.addDate(key, dateObj);
        Header header = headers.lastWithName(key);
        assertEquals(days, (int) Values.convertToInteger(header.schema(), header.value()));
        assertSame(dateObj, Values.convertToDate(header.schema(), header.value()));

        headers.addInt(other, days);
        header = headers.lastWithName(other);
        assertEquals(days, (int) Values.convertToInteger(header.schema(), header.value()));
        assertEquals(dateObj, Values.convertToDate(header.schema(), header.value()));
    }

    @Test
    public void shouldAddTime() {
        java.util.Date dateObj = EPOCH_PLUS_TEN_THOUSAND_MILLIS.getTime();
        long millis = Time.fromLogical(Time.SCHEMA, dateObj);
        headers.addTime(key, dateObj);
        Header header = headers.lastWithName(key);
        assertEquals(millis, (long) Values.convertToLong(header.schema(), header.value()));
        assertSame(dateObj, Values.convertToTime(header.schema(), header.value()));

        headers.addLong(other, millis);
        header = headers.lastWithName(other);
        assertEquals(millis, (long) Values.convertToLong(header.schema(), header.value()));
        assertEquals(dateObj, Values.convertToTime(header.schema(), header.value()));
    }

    @Test
    public void shouldAddTimestamp() {
        java.util.Date dateObj = EPOCH_PLUS_TEN_THOUSAND_MILLIS.getTime();
        long millis = Timestamp.fromLogical(Timestamp.SCHEMA, dateObj);
        headers.addTimestamp(key, dateObj);
        Header header = headers.lastWithName(key);
        assertEquals(millis, (long) Values.convertToLong(header.schema(), header.value()));
        assertSame(dateObj, Values.convertToTimestamp(header.schema(), header.value()));

        headers.addLong(other, millis);
        header = headers.lastWithName(other);
        assertEquals(millis, (long) Values.convertToLong(header.schema(), header.value()));
        assertEquals(dateObj, Values.convertToTimestamp(header.schema(), header.value()));
    }

    @Test
    public void shouldAddDecimal() {
        BigDecimal value = new BigDecimal("3.038573478e+3");
        headers.addDecimal(key, value);
        Header header = headers.lastWithName(key);
        assertEquals(value.doubleValue(), Values.convertToDouble(header.schema(), header.value()), 0.00001d);
        assertEquals(value, Values.convertToDecimal(header.schema(), header.value(), value.scale()));

        value = value.setScale(3, RoundingMode.DOWN);
        BigDecimal decimal = Values.convertToDecimal(header.schema(), header.value(), value.scale());
        assertEquals(value, decimal.setScale(value.scale(), RoundingMode.DOWN));
    }

    @Test
    public void shouldDuplicateAndAlwaysReturnEquivalentButDifferentObject() {
        assertEquals(headers, headers.duplicate());
        assertNotSame(headers, headers.duplicate());
    }

    protected void assertSchemaMatches(Schema schema, Object value) {
        headers.checkSchemaMatches(new SchemaAndValue(schema.schema(), value));
    }

    protected void assertSchemaDoesNotMatch(Schema schema, Object value) {
        try {
            assertSchemaMatches(schema, value);
            fail("Should have failed to validate value '" + value + "' and schema: " + schema);
        } catch (DataException e) {
            // expected
        }
    }

    protected void attemptAndFailToAddHeader(String key, Schema schema, Object value) {
        try {
            headers.add(key, value, schema);
            fail("Should have failed to add header with key '" + key + "', value '" + value + "', and schema: " + schema);
        } catch (DataException e) {
            // expected
        }
    }

    protected void addHeader(String key, Schema schema, Object value) {
        headers.add(key, value, schema);
        Header header = headers.lastWithName(key);
        assertNotNull(header);
        assertHeader(header, key, schema, value);
    }

    protected void assertNoHeaderWithKey(String key) {
        assertNoNextHeader(headers.allWithName(key));
    }

    protected void assertContainsHeader(String key, Schema schema, Object value) {
        Header expected = new ConnectHeader(key, new SchemaAndValue(schema, value));
        Iterator<Header> iter = headers.allWithName(key);
        while (iter.hasNext()) {
            Header header = iter.next();
            if (header.equals(expected))
                return;
        }
        fail("Should have found header " + expected);
    }

    protected void assertOnlySingleHeader(String key, Schema schema, Object value) {
        assertOnlyNextHeader(headers.allWithName(key), key, schema, value);
    }

    protected void assertOnlyNextHeader(Iterator<Header> iter, String key, Schema schema, Object value) {
        assertNextHeader(iter, key, schema, value);
        assertNoNextHeader(iter);
    }

    protected void assertNextHeader(Iterator<Header> iter, String key, Schema schema, Object value) {
        Header header = iter.next();
        assertHeader(header, key, schema, value);
    }

    protected void assertNoNextHeader(Iterator<Header> iter) {
        assertFalse(iter.hasNext());
    }

    protected void assertHeader(Header header, String key, Schema schema, Object value) {
        assertNotNull(header);
        assertSame(schema, header.schema());
        assertSame(value, header.value());
    }
}
