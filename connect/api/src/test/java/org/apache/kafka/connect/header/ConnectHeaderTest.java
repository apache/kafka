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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class ConnectHeaderTest {

    private String key;
    private ConnectHeader header;

    @BeforeEach
    public void beforeEach() {
        key = "key";
        withString("value");
    }

    protected Header withValue(Schema schema, Object value) {
        header = new ConnectHeader(key, new SchemaAndValue(schema, value));
        return header;
    }

    protected Header withString(String value) {
        return withValue(Schema.STRING_SCHEMA, value);
    }

    @Test
    public void shouldAllowNullValues() {
        withValue(Schema.OPTIONAL_STRING_SCHEMA, null);
    }

    @Test
    public void shouldAllowNullSchema() {
        withValue(null, null);
        assertNull(header.schema());
        assertNull(header.value());

        String value = "non-null value";
        withValue(null, value);
        assertNull(header.schema());
        assertSame(value, header.value());
    }

    @Test
    public void shouldAllowNonNullValue() {
        String value = "non-null value";
        withValue(Schema.STRING_SCHEMA, value);
        assertSame(Schema.STRING_SCHEMA, header.schema());
        assertEquals(value, header.value());

        withValue(Schema.BOOLEAN_SCHEMA, true);
        assertSame(Schema.BOOLEAN_SCHEMA, header.schema());
        assertEquals(true, header.value());
    }

    @Test
    public void shouldGetSchemaFromStruct() {
        Schema schema = SchemaBuilder.struct()
                                     .field("foo", Schema.STRING_SCHEMA)
                                     .field("bar", Schema.INT32_SCHEMA)
                                     .build();
        Struct value = new Struct(schema);
        value.put("foo", "value");
        value.put("bar", 100);
        withValue(null, value);
        assertSame(schema, header.schema());
        assertSame(value, header.value());
    }

    @Test
    public void shouldSatisfyEquals() {
        String value = "non-null value";
        Header h1 = withValue(Schema.STRING_SCHEMA, value);
        assertSame(Schema.STRING_SCHEMA, header.schema());
        assertEquals(value, header.value());

        Header h2 = withValue(Schema.STRING_SCHEMA, value);
        assertEquals(h1, h2);
        assertEquals(h1.hashCode(), h2.hashCode());

        Header h3 = withValue(Schema.INT8_SCHEMA, 100);
        assertNotEquals(h3, h2);
    }
}