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
package org.apache.kafka.connect.transforms.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaUtilTest {

    @Test
    public void testCopyMapSchemaPreservesKeyAndValue() {
        // Given
        Schema source = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

        // When
        SchemaBuilder result = SchemaUtil.copySchemaBasics(source);

        // Then
        assertEquals(Schema.Type.MAP, result.type());
        assertEquals(source.keySchema(), result.keySchema());
        assertEquals(source.valueSchema(), result.valueSchema());
    }

    @Test
    public void testCopyMapSchemaWithComplexValue() {
        // Given
        Schema arrayValueSchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        Schema source = SchemaBuilder.map(Schema.STRING_SCHEMA, arrayValueSchema).build();

        // When
        SchemaBuilder result = SchemaUtil.copySchemaBasics(source);

        // Then
        assertEquals(Schema.Type.MAP, result.type());
        assertEquals(source.keySchema(), result.keySchema());
        assertEquals(source.valueSchema(), result.valueSchema());
        assertEquals(arrayValueSchema.valueSchema(), result.valueSchema().valueSchema());
    }

    @Test
    public void testCopyMapSchemaWithNonStringKey() {
        // Given
        Schema source = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build();

        // When
        SchemaBuilder result = SchemaUtil.copySchemaBasics(source);

        // Then
        assertEquals(Schema.Type.MAP, result.type());
        assertEquals(source.keySchema(), result.keySchema());
        assertEquals(source.valueSchema(), result.valueSchema());
    }
}