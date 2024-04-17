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
package org.apache.kafka.connect.transforms.field;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

class SingleFieldPathTest {

    @Test
    void shouldIncludeEmptyFieldNames() {
        assertArrayEquals(
            new String[] {"", "", ""},
            new SingleFieldPath("..", FieldSyntaxVersion.V2).path()
        );
        assertArrayEquals(
            new String[] {"foo", "", ""},
            new SingleFieldPath("foo..", FieldSyntaxVersion.V2).path()
        );
        assertArrayEquals(
            new String[] {"", "bar", ""},
            new SingleFieldPath(".bar.", FieldSyntaxVersion.V2).path()
        );
        assertArrayEquals(
            new String[] {"", "", "baz"},
            new SingleFieldPath("..baz", FieldSyntaxVersion.V2).path()
        );
    }

    @Test void shouldReturnNullFieldWhenFieldNotFound() {
        SchemaBuilder barSchema = SchemaBuilder.struct().field("bar", Schema.INT32_SCHEMA);
        Schema schema = SchemaBuilder.struct().field("foo", barSchema).build();

        assertNull(new SingleFieldPath("un.known", FieldSyntaxVersion.V2).fieldFrom(schema));
        assertNull(new SingleFieldPath("foo.unknown", FieldSyntaxVersion.V2).fieldFrom(schema));
        assertNull(new SingleFieldPath("unknown", FieldSyntaxVersion.V2).fieldFrom(schema));
    }

    @Test void shouldReturnNullValueWhenFieldNotFound() {
        SchemaBuilder barSchema = SchemaBuilder.struct().field("bar", Schema.INT32_SCHEMA);
        Schema schema = SchemaBuilder.struct().field("foo", barSchema).build();
        Struct struct = new Struct(schema).put("foo", new Struct(barSchema).put("bar", 42));

        assertNull(new SingleFieldPath("un.known", FieldSyntaxVersion.V2).valueFrom(struct));
        assertNull(new SingleFieldPath("foo.unknown", FieldSyntaxVersion.V2).valueFrom(struct));
        assertNull(new SingleFieldPath("unknown", FieldSyntaxVersion.V2).valueFrom(struct));
    }
}