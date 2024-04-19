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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MultiFieldPathsTest {

    @Test void noNullInputs() {
        assertThrows(NullPointerException.class, () -> new MultiFieldPaths(null));
        assertThrows(NullPointerException.class, () -> new MultiFieldPaths(null, FieldSyntaxVersion.V2));
        assertThrows(NullPointerException.class, () -> new MultiFieldPaths(Collections.singletonList("tes"), null));
    }

    @Test void empty() {
        MultiFieldPaths empty = new MultiFieldPaths(Collections.emptyList());
        assertEquals(0, empty.size());
    }

    @Test void shouldFindFieldAndValueInMap() {
        Map<String, Object> foo = new HashMap<>();
        foo.put("bar", 42);
        foo.put("baz", null);
        Map<String, Object> map = new HashMap<>();
        map.put("foo", foo);

        MultiFieldPaths empty = new MultiFieldPaths(new ArrayList<>());
        assertEquals(0, empty.fieldAndValuesFrom(map).size());

        MultiFieldPaths paths = multiFieldPathsV2("foo.bar", "foo.baz");
        Map<SingleFieldPath, Map.Entry<String, Object>> fieldAndValues = paths.fieldAndValuesFrom(map);
        Map.Entry<String, Object> fooBar = fieldAndValues.get(pathV2("foo.bar"));
        assertEquals("bar", fooBar.getKey());
        assertEquals(42, fooBar.getValue());
        Map.Entry<String, Object> fooBaz = fieldAndValues.get(pathV2("foo.baz"));
        assertEquals("baz", fooBaz.getKey());
        assertNull(fooBaz.getValue());
    }

    @Test void shouldFindFieldAndValueInStruct() {
        SchemaBuilder bazSchema = SchemaBuilder.struct()
            .field("inner", Schema.STRING_SCHEMA);
        SchemaBuilder barSchema = SchemaBuilder.struct()
            .field("bar", Schema.INT32_SCHEMA)
            .field("baz", bazSchema.optional());
        Schema schema = SchemaBuilder.struct().field("foo", barSchema).build();
        Struct foo = new Struct(barSchema)
            .put("bar", 42)
            .put("baz", null);
        Struct struct = new Struct(schema).put("foo", foo);

        MultiFieldPaths paths = multiFieldPathsV2("foo.bar", "foo.baz");
        Map<SingleFieldPath, Map.Entry<Field, Object>> fieldAndValues = paths.fieldAndValuesFrom(struct);
        Map.Entry<Field, Object> fooBar = fieldAndValues.get(pathV2("foo.bar"));
        assertEquals("bar", fooBar.getKey().name());
        assertEquals(42, fooBar.getValue());
        Map.Entry<Field, Object> fooBaz = fieldAndValues.get(pathV2("foo.baz"));
        assertEquals("baz", fooBaz.getKey().name());
        assertNull(fooBaz.getValue());
    }

    private static MultiFieldPaths multiFieldPathsV2(String... paths) {
        List<SingleFieldPath> singlePaths = Stream.of(paths)
            .map(MultiFieldPathsTest::pathV2)
            .collect(Collectors.toList());
        return new MultiFieldPaths(singlePaths);
    }

    private static SingleFieldPath pathV2(String path) {
        return new SingleFieldPath(path, FieldSyntaxVersion.V2);
    }
}