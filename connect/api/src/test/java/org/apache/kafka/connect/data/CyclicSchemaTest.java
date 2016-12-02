/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.connect.data;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CyclicSchemaTest {

    private static final class LinkedListSchema extends CyclicSchema {

        final Schema underlying = SchemaBuilder.struct().name("LinkedList")
                .field("value", Schema.INT64_SCHEMA)
                .field("next", this)
                .optional()
                .build();

        @Override
        public Schema underlying() {
            return underlying;
        }

    }

    private static final class TreeSchema extends CyclicSchema {

        final Schema underlying = SchemaBuilder.struct().name("Tree")
                .field("value", Schema.INT64_SCHEMA)
                .field("children", SchemaBuilder.array(this).build())
                .build();

        @Override
        public Schema underlying() {
            return underlying;
        }

    }

    private static final class CyclicMapSchema extends CyclicSchema {

        final Schema underlying = SchemaBuilder.map(Schema.STRING_SCHEMA, this).name("CyclicMap").build();

        @Override
        public Schema underlying() {
            return underlying;
        }

    }

    @Test
    public void equals() {
        assertEquals(new LinkedListSchema(), new LinkedListSchema());
        assertEquals(new TreeSchema(), new TreeSchema());
        assertEquals(new CyclicMapSchema(), new CyclicMapSchema());
    }

}
