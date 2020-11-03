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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.junit.Assert;

import static org.junit.Assert.assertTrue;

public final class SchemaTestUtils {

    private SchemaTestUtils() {
    }

    static void assertEquals(Type lhs, Type rhs) {
        if (lhs instanceof Schema) {
            assertTrue(rhs instanceof Schema);
            Schema lhsSchema = (Schema) lhs;
            Schema rhsSchema = (Schema) rhs;
            Assert.assertEquals(lhsSchema.numFields(), rhsSchema.numFields());
            int fieldIndex = 0;
            for (BoundField f : lhsSchema.fields()) {
                Field previousField = f.def;
                Field currentField = rhsSchema.fields()[fieldIndex++].def;
                Assert.assertEquals(previousField.name, currentField.name);
                assertEquals(previousField.type, currentField.type);
                // hasDefaultValue and defaultValue are not used by automatic protocol so we don't need to check them.
            }
        } else if (lhs instanceof ArrayOf) {
            assertTrue(rhs instanceof ArrayOf);
            lhs.arrayElementType().ifPresent(lhsType ->
                rhs.arrayElementType().ifPresent(rhsType -> assertEquals(lhsType, rhsType)));
        } else Assert.assertEquals(lhs, rhs);
    }
}