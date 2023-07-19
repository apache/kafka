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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class FieldTest {

    @Test
    public void testEquality() {
        Field field1 = new Field("name", 0, Schema.INT8_SCHEMA);
        Field field2 = new Field("name", 0, Schema.INT8_SCHEMA);
        Field differentName = new Field("name2", 0, Schema.INT8_SCHEMA);
        Field differentIndex = new Field("name", 1, Schema.INT8_SCHEMA);
        Field differentSchema = new Field("name", 0, Schema.INT16_SCHEMA);

        assertEquals(field1, field2);
        assertNotEquals(field1, differentName);
        assertNotEquals(field1, differentIndex);
        assertNotEquals(field1, differentSchema);
    }
}
