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
package org.apache.kafka.common.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PrimitiveRefTest {

    @Test
    public void testIntRef() {
        PrimitiveRef.IntRef ref = PrimitiveRef.ofInt(3);
        assertEquals(3, ref.value++);
        assertEquals(4, ref.value);
        assertEquals(5, ++ref.value);
        assertEquals(5, ref.value);
    }

    @Test
    public void testLongRef() {
        PrimitiveRef.LongRef ref = PrimitiveRef.ofLong(5L);
        assertEquals(5L, ref.value++);
        assertEquals(6L, ref.value);
        assertEquals(7L, ++ref.value);
        assertEquals(7L, ref.value);
    }

}
