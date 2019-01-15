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

package org.apache.kafka.message;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class MessageGeneratorTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testCapitalizeFirst() throws Exception {
        assertEquals("", MessageGenerator.capitalizeFirst(""));
        assertEquals("AbC", MessageGenerator.capitalizeFirst("abC"));
    }

    @Test
    public void testLowerCaseFirst() throws Exception {
        assertEquals("", MessageGenerator.lowerCaseFirst(""));
        assertEquals("fORTRAN", MessageGenerator.lowerCaseFirst("FORTRAN"));
        assertEquals("java", MessageGenerator.lowerCaseFirst("java"));
    }

    @Test
    public void testFirstIsCapitalized() throws Exception {
        assertFalse(MessageGenerator.firstIsCapitalized(""));
        assertTrue(MessageGenerator.firstIsCapitalized("FORTRAN"));
        assertFalse(MessageGenerator.firstIsCapitalized("java"));
    }

    @Test
    public void testToSnakeCase() throws Exception {
        assertEquals("", MessageGenerator.toSnakeCase(""));
        assertEquals("foo_bar_baz", MessageGenerator.toSnakeCase("FooBarBaz"));
        assertEquals("foo_bar_baz", MessageGenerator.toSnakeCase("fooBarBaz"));
        assertEquals("fortran", MessageGenerator.toSnakeCase("FORTRAN"));
    }
}
