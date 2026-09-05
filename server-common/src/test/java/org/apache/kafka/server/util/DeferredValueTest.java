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
package org.apache.kafka.server.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
public class DeferredValueTest {

    @Test
    public void testConstructWithValue() {
        DeferredValue<String> deferred = DeferredValue.completed("hello");
        assertTrue(deferred.isDone());
        assertEquals("hello", deferred.getNow());
    }

    @Test
    public void testGetOrThrow() throws Exception {
        DeferredValue<String> deferred = DeferredValue.incomplete("default");

        assertEquals("default", deferred.getNow());
        assertThrows(IllegalStateException.class, deferred::getOrThrow);

        deferred.complete("hello");
        assertEquals("hello", deferred.getOrThrow());
    }

    @Test
    public void testComplete() {
        DeferredValue<String> deferred = DeferredValue.incomplete("default");
        assertFalse(deferred.isDone());
        assertEquals("default", deferred.getNow());

        deferred.complete("value");
        assertTrue(deferred.isDone());
        assertEquals("value", deferred.getNow());
    }

    @Test
    public void testSecondCompleteIsIgnored() {
        DeferredValue<String> deferred = DeferredValue.incomplete("default");
        deferred.complete("first");
        deferred.complete("second");
        assertEquals("first", deferred.getNow());
    }
}
