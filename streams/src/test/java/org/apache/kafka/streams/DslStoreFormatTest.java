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
package org.apache.kafka.streams;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DslStoreFormatTest {

    @Test
    public void shouldResolvePlain() {
        assertEquals(DslStoreFormat.PLAIN, DslStoreFormat.of("PLAIN"));
    }

    @Test
    public void shouldResolveTimestamped() {
        assertEquals(DslStoreFormat.TIMESTAMPED, DslStoreFormat.of("TIMESTAMPED"));
    }

    @Test
    public void shouldResolveHeaders() {
        assertEquals(DslStoreFormat.HEADERS, DslStoreFormat.of("HEADERS"));
    }

    @Test
    public void shouldResolveHeadersCaseInsensitively() {
        assertEquals(DslStoreFormat.HEADERS, DslStoreFormat.of("headers"));
        assertEquals(DslStoreFormat.HEADERS, DslStoreFormat.of("Headers"));
        assertEquals(DslStoreFormat.HEADERS, DslStoreFormat.of("hEaDeRs"));
    }

    @Test
    public void shouldExposeHeadersNameField() {
        assertEquals("HEADERS", DslStoreFormat.HEADERS.name);
    }

    @Test
    public void shouldThrowOnInvalidInput() {
        assertThrows(IllegalArgumentException.class, () -> DslStoreFormat.of("not-a-format"));
    }

    @Test
    public void shouldThrowOnNullInput() {
        assertThrows(NullPointerException.class, () -> DslStoreFormat.of(null));
    }
}
