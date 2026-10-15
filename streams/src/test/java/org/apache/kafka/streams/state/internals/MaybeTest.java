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
package org.apache.kafka.streams.state.internals;

import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class MaybeTest {
    @Test
    public void shouldReturnDefinedValue() {
        assertNull(Maybe.defined(null).getNullableValue());
        assertEquals("ASDF", Maybe.defined("ASDF").getNullableValue());
    }

    @Test
    public void shouldAnswerIsDefined() {
        assertTrue(Maybe.defined(null).isDefined());
        assertTrue(Maybe.defined("ASDF").isDefined());
        assertFalse(Maybe.undefined().isDefined());
    }

    @Test
    public void shouldThrowOnGetUndefinedValue() {
        final Maybe<Object> undefined = Maybe.undefined();
        try {
            undefined.getNullableValue();
            fail();
        } catch (final NoSuchElementException e) {
            // no assertion necessary
        }
    }

    @Test
    public void shouldUpholdEqualityCorrectness() {
        assertTrue(Maybe.undefined().equals(Maybe.undefined()));
        assertTrue(Maybe.defined(null).equals(Maybe.defined(null)));
        assertTrue(Maybe.defined("q").equals(Maybe.defined("q")));

        assertFalse(Maybe.undefined().equals(Maybe.defined(null)));
        assertFalse(Maybe.undefined().equals(Maybe.defined("x")));

        assertFalse(Maybe.defined(null).equals(Maybe.undefined()));
        assertFalse(Maybe.defined(null).equals(Maybe.defined("x")));

        assertFalse(Maybe.defined("a").equals(Maybe.undefined()));
        assertFalse(Maybe.defined("a").equals(Maybe.defined(null)));
        assertFalse(Maybe.defined("a").equals(Maybe.defined("b")));
    }

    @Test
    public void shouldUpholdHashCodeCorrectness() {
        // This specifies the current implementation, which is simpler to write than an exhaustive test.
        // As long as this implementation doesn't change, then the equals/hashcode contract is upheld.

        assertEquals(-1, Maybe.undefined().hashCode());
        assertEquals(0, Maybe.defined(null).hashCode());
        assertEquals("a".hashCode(), Maybe.defined("a").hashCode());
    }
}
