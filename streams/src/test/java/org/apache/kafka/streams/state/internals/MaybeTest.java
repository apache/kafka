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

import org.junit.Test;

import java.util.NoSuchElementException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

public class MaybeTest {
    @Test
    public void shouldReturnDefinedValue() {
        assertThat(Maybe.defined(null).getNullableValue(), nullValue());
        assertThat(Maybe.defined("ASDF").getNullableValue(), is("ASDF"));
    }

    @Test
    public void shouldAnswerIsDefined() {
        assertThat(Maybe.defined(null).isDefined(), is(true));
        assertThat(Maybe.defined("ASDF").isDefined(), is(true));
        assertThat(Maybe.undefined().isDefined(), is(false));
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
        assertThat(Maybe.undefined().equals(Maybe.undefined()), is(true));
        assertThat(Maybe.defined(null).equals(Maybe.defined(null)), is(true));
        assertThat(Maybe.defined("q").equals(Maybe.defined("q")), is(true));

        assertThat(Maybe.undefined().equals(Maybe.defined(null)), is(false));
        assertThat(Maybe.undefined().equals(Maybe.defined("x")), is(false));

        assertThat(Maybe.defined(null).equals(Maybe.undefined()), is(false));
        assertThat(Maybe.defined(null).equals(Maybe.defined("x")), is(false));

        assertThat(Maybe.defined("a").equals(Maybe.undefined()), is(false));
        assertThat(Maybe.defined("a").equals(Maybe.defined(null)), is(false));
        assertThat(Maybe.defined("a").equals(Maybe.defined("b")), is(false));
    }

    @Test
    public void shouldUpholdHashCodeCorrectness() {
        // This specifies the current implementation, which is simpler to write than an exhaustive test.
        // As long as this implementation doesn't change, then the equals/hashcode contract is upheld.

        assertThat(Maybe.undefined().hashCode(), is(-1));
        assertThat(Maybe.defined(null).hashCode(), is(0));
        assertThat(Maybe.defined("a").hashCode(), is("a".hashCode()));
    }
}
