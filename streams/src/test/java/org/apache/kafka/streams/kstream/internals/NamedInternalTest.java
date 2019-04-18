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
package org.apache.kafka.streams.kstream.internals;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NamedInternalTest {

    private static final String TEST_VALUE  = "default-value";
    private static final String TEST_SUFFIX = "-suffix";

    @Test
    public void shouldSuffixNameOrReturnProviderValue() {
        final String name = "foo";
        assertEquals(
                name + TEST_SUFFIX,
                NamedInternal.with(name).suffixWithOrElseGet(TEST_SUFFIX, () -> TEST_VALUE)
        );
        assertEquals(
                TEST_VALUE,
                NamedInternal.with(null).suffixWithOrElseGet(TEST_SUFFIX, () -> TEST_VALUE)
        );
    }

    @Test
    public void shouldGenerateWithPrefixGivenEmptyName() {
        final String prefix = "KSTREAM-MAP-";
        assertEquals(prefix + "PROCESSOR-NAME", NamedInternal.with(null).orElseGenerateWithPrefix(
                new InternalNameProvider() {
                    @Override
                    public String newProcessorName(final String prefix) {
                        return prefix + "PROCESSOR-NAME";
                    }

                    @Override
                    public String newStoreName(final String prefix) {
                        return null;
                    }
                },
                prefix)
        );
    }

    @Test
    public void shouldNotGenerateWithPrefixGivenValidName() {
        final String validName = "validName";
        assertEquals(validName, NamedInternal.with(validName).orElseGenerateWithPrefix(null, "KSTREAM-MAP-")
        );
    }
}