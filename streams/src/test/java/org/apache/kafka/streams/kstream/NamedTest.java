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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.errors.TopologyException;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.fail;

public class NamedTest {

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionGivenNullName() {
        Named.as(null);
    }

    @Test
    public void shouldThrowExceptionOnInvalidTopicNames() {
        final char[] longString = new char[250];
        Arrays.fill(longString, 'a');
        final String[] invalidNames = {"", "foo bar", "..", "foo:bar", "foo=bar", ".", new String(longString)};

        for (final String name : invalidNames) {
            try {
                Named.validate(name);
                fail("No exception was thrown for named with invalid name: " + name);
            } catch (final TopologyException e) {
                // success
            }
        }
    }
}