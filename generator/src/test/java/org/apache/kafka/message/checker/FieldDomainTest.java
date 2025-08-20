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

package org.apache.kafka.message.checker;

import org.apache.kafka.message.Versions;

import org.junit.jupiter.api.Test;

import static org.apache.kafka.message.checker.CheckerTestUtils.field;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FieldDomainTest {
    @Test
    public void testMessage1Only() {
        assertEquals(FieldDomain.MESSAGE1_ONLY,
            FieldDomain.of(field("bar", "1-1", "string"),
                new Versions((short) 0, (short) 1),
                new Versions((short) 2, (short) 5)));
    }

    @Test
    public void testBoth() {
        assertEquals(FieldDomain.BOTH,
            FieldDomain.of(field("bar", "1+", "string"),
                new Versions((short) 0, (short) 1),
                new Versions((short) 0, (short) 3)));
    }

    @Test
    public void testMessage2Only() {
        assertEquals(FieldDomain.MESSAGE2_ONLY,
            FieldDomain.of(field("bar", "1+", "string"),
                new Versions((short) 0, (short) 0),
                new Versions((short) 0, (short) 1)));
    }

    @Test
    public void testNeither() {
        assertEquals(FieldDomain.NEITHER,
            FieldDomain.of(field("bar", "2+", "string"),
                new Versions((short) 0, (short) 0),
                new Versions((short) 0, (short) 1)));
    }
}
