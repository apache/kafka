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
package org.apache.kafka.coordinator.group.modern.consumer;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ResolvedRegularExpressionTest {
    @Test
    public void testConstructor() {
        ResolvedRegularExpression resolvedRegularExpression = new ResolvedRegularExpression(
            Set.of("foo", "bar"),
            10L,
            12345L
        );

        assertEquals(Set.of("foo", "bar"), resolvedRegularExpression.topics);
        assertEquals(10L, resolvedRegularExpression.version);
        assertEquals(12345L, resolvedRegularExpression.timestamp);
    }

    @Test
    public void testEquals() {
        assertEquals(
            new ResolvedRegularExpression(
                Set.of("foo", "bar"),
                10L,
                12345L
            ),
            new ResolvedRegularExpression(
                Set.of("foo", "bar"),
                10L,
                12345L
            )
        );

        assertNotEquals(
            new ResolvedRegularExpression(
                Set.of("foo", "bar"),
                10L,
                12345L
            ),
            new ResolvedRegularExpression(
                Set.of("foo"),
                10L,
                12345L
            )
        );
    }
}
