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

package org.apache.kafka.metadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Optional;

import static org.apache.kafka.metadata.OptionalStringComparator.INSTANCE;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class OptionalStringComparatorTest {
    @Test
    public void testComparisons() {
        assertEquals(0, INSTANCE.compare(Optional.of("foo"), Optional.of("foo")));
        assertEquals(-1, INSTANCE.compare(Optional.of("a"), Optional.of("b")));
        assertEquals(1, INSTANCE.compare(Optional.of("b"), Optional.of("a")));
        assertEquals(-1, INSTANCE.compare(Optional.empty(), Optional.of("a")));
        assertEquals(1, INSTANCE.compare(Optional.of("a"), Optional.empty()));
        assertEquals(0, INSTANCE.compare(Optional.empty(), Optional.empty()));
    }
}
