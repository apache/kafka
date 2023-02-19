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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CoreUtilsTest {
    @Test
    public void testDuplicates() {
        assertIterableEquals(
            CoreUtils.duplicates(Arrays.asList("1", "2", "1", "3", "3")),
            Arrays.asList("1", "3")
        );

        assertIterableEquals(
            CoreUtils.duplicates(Arrays.asList("1", "2", "3")),
            Collections.emptyList()
        );
    }

    public <T> void assertIterableEquals(Iterable<T> exp, Collection<T> res) {
        Set<T> expSet = StreamSupport.stream(exp.spliterator(), false).collect(Collectors.toSet());

        assertEquals(expSet.size(), res.size(), "Size must be equal");

        for (T item : expSet) {
            if (!res.contains(item))
                throw new RuntimeException("Item not in result collection " + item);
        }
    }
}
