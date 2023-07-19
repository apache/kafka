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
package org.apache.kafka.common.utils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MappedIteratorTest {

    @Test
    public void testStringToInteger() {
        List<String> list = asList("foo", "", "bar2", "baz45");
        Function<String, Integer> mapper = s -> s.length();

        Iterable<Integer> mappedIterable = () -> new MappedIterator<>(list.iterator(), mapper);
        List<Integer> mapped = new ArrayList<>();
        mappedIterable.forEach(mapped::add);

        assertEquals(list.stream().map(mapper).collect(Collectors.toList()), mapped);

        // Ensure that we can iterate a second time
        List<Integer> mapped2 = new ArrayList<>();
        mappedIterable.forEach(mapped2::add);
        assertEquals(mapped, mapped2);
    }

    @Test
    public void testEmptyList() {
        List<String> list = emptyList();
        Function<String, Integer> mapper = s -> s.length();

        Iterable<Integer> mappedIterable = () -> new MappedIterator<>(list.iterator(), mapper);
        List<Integer> mapped = new ArrayList<>();
        mappedIterable.forEach(mapped::add);

        assertEquals(emptyList(), mapped);
    }

}
