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
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlattenedIteratorTest {

    @Test
    public void testNestedLists() {
        List<List<String>> list = asList(
            asList("foo", "a", "bc"),
            asList("ddddd"),
            asList("", "bar2", "baz45"));

        Iterable<String> flattenedIterable = () -> new FlattenedIterator<>(list.iterator(), l -> l.iterator());
        List<String> flattened = new ArrayList<>();
        flattenedIterable.forEach(flattened::add);

        assertEquals(list.stream().flatMap(l -> l.stream()).collect(Collectors.toList()), flattened);

        // Ensure we can iterate multiple times
        List<String> flattened2 = new ArrayList<>();
        flattenedIterable.forEach(flattened2::add);

        assertEquals(flattened, flattened2);
    }

    @Test
    public void testEmptyList() {
        List<List<String>> list = emptyList();

        Iterable<String> flattenedIterable = () -> new FlattenedIterator<>(list.iterator(), l -> l.iterator());
        List<String> flattened = new ArrayList<>();
        flattenedIterable.forEach(flattened::add);

        assertEquals(emptyList(), flattened);
    }

    @Test
    public void testNestedSingleEmptyList() {
        List<List<String>> list = asList(emptyList());

        Iterable<String> flattenedIterable = () -> new FlattenedIterator<>(list.iterator(), l -> l.iterator());
        List<String> flattened = new ArrayList<>();
        flattenedIterable.forEach(flattened::add);

        assertEquals(emptyList(), flattened);
    }

    @Test
    public void testEmptyListFollowedByNonEmpty() {
        List<List<String>> list = asList(
            emptyList(),
            asList("boo", "b", "de"));

        Iterable<String> flattenedIterable = () -> new FlattenedIterator<>(list.iterator(), l -> l.iterator());
        List<String> flattened = new ArrayList<>();
        flattenedIterable.forEach(flattened::add);

        assertEquals(list.stream().flatMap(l -> l.stream()).collect(Collectors.toList()), flattened);
    }

    @Test
    public void testEmptyListInBetweenNonEmpty() {
        List<List<String>> list = asList(
            asList("aadwdwdw"),
            emptyList(),
            asList("ee", "aa", "dd"));

        Iterable<String> flattenedIterable = () -> new FlattenedIterator<>(list.iterator(), l -> l.iterator());
        List<String> flattened = new ArrayList<>();
        flattenedIterable.forEach(flattened::add);

        assertEquals(list.stream().flatMap(l -> l.stream()).collect(Collectors.toList()), flattened);
    }

    @Test
    public void testEmptyListAtTheEnd() {
        List<List<String>> list = asList(
            asList("ee", "dd"),
            asList("e"),
            emptyList());

        Iterable<String> flattenedIterable = () -> new FlattenedIterator<>(list.iterator(), l -> l.iterator());
        List<String> flattened = new ArrayList<>();
        flattenedIterable.forEach(flattened::add);

        assertEquals(list.stream().flatMap(l -> l.stream()).collect(Collectors.toList()), flattened);
    }

}

