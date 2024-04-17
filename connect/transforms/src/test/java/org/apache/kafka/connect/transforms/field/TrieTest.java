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
package org.apache.kafka.connect.transforms.field;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrieTest {

    @Test void shouldBuildEmptyTrie() {
        Trie trie = new Trie();
        assertTrue(trie.isEmpty());
        assertFalse(trie.root.isLeaf());
        assertTrue(trie.root.isEmpty());
    }

    @Test void shouldBuildMinimalTrie() {
        Trie trie = new Trie();
        trie.insert(new SingleFieldPath("test", FieldSyntaxVersion.V2));
        assertFalse(trie.isEmpty());
        assertEquals(1, trie.size());
        assertFalse(trie.root.isLeaf());
        assertTrue(trie.get("test").isLeaf());
    }

    @Test void shouldBuildTrieWithSeparatePaths() {
        Trie trie = new Trie();
        trie.insert(new SingleFieldPath("foo", FieldSyntaxVersion.V2));
        trie.insert(new SingleFieldPath("bar", FieldSyntaxVersion.V2));
        assertEquals(2, trie.size());
        assertFalse(trie.root.isLeaf());
        assertTrue(trie.get("foo").isLeaf());
        assertTrue(trie.get("bar").isLeaf());
    }

    public static Stream<Arguments> overlappingPaths() {
        final SingleFieldPath foo = new SingleFieldPath("foo", FieldSyntaxVersion.V2);
        final SingleFieldPath fooBar = new SingleFieldPath("foo.bar", FieldSyntaxVersion.V2);
        return Stream.of(
            Arguments.of(foo, fooBar),
            Arguments.of(fooBar, foo)
        );
    }

    @ParameterizedTest
    @MethodSource("overlappingPaths")
    void shouldFlatOverlappingPaths(SingleFieldPath first, SingleFieldPath second) {
        Trie trie1 = new Trie();
        trie1.insert(first);
        trie1.insert(second);
        assertFalse(trie1.isEmpty());
        assertEquals(1, trie1.size());

        Trie.Node foo1 = trie1.get("foo");
        assertFalse(foo1.isLeaf());
        Trie.Node bar1 = foo1.get("bar");
        assertTrue(bar1.isLeaf());
        assertArrayEquals(new String[]{"foo", "bar"}, bar1.path.path());
    }
}