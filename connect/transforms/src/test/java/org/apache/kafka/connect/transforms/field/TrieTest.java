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
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrieTest {

    @Test void shouldBuildEmptyTrie() {
        Trie trie = new Trie();
        assertTrue(trie.isEmpty());
        assertFalse(trie.root.isLeaf());
        assertTrue(trie.root.isEmpty());
    }

    @Test void shouldBuildMinimalTrie() {
        Trie trie = trieWithPathsV2("test");
        assertFalse(trie.isEmpty());
        assertEquals(1, trie.size());
        assertFalse(trie.root.isLeaf());
        assertTrue(trie.get("test").isLeaf());
    }

    @Test void shouldBuildTrieWithSinglePathV1() {
        String path = "foo.bar.baz";
        Trie trie = trieWithPathsV1(path);
        assertFalse(trie.isEmpty());
        assertEquals(1, trie.size());

        Trie.Node maybeFoo = trie.get("foo.bar.baz");
        assertNotNull(maybeFoo);
        assertArrayEquals(new String[]{path}, maybeFoo.path.path());
    }

    @Test void shouldBuildTrieWithSeparatePaths() {
        Trie trie = trieWithPathsV2("foo", "bar");
        assertEquals(2, trie.size());
        assertFalse(trie.root.isLeaf());
        assertTrue(trie.get("foo").isLeaf());
        assertTrue(trie.get("bar").isLeaf());
    }

    @ParameterizedTest
    @CsvSource({"foo,foo.bar", "foo.bar,foo"})
    void shouldFlatOverlappingPaths(String first, String second) {
        Trie trie1 = trieWithPathsV2(first, second);
        assertFalse(trie1.isEmpty());
        assertEquals(1, trie1.size());

        Trie.Node foo1 = trie1.get("foo");
        assertFalse(foo1.isLeaf());
        Trie.Node bar1 = foo1.get("bar");
        assertTrue(bar1.isLeaf());
        assertArrayEquals(new String[]{"foo", "bar"}, bar1.path.path());
    }

    @Test void shouldCreateTrieWithDups() {
        Trie trie = trieWithPathsV2("test", "test");
        assertEquals(1, trie.size());
        Trie.Node test = trie.get("test");
        assertNotNull(test);
        assertArrayEquals(test.path.path(), new String[]{"test"});
    }

    @Test void shouldCreateTrieWithNestedNodes() {
        Trie trie = trieWithPathsV2("foo.bar.baz");
        assertFalse(trie.isEmpty());
        assertEquals(1, trie.size());

        Trie.Node maybeV1 = trie.get("foo.bar.baz");
        assertNull(maybeV1);
        Trie.Node maybeFoo = trie.get("foo");
        assertNotNull(maybeFoo);
        assertFalse(maybeFoo.isLeaf());
        Trie.Node maybeBar = maybeFoo.get("bar");
        assertNotNull(maybeBar);
        assertFalse(maybeBar.isLeaf());
        Trie.Node maybeBaz = maybeBar.get("baz");
        assertNotNull(maybeBaz);
        assertTrue(maybeBaz.isLeaf());
        assertArrayEquals(new String[]{"foo", "bar", "baz"}, maybeBaz.path.path());
    }

    @Test void shouldCreateTrieWithMultipleBranchesV2() {
        Trie trie = trieWithPathsV2("foo.bar", "foo.baz", "test");
        assertFalse(trie.isEmpty());
        assertEquals(3, trie.size());

        Trie.Node maybeFoo = trie.get("foo");
        assertNotNull(maybeFoo);
        assertFalse(maybeFoo.isLeaf());
        Trie.Node maybeBar = maybeFoo.get("bar");
        assertNotNull(maybeBar);
        assertTrue(maybeBar.isLeaf());
        Trie.Node maybeBaz = maybeFoo.get("baz");
        assertNotNull(maybeBaz);
        assertTrue(maybeBaz.isLeaf());
        Trie.Node maybeTest = trie.get("test");
        assertNotNull(maybeTest);
        assertTrue(maybeTest.isLeaf());
    }

    private static Trie trieWithPathsV2(String... paths) {
        return trieWithPaths(FieldSyntaxVersion.V2, paths);
    }

    private static Trie trieWithPathsV1(String... paths) {
        return trieWithPaths(FieldSyntaxVersion.V1, paths);
    }

    private static Trie trieWithPaths(FieldSyntaxVersion syntaxVersion, String... paths) {
        Trie trie = new Trie();
        for (String path : paths) {
            trie.insert(new SingleFieldPath(path, syntaxVersion));
        }
        return trie;
    }
}