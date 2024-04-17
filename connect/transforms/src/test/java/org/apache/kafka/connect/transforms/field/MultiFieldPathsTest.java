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

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiFieldPathsTest {

    @Test void shouldCreateMultiPathsWithDups() {
        MultiFieldPaths paths = new MultiFieldPaths(Arrays.asList("test", "test"), FieldSyntaxVersion.V2);
        assertEquals(1, paths.size());
        MultiFieldPaths.TrieNode test = paths.trie.get("test");
        assertNotNull(test);
        assertArrayEquals(test.path.path(), new String[]{"test"});
    }

    @Test void shouldBuildEmptyTrie() {
        MultiFieldPaths.Trie trie = new MultiFieldPaths.Trie();
        assertTrue(trie.isEmpty());
    }

    @Test void shouldBuildMultiPathWithSinglePathV1() {
        SingleFieldPath path = new SingleFieldPath("foo.bar.baz", FieldSyntaxVersion.V1);
        MultiFieldPaths paths = new MultiFieldPaths(Collections.singletonList(path));
        assertFalse(paths.trie.isEmpty());
        assertEquals(1, paths.trie.size());

        MultiFieldPaths.TrieNode maybeFoo = paths.trie.get("foo.bar.baz");
        assertNotNull(maybeFoo);
        assertEquals(path, maybeFoo.path);
    }

    @Test void shouldBuildMultiPathWithSinglePathV2() {
        SingleFieldPath path = new SingleFieldPath("foo.bar.baz", FieldSyntaxVersion.V2);
        MultiFieldPaths paths = new MultiFieldPaths(Collections.singletonList(path));
        assertFalse(paths.trie.isEmpty());
        assertEquals(1, paths.trie.size());

        MultiFieldPaths.TrieNode maybeV1 = paths.trie.get("foo.bar.baz");
        assertNull(maybeV1);
        MultiFieldPaths.TrieNode maybeFoo = paths.trie.get("foo");
        assertNotNull(maybeFoo);
        assertFalse(maybeFoo.isLeaf());
        MultiFieldPaths.TrieNode maybeBar = maybeFoo.get("bar");
        assertNotNull(maybeBar);
        assertFalse(maybeBar.isLeaf());
        MultiFieldPaths.TrieNode maybeBaz = maybeBar.get("baz");
        assertNotNull(maybeBaz);
        assertTrue(maybeBaz.isLeaf());
        assertEquals(path, maybeBaz.path);
    }

    @Test void shouldBuildMultiPathWithMultipleSinglePathV2() {
        MultiFieldPaths paths = new MultiFieldPaths(
            Arrays.asList(
                new SingleFieldPath("foo.bar", FieldSyntaxVersion.V2),
                new SingleFieldPath("foo.baz", FieldSyntaxVersion.V2),
                new SingleFieldPath("test", FieldSyntaxVersion.V2)));
        assertFalse(paths.trie.isEmpty());
        assertEquals(3, paths.trie.size());

        MultiFieldPaths.TrieNode maybeFoo = paths.trie.get("foo");
        assertNotNull(maybeFoo);
        assertFalse(maybeFoo.isLeaf());
        MultiFieldPaths.TrieNode maybeBar = maybeFoo.get("bar");
        assertNotNull(maybeBar);
        assertTrue(maybeBar.isLeaf());
        MultiFieldPaths.TrieNode maybeBaz = maybeFoo.get("baz");
        assertNotNull(maybeBaz);
        assertTrue(maybeBaz.isLeaf());
        MultiFieldPaths.TrieNode maybeTest = paths.trie.get("test");
        assertNotNull(maybeTest);
        assertTrue(maybeTest.isLeaf());
    }
}