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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TrieSetTest {

    private TrieSet trieSet;

    @BeforeEach
    public void setUp() {
        trieSet = new TrieSet();
    }

    @Test
    public void testEmptySet() {
        assertTrue(trieSet.isEmpty());
        assertEquals(0, trieSet.size());
    }

    @Test
    public void testAddSingleElement() {
        assertTrue(trieSet.add("test"));
        assertFalse(trieSet.isEmpty());
        assertEquals(1, trieSet.size());
        assertTrue(trieSet.contains("test"));
    }

    @Test
    public void testAddDuplicateElement() {
        assertTrue(trieSet.add("test"));
        assertFalse(trieSet.add("test"));
        assertEquals(1, trieSet.size());
    }

    @Test
    public void testAddNullElement() {
        assertThrows(NullPointerException.class, () -> trieSet.add(null));
    }

    @Test
    public void testContains() {
        trieSet.add("apple");
        trieSet.add("application");
        trieSet.add("apply");

        assertTrue(trieSet.contains("apple"));
        assertTrue(trieSet.contains("application"));
        assertTrue(trieSet.contains("apply"));
        assertFalse(trieSet.contains("app"));
        assertFalse(trieSet.contains("apples"));
    }

    @Test
    public void testRemove() {
        trieSet.add("test");
        assertTrue(trieSet.remove("test"));
        assertFalse(trieSet.contains("test"));
        assertEquals(0, trieSet.size());
        assertTrue(trieSet.isEmpty());
    }

    @Test
    public void testRemoveNonExistent() {
        trieSet.add("test");
        assertFalse(trieSet.remove("nonexistent"));
        assertEquals(1, trieSet.size());
    }

    @Test
    public void testClear() {
        trieSet.add("test1");
        trieSet.add("test2");
        trieSet.add("test3");

        assertEquals(3, trieSet.size());
        trieSet.clear();
        assertEquals(0, trieSet.size());
        assertTrue(trieSet.isEmpty());
    }

    @Test
    public void testAddAll() {
        Set<String> elements = new HashSet<>(Arrays.asList("apple", "banana", "cherry"));
        assertTrue(trieSet.addAll(elements));
        assertEquals(3, trieSet.size());
        assertTrue(trieSet.contains("apple"));
        assertTrue(trieSet.contains("banana"));
        assertTrue(trieSet.contains("cherry"));
    }

    @Test
    public void testAddAllEmpty() {
        Set<String> elements = new HashSet<>();
        assertFalse(trieSet.addAll(elements));
        assertEquals(0, trieSet.size());
    }

    @Test
    public void testAddAllWithExistingElements() {
        trieSet.add("apple");
        Set<String> elements = new HashSet<>(Arrays.asList("apple", "banana"));
        assertTrue(trieSet.addAll(elements));
        assertEquals(2, trieSet.size());
    }

    @Test
    public void testRemoveAll() {
        trieSet.add("apple");
        trieSet.add("banana");
        trieSet.add("cherry");

        Set<String> toRemove = new HashSet<>(Arrays.asList("apple", "cherry"));
        assertTrue(trieSet.removeAll(toRemove));
        assertEquals(1, trieSet.size());
        assertTrue(trieSet.contains("banana"));
        assertFalse(trieSet.contains("apple"));
        assertFalse(trieSet.contains("cherry"));
    }

    @Test
    public void testRemoveAllNonExistent() {
        trieSet.add("apple");
        Set<String> toRemove = new HashSet<>(Arrays.asList("banana", "cherry"));
        assertFalse(trieSet.removeAll(toRemove));
        assertEquals(1, trieSet.size());
        assertTrue(trieSet.contains("apple"));
    }

    @Test
    public void testContainsAll() {
        trieSet.add("apple");
        trieSet.add("banana");
        trieSet.add("cherry");

        Set<String> subset = new HashSet<>(Arrays.asList("apple", "banana"));
        assertTrue(trieSet.containsAll(subset));

        Set<String> notSubset = new HashSet<>(Arrays.asList("apple", "durian"));
        assertFalse(trieSet.containsAll(notSubset));
    }

    @Test
    public void testRetainAll() {
        trieSet.add("apple");
        trieSet.add("banana");
        trieSet.add("cherry");
        trieSet.add("durian");

        Set<String> toRetain = new HashSet<>(Arrays.asList("apple", "cherry", "elderberry"));
        assertTrue(trieSet.retainAll(toRetain));
        assertEquals(2, trieSet.size());
        assertTrue(trieSet.contains("apple"));
        assertTrue(trieSet.contains("cherry"));
        assertFalse(trieSet.contains("banana"));
        assertFalse(trieSet.contains("durian"));
    }

    @Test
    public void testRetainAllNoChange() {
        trieSet.add("apple");
        trieSet.add("banana");

        Set<String> toRetain = new HashSet<>(Arrays.asList("apple", "banana", "cherry"));
        assertFalse(trieSet.retainAll(toRetain));
        assertEquals(2, trieSet.size());
    }

    @Test
    public void testIterator() {
        trieSet.add("apple");
        trieSet.add("banana");
        trieSet.add("cherry");

        Set<String> iterated = new HashSet<>();
        for (String s : trieSet) {
            iterated.add(s);
        }
        assertEquals(3, iterated.size());
        assertTrue(iterated.contains("apple"));
        assertTrue(iterated.contains("banana"));
        assertTrue(iterated.contains("cherry"));
    }

    @Test
    public void testIteratorRemove() {
        trieSet.add("apple");
        trieSet.add("banana");
        trieSet.add("cherry");

        Iterator<String> it = trieSet.iterator();
        while (it.hasNext()) {
            String s = it.next();
            if (s.equals("banana")) {
                it.remove();
            }
        }
        assertEquals(2, trieSet.size());
        assertFalse(trieSet.contains("banana"));
    }

    @Test
    public void testToArray() {
        trieSet.add("apple");
        trieSet.add("banana");
        trieSet.add("cherry");

        Object[] array = trieSet.toArray();
        assertEquals(3, array.length);
        Set<String> arraySet = new HashSet<>(Arrays.asList((String[]) Arrays.copyOf(array, array.length, String[].class)));
        assertTrue(arraySet.contains("apple"));
        assertTrue(arraySet.contains("banana"));
        assertTrue(arraySet.contains("cherry"));
    }

    @Test
    public void testToArrayTyped() {
        trieSet.add("apple");
        trieSet.add("banana");

        String[] array = trieSet.toArray(new String[0]);
        assertEquals(2, array.length);
        Set<String> arraySet = new HashSet<>(Arrays.asList(array));
        assertTrue(arraySet.contains("apple"));
        assertTrue(arraySet.contains("banana"));
    }

    // ========== PREFIX QUERY TESTS ==========

    @Test
    public void testPrefixSetEmpty() {
        Set<String> result = trieSet.prefixSet("app");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testPrefixSetExactMatch() {
        trieSet.add("apple");
        Set<String> result = trieSet.prefixSet("apple");
        assertEquals(1, result.size());
        assertTrue(result.contains("apple"));
    }

    @Test
    public void testPrefixSetMultipleMatches() {
        trieSet.add("apple");
        trieSet.add("application");
        trieSet.add("apply");
        trieSet.add("ape");
        trieSet.add("banana");

        Set<String> result = trieSet.prefixSet("app");
        assertEquals(3, result.size());
        assertTrue(result.contains("apple"));
        assertTrue(result.contains("application"));
        assertTrue(result.contains("apply"));
        assertFalse(result.contains("ape"));
        assertFalse(result.contains("banana"));
    }

    @Test
    public void testPrefixSetSingleCharacter() {
        trieSet.add("apple");
        trieSet.add("application");
        trieSet.add("ape");
        trieSet.add("banana");

        Set<String> result = trieSet.prefixSet("a");
        assertEquals(3, result.size());
        assertTrue(result.contains("apple"));
        assertTrue(result.contains("application"));
        assertTrue(result.contains("ape"));
        assertFalse(result.contains("banana"));
    }

    @Test
    public void testPrefixSetNoMatches() {
        trieSet.add("apple");
        trieSet.add("banana");

        Set<String> result = trieSet.prefixSet("cherry");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testPrefixSetEmptyPrefix() {
        trieSet.add("apple");
        trieSet.add("banana");
        trieSet.add("cherry");

        Set<String> result = trieSet.prefixSet("");
        assertEquals(3, result.size());
    }

    @Test
    public void testPrefixSetLongerThanAnyElement() {
        trieSet.add("app");

        Set<String> result = trieSet.prefixSet("application");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testPrefixSetCommonPrefixes() {
        trieSet.add("topic-foo-1");
        trieSet.add("topic-foo-2");
        trieSet.add("topic-foo-3");
        trieSet.add("topic-bar-1");
        trieSet.add("other-topic");

        Set<String> result = trieSet.prefixSet("topic-foo");
        assertEquals(3, result.size());
        assertTrue(result.contains("topic-foo-1"));
        assertTrue(result.contains("topic-foo-2"));
        assertTrue(result.contains("topic-foo-3"));
        assertFalse(result.contains("topic-bar-1"));
        assertFalse(result.contains("other-topic"));
    }

    @Test
    public void testPrefixSetNestedPrefixes() {
        trieSet.add("a");
        trieSet.add("ab");
        trieSet.add("abc");
        trieSet.add("abcd");

        Set<String> result = trieSet.prefixSet("ab");
        assertEquals(3, result.size());
        assertTrue(result.contains("ab"));
        assertTrue(result.contains("abc"));
        assertTrue(result.contains("abcd"));
        assertFalse(result.contains("a"));
    }

    // ========== PERFORMANCE/STRESS TESTS ==========

    @Test
    public void testLargeDataset() {
        for (int i = 0; i < 1000; i++) {
            trieSet.add("element-" + i);
        }
        assertEquals(1000, trieSet.size());

        Set<String> result = trieSet.prefixSet("element-1");
        // Should match: element-1, element-10, element-11, ..., element-19, element-100, ..., element-199
        assertTrue(result.size() > 1);
        assertTrue(result.contains("element-1"));
        assertTrue(result.contains("element-10"));
        assertTrue(result.contains("element-100"));
    }

    @Test
    public void testPrefixSetAfterRemoval() {
        trieSet.add("apple");
        trieSet.add("application");
        trieSet.add("apply");

        Set<String> result1 = trieSet.prefixSet("app");
        assertEquals(3, result1.size());

        trieSet.remove("application");

        Set<String> result2 = trieSet.prefixSet("app");
        assertEquals(2, result2.size());
        assertTrue(result2.contains("apple"));
        assertTrue(result2.contains("apply"));
        assertFalse(result2.contains("application"));
    }

    @Test
    public void testRemoveAllFromPrefixSet() {
        trieSet.add("topic-foo-1");
        trieSet.add("topic-foo-2");
        trieSet.add("topic-foo-3");
        trieSet.add("topic-bar-1");
        trieSet.add("other-topic");

        // Get all with prefix "topic-foo" - need to copy to avoid ConcurrentModificationException
        // since prefixSet() returns a view backed by the trie
        Set<String> prefixMatches = new HashSet<>(trieSet.prefixSet("topic-foo"));
        assertEquals(3, prefixMatches.size());

        // Remove them all
        assertTrue(trieSet.removeAll(prefixMatches));
        assertEquals(2, trieSet.size());
        assertTrue(trieSet.contains("topic-bar-1"));
        assertTrue(trieSet.contains("other-topic"));
        assertFalse(trieSet.contains("topic-foo-1"));
        assertFalse(trieSet.contains("topic-foo-2"));
        assertFalse(trieSet.contains("topic-foo-3"));
    }

    @Test
    public void testPrefixSetWithSpecialCharacters() {
        trieSet.add("user-123");
        trieSet.add("user-456");
        trieSet.add("user_admin");
        trieSet.add("user.guest");

        Set<String> result = trieSet.prefixSet("user-");
        assertEquals(2, result.size());
        assertTrue(result.contains("user-123"));
        assertTrue(result.contains("user-456"));
    }

    @Test
    public void testPrefixSetCaseSensitive() {
        trieSet.add("Apple");
        trieSet.add("application");
        trieSet.add("APPLY");

        Set<String> result1 = trieSet.prefixSet("app");
        assertEquals(1, result1.size());
        assertTrue(result1.contains("application"));

        Set<String> result2 = trieSet.prefixSet("App");
        assertEquals(1, result2.size());
        assertTrue(result2.contains("Apple"));

        Set<String> result3 = trieSet.prefixSet("APP");
        assertEquals(1, result3.size());
        assertTrue(result3.contains("APPLY"));
    }
}
