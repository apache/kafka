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
package org.apache.kafka.metadata.authorizer.trie;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class TrieTest {

    private Trie<Integer> standardSetup() {
        Trie<Integer> trie = new Trie<>();
        trie.insert(new StandardInserter("onetwothree"), 3);
        trie.insert(new StandardInserter("onethreefour"), 4);
        trie.insert(new StandardInserter("twothreefive"), 5);
        return trie;
    }

    @Test
    public void testEquality() {
        NodeCounter<Integer> nodeCounter = new NodeCounter<>();
        Trie<Integer> trie = new Trie<>();
        Walker.depthFirst(nodeCounter, trie);
        assertEquals(0, nodeCounter.count());
        trie.insert(new StandardInserter("onetwothree"), 3, (x, y) -> 3);
        Walker.depthFirst(nodeCounter, trie);
        assertEquals(1, nodeCounter.count());
        trie.insert(new StandardInserter("onethreefour"), 4, (x, y) -> 4);
        Walker.depthFirst(nodeCounter, trie);
        assertEquals(2, nodeCounter.count());
        trie.insert(new StandardInserter("twothreefive"), 5);
        Walker.depthFirst(nodeCounter, trie);
        assertEquals(3, nodeCounter.count());

        ReadOnlyNode<Integer> result = trie.search(new StandardMatcher<>("onetwothree"));
        assertEquals(Integer.valueOf(3), result.getContents());
        assertEquals("onetwothree", result.getName());

        NodeNameCollector<Integer> collector = new NodeNameCollector<>();
        Walker.preOrder(collector, trie);
        for (String o : collector.tokens()) {
            System.out.println(o);
        }

        List<Node<Integer>> lst = ((Node<Integer>) result.delegate).pathTo();
        assertEquals(2, lst.size());
        assertEquals("onet", lst.get(0).getFragment());
        assertEquals("wothree", lst.get(1).getFragment());
        assertEquals(result.delegate, lst.get(1));
    }

    @Test
    public void testPartialSearch() {
        Trie<Integer> trie = new Trie<>();
        trie.insert(new StandardInserter("HotWheels"), 1);
        trie.insert(new StandardInserter("HatWheels"), 2);
        trie.insert(new StandardInserter("HotMama"), 3);


        ReadOnlyNode<Integer> result = trie.search(new StandardMatcher<>("HotWheelsCar"));

        assertEquals("HotWheels", result.toString());
        result = trie.search(new StandardMatcher<>("HotWhee"));
        assertEquals("", result.toString());
    }

    @Test
    public void getTest() {
        Trie<Integer> trie = standardSetup();

        ReadOnlyNode result = trie.search(new StandardMatcher<>("onetwothree"));
        assertEquals(3, trie.search(new StandardMatcher<>("onetwothree")).getContents());
        assertNull(trie.search(new StandardMatcher<>("onetwoth")).getContents());
        assertNull(trie.search(new StandardMatcher<>("onet")).getContents());
    }
}
