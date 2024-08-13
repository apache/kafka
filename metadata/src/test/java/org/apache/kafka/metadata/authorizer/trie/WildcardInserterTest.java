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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WildcardInserterTest extends AbstractInserterTest {


    @Override
    protected InserterMetadata getMetadata() {
        return new InserterMetadata() {
            @Override
            public Inserter getInserter(String pattern) {
                return new WildcardInserter(pattern);
            }

            @Override
            public boolean supportWildcard() {
                return true;
            }
        };
    }

    @Test
    public void noWildcardPatternTests() {
        WildcardInserter underTest = new WildcardInserter("HelloWorld");
        assertEquals("HelloWorld", underTest.getFragment());
        assertFalse(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertTrue(underTest.isEmpty());
    }

    @Test
    public void questionWildcardPatternTest() {
        WildcardInserter underTest = new WildcardInserter("Hell?World");
        assertEquals("Hell", underTest.getFragment());
        assertFalse(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertFalse(underTest.isEmpty());

        assertEquals("?", underTest.getFragment());
        assertTrue(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertFalse(underTest.isEmpty());

        assertEquals("World", underTest.getFragment());
        assertFalse(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertTrue(underTest.isEmpty());
    }

    @Test
    public void splatWildcardPatternTest() {
        WildcardInserter underTest = new WildcardInserter("Hell*rld");
        assertEquals("Hell", underTest.getFragment());
        assertFalse(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertFalse(underTest.isEmpty());

        assertEquals("*", underTest.getFragment());
        assertTrue(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertFalse(underTest.isEmpty());

        assertEquals("rld", underTest.getFragment());
        assertFalse(underTest.isWildcard());
        underTest.advance(underTest.getFragment().length());
        assertTrue(underTest.isEmpty());
    }

    @Test
    public void insertInTest() {
        Node<String> root = Node.makeRoot();
        NodeCounter<String> nodeCounter = new NodeCounter<>();

        WildcardInserter inserter = new WildcardInserter("HelloWorld");
        Node<String> helloWorld = inserter.insertIn(root);
        helloWorld.setContents("HelloWorld");
        assertEquals("HelloWorld", helloWorld.getFragment());
        assertEquals(root, helloWorld.getParent());

        inserter = new WildcardInserter("HelloDolly");
        Node<String> helloDolly = inserter.insertIn(root);
        helloDolly.setContents("HelloDolly");
        assertEquals("Dolly", helloDolly.getFragment());
        Walker.depthFirst(nodeCounter, root);
        assertEquals(2, nodeCounter.count());

        // expect root -> Hello -+-> Dolly
        //                       +-> World

        assertEquals(1, root.getChildren().size());
        Node<String> hello = root.getChildren().first();

        assertEquals(2, hello.getChildren().size());
        assertEquals("Hello", hello.getFragment());
        assertNull(hello.getContents());

        Iterator<Node<String>> iter = hello.getChildren().iterator();
        Node<String> child = iter.next();
        assertEquals(0, child.getChildren().size());
        assertEquals("Dolly", child.getFragment());
        assertEquals("HelloDolly", child.getContents());

        child = iter.next();
        assertEquals(0, child.getChildren().size());
        assertEquals("World", child.getFragment());
        assertEquals("HelloWorld", child.getContents());

        assertFalse(iter.hasNext());

        inserter = new WildcardInserter("H*Wheels");
        Node<String> hWheels = inserter.insertIn(root);
        hWheels.setContents("H*Wheels");
        assertEquals("Wheels", hWheels.getFragment());
        Walker.depthFirst(nodeCounter, root);
        assertEquals(3, nodeCounter.count());

        // expect root --> H -+-> * ---> Wheels
        //                    +-ello -+-> Dolly
        //                            +-> World
        NodeNameCollector<String> collector = new NodeNameCollector<>();
        List<String> expected = Arrays.asList("", "H", "*", "Wheels", "ello", "Dolly", "World");
        Walker.preOrder(collector, root);
        assertEquals(expected, collector.tokens());

        inserter = new WildcardInserter("H?tWheels");
        Node<String> htWheels = inserter.insertIn(root);
        htWheels.setContents("H?tWheels");
        assertEquals("tWheels", htWheels.getFragment());
        Walker.depthFirst(nodeCounter, root);
        assertEquals(4, nodeCounter.count());

        expected = Arrays.asList("", "H", "*", "Wheels", "?", "tWheels", "ello", "Dolly", "World");
        Walker.preOrder(collector, root);
        assertEquals(expected, collector.tokens());
    }

    @Test
    public void testMultipleWildcards() {
        String[] inserts = {"HelloWorld", "HelloDolly", "H*Wheels", "H?tWheels", "H??Wheels", "H*W*"};
        String[][] tests = {
                {"HotWheels", "H?tWheels"}, {"HatWheels", "H?tWheels"},
                {"HamWheels", "H??Wheels"}, {"HeetWheels", "H*Wheels"}, {"HelloWorld", "HelloWorld"},
                {"HelloWales", "H*W*"}, {"HamWeed", "H*W*"}
        };

        Node<String> root = Node.makeRoot();
        doInserts(root, inserts);
        NodeNameCollector<String> collector = new NodeNameCollector<>();
        Walker.preOrder(collector, root);
        List<String> expected = Arrays.asList("", "H", "*", "W", "*", "heels", "?", "?", "Wheels", "tWheels", "ello", "Dolly", "World");
        assertEquals(expected, collector.tokens());
    }
}
