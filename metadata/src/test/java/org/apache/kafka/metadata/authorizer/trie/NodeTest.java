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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.function.Predicate;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class NodeTest {

    @Test
    public void addNodeForTest() {


        Node<String> root = Node.makeRoot();
        NodeCounter<String> nodeCounter = new NodeCounter<>();

        StringInserter inserter = new StringInserter("HelloWorld");
        Node<String> helloWorld = root.addNodeFor(inserter);
        helloWorld.setContents("HelloWorld");
        assertEquals("HelloWorld", helloWorld.getFragment());
        assertEquals(root, helloWorld.getParent());

        inserter = new StringInserter("HelloDolly");
        Node<String> helloDolly = root.addNodeFor(inserter);
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

        inserter = new StringInserter("H*Wheels");
        Node<String> hWheels = root.addNodeFor(inserter);
        hWheels.setContents("H*Wheels");
        assertEquals("Wheels", hWheels.getFragment());
        Walker.depthFirst(nodeCounter, root);
        assertEquals(3, nodeCounter.count());

        // expect root --> H -+-> * ---> Wheels
        //                    +-ello -+-> Dolly
        //                            +-> World
        Collector<String> collector = new Collector<>();
        List<String> expected = Arrays.asList("", "H", "*", "Wheels", "ello", "Dolly", "World");
        Walker.preOrder(collector, root);
        assertEquals(expected, collector.tokens());

        inserter = new StringInserter("H?tWheels");
        Node<String> htWheels = root.addNodeFor(inserter);
        htWheels.setContents("H?tWheels");
        assertEquals("tWheels", htWheels.getFragment());
        Walker.depthFirst(nodeCounter, root);
        assertEquals(4, nodeCounter.count());

        expected = Arrays.asList("", "H", "*", "Wheels", "?", "tWheels", "ello", "Dolly", "World");
        Walker.preOrder(collector, root);
        assertEquals(expected, collector.tokens());
    }

    @Test
    public void findNodeForTest() {

        String[] inserts = {"HelloWorld", "HelloDolly", "H*Wheels", "H?tWheels"};
        String[][] tests = {
                {"HotWheels", "H?tWheels"}, {"HotWheelsCar", "H?tWheels"},
                {"HelloWheels", "H*Wheels"}, {"Hello", null}, {"HelloDollyClone", "HelloDolly"},
                {"HelloWorlds", "HelloWorld"}, {"HelloD", null}
        };

        // setup same as addNodeFor

        Node<String> root = Node.makeRoot();
        doInserts(root, inserts);
        doTests(root, tests);
    }

    private void doInserts(Node<String> root, String[] inserts) {
        for (String insert : inserts) {
            StringInserter inserter = new StringInserter(insert);
            Node<String> n = root.addNodeFor(inserter);
            n.setContents(insert);
        }
    }

    private void doTests(Node<String> root, String[][] tests) {
        for (String[] test : tests) {
            Node<String> n = root.findNodeFor(new StringMatcher<>(test[0], p -> false));
            if (n == null) {
                assertNull(test[1], () -> format("%s -> %s", test[0], test[1]));
            } else {
                assertEquals(test[1], n.getContents(), () -> format("%s -> %s", test[0], test[1]));
            }
        }
    }

    @Test
    public void deleteTest() {
        NodeCounter<String> nodeCounter = new NodeCounter<>();
        String[] inserts = {"HelloWorld", "HelloDolly", "H*Wheels", "H?tWheels"};

        Node<String> root = Node.makeRoot();
        doInserts(root, inserts);

        Walker.depthFirst(nodeCounter, root);
        assertEquals(4, nodeCounter.count());
        Node<String> n = root.findNodeFor(new StringMatcher<>("HelloDolly", p -> false));
        n.delete();
        Walker.depthFirst(nodeCounter, root);
        assertEquals(3, nodeCounter.count());
        assertEquals(root, root.findNodeFor(new StringMatcher<>("HelloDolly", p -> false)), "found 'HelloDolly'");
        assertNotNull(root.findNodeFor(new StringMatcher<>("HelloWorld", p -> false)), "missing 'HelloWorld'");

        n = root.findNodeFor(new StringMatcher<>("Hello", p -> false));
        n.delete();
        Walker.depthFirst(nodeCounter, root);
        assertEquals(3, nodeCounter.count());
        assertEquals(root, root.findNodeFor(new StringMatcher<>("HelloDolly", p -> false)), "found 'HelloDolly'");
        assertNotNull(root.findNodeFor(new StringMatcher<>("HelloWorld", p -> false)), "missing 'HelloWorld'");

        n = root.findNodeFor(new StringMatcher<>("H?tWheels", p -> false));
        n.delete();
        Walker.depthFirst(nodeCounter, root);
        assertEquals(2, nodeCounter.count());
        assertEquals(root, root.findNodeFor(new StringMatcher<>("HelloDolly", p -> false)), "found 'HelloDolly'");
        assertNotNull(root.findNodeFor(new StringMatcher<>("HelloWorld", p -> false)), "missing 'HelloWorld'");
        // HotWheels should have moved from H?tWheels to H*Wheels
        n = root.findNodeFor(new StringMatcher<>("HotWheels", p -> false));
        assertEquals("H*Wheels", n.getContents(), "missing 'HotWheels'");
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
        Collector<String> collector = new Collector<>();
        Walker.preOrder(collector, root);
        List<String> expected = Arrays.asList("", "H", "*", "W", "*", "heels", "?", "?", "Wheels", "tWheels", "ello", "Dolly", "World");
        assertEquals(expected, collector.tokens());

        doTests(root, tests);
    }

    /**
     * Return the list of fragment strings of a depth first walk of the trie.
     */
    private static class Collector<T> implements Predicate<Node<T>> {
        private List<String> tokenList;

        Collector() {
            tokenList = new ArrayList<>();
        }

        public List<String> tokens() {
            List<String> result = new ArrayList<>(tokenList);
            tokenList.clear();
            return result;
        }

        @Override
        public boolean test(Node<T> node) {
            tokenList.add(node.getFragment());
            return false;
        }
    }
}
