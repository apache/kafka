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

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class NodeTest {

    @Test
    public void findNodeForTest() {

        String[] inserts = {"HelloWorld", "HelloDolly", "H*Wheels", "H?tWheels"};
        String[][] tests = {
                {"HotWheels", null}, {"HotWheelsCar", null},
                {"HelloWheels", null}, {"Hello", null}, {"HelloDollyClone", "HelloDolly"},
                {"HelloWorlds", "HelloWorld"}, {"HelloD", null}
        };

        // setup same as addNodeFor

        Node<String> root = Node.makeRoot();
        doInserts(root, inserts);
        doTests(root, tests);
    }

    private void doInserts(Node<String> node, String[] inserts) {
        for (String pattern : inserts) {
            new StandardInserter(pattern).insertIn(node).setContents(pattern);
        }
    }

    private void doTests(Node<String> root, String[][] tests) {
        for (String[] test : tests) {
            Matcher.SearchResult<String> result = new StandardMatcher<String>(test[0], p -> false).searchIn(root);
            if (!result.hasContents()) {
                assertNull(test[1], () -> format("%s -> %s", test[0], test[1]));
            } else {
                assertEquals(test[1], result.getContents(), () -> format("%s -> %s", test[0], test[1]));
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
        Node<String> n = new StandardMatcher<String>("HelloDolly", p -> false).searchIn(root).getNode();
        n.delete();
        Walker.depthFirst(nodeCounter, root);
        assertEquals(3, nodeCounter.count());
        assertEquals(root, new StandardMatcher<String>("HelloDolly", p -> false).searchIn(root).getNode(), "found 'HelloDolly'");
        assertNotEquals(root, new StandardMatcher<String>("HelloWorld", p -> false).searchIn(root).getNode(), "missing 'HelloWorld'");

        n = new StandardMatcher<String>("Hello", p -> false).searchIn(root).getNode();
        n.delete();
        Walker.depthFirst(nodeCounter, root);
        assertEquals(3, nodeCounter.count());
        assertEquals(root, new StandardMatcher<String>("HelloDolly", p -> false).searchIn(root).getNode(), "found 'HelloDolly'");
        assertNotEquals(root, new StandardMatcher<String>("HelloWorld", p -> false).searchIn(root).getNode(), "missing 'HelloWorld'");

        n = new StandardMatcher<String>("H?tWheels", p -> false).searchIn(root).getNode();
        n.delete();
        Walker.depthFirst(nodeCounter, root);
        assertEquals(2, nodeCounter.count());
        assertEquals(root, new StandardMatcher<String>("HelloDolly", p -> false).searchIn(root).getNode(), "found 'HelloDolly'");
        assertNotEquals(root, new StandardMatcher<String>("HelloWorld", p -> false).searchIn(root).getNode(), "missing 'HelloWorld'");
        // HotWheels should have moved from H?tWheels to H*Wheels
        n = new StandardMatcher<String>("H*Wheels", p -> false).searchIn(root).getNode();
        assertEquals("H*Wheels", n.getContents(), "missing 'H*Wheels'");
    }
}
