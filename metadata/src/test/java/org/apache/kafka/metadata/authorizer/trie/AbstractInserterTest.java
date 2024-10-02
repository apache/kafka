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

public abstract class AbstractInserterTest {


    protected abstract InserterMetadata getMetadata();

    protected void doInserts(Node<String> root, String[] inserts) {
        InserterMetadata metadata = getMetadata();
        for (String insert : inserts) {
            Inserter inserter = metadata.getInserter(insert);
            Node<String> n = inserter.insertIn(root);
            n.setContents(insert);
        }
    }


    @Test
    public void insertInTest() {
        InserterMetadata metadata = getMetadata();
        Node<String> root = Node.makeRoot();
        NodeCounter<String> nodeCounter = new NodeCounter<>();


        Node<String> helloWorld = metadata.getInserter("HelloWorld").insertIn(root);
        helloWorld.setContents("HelloWorld");
        assertEquals("HelloWorld", helloWorld.getFragment());
        assertEquals(root, helloWorld.getParent());

        Node<String> helloDolly = metadata.getInserter("HelloDolly").insertIn(root);
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

        NodeNameCollector<String> collector = new NodeNameCollector<>();

        if (!metadata.supportWildcard()) {
            Node<String> hWheels = metadata.getInserter("HotWheels").insertIn(root);
            hWheels.setContents("HoWheels");

            List<String> expected = Arrays.asList("", "H", "ello", "Dolly", "World", "otWheels");
            Walker.preOrder(collector, root);
            assertEquals(expected, collector.tokens());
        } else {
            Node<String> hWheels = metadata.getInserter("H*Wheels").insertIn(root);
            hWheels.setContents("H*Wheels");
            assertEquals("Wheels", hWheels.getFragment());
            Walker.depthFirst(nodeCounter, root);
            assertEquals(3, nodeCounter.count());

            // expect root --> H -+-> * ---> Wheels
            //                    +-ello -+-> Dolly
            //                            +-> World
            List<String> expected = Arrays.asList("", "H", "*", "Wheels", "ello", "Dolly", "World");
            Walker.preOrder(collector, root);
            assertEquals(expected, collector.tokens());


            Node<String> htWheels = metadata.getInserter("H?tWheels").insertIn(root);
            htWheels.setContents("H?tWheels");
            assertEquals("tWheels", htWheels.getFragment());
            Walker.depthFirst(nodeCounter, root);
            assertEquals(4, nodeCounter.count());

            expected = Arrays.asList("", "H", "*", "Wheels", "?", "tWheels", "ello", "Dolly", "World");
            Walker.preOrder(collector, root);
            assertEquals(expected, collector.tokens());
        }
    }

    public interface InserterMetadata {
        Inserter getInserter(String pattern);
        boolean supportWildcard();
    }
}
