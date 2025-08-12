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
package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GraphNodeTest {

    @Test
    public void testAddChild() {
        final GraphNode parentNode = new ExtendedGraphNode("");
        final GraphNode childNode = new ExtendedGraphNode("");
        parentNode.addChild(childNode);

        assertTrue(parentNode.children().contains(childNode));
        assertEquals(1, parentNode.children().size());

        assertTrue(childNode.parentNodes().contains(parentNode));
        assertEquals(1, childNode.parentNodes().size());
    }

    @Test
    public void testRemoveValidChild() {
        final GraphNode parentNode = new ExtendedGraphNode("");
        final GraphNode childNode = new ExtendedGraphNode("");
        parentNode.addChild(childNode);

        parentNode.removeChild(childNode);
        assertFalse(parentNode.children().contains(childNode));
        assertEquals(0, parentNode.children().size());

        assertFalse(childNode.parentNodes().contains(parentNode));
        assertEquals(0, childNode.parentNodes().size());
    }

    @Test
    public void testClearChildren() {
        final GraphNode parentNode = new ExtendedGraphNode("");
        final GraphNode firstChildNode = new ExtendedGraphNode("");
        final GraphNode secondChildNode = new ExtendedGraphNode("");
        final GraphNode thirdChildNode = new ExtendedGraphNode("");

        parentNode.addChild(firstChildNode);
        parentNode.addChild(secondChildNode);
        parentNode.addChild(thirdChildNode);

        parentNode.clearChildren();

        assertEquals(0, parentNode.children().size());
        assertEquals(0, firstChildNode.parentNodes().size());
        assertEquals(0, secondChildNode.parentNodes().size());
        assertEquals(0, thirdChildNode.parentNodes().size());
    }

    @Test
    public void testParentsWrittenToTopology() {
        final GraphNode firstParentNode = new ExtendedGraphNode("");
        final GraphNode secondParentNode = new ExtendedGraphNode("");
        final GraphNode childNode = new ExtendedGraphNode("");

        firstParentNode.addChild(childNode);
        secondParentNode.addChild(childNode);
        assertFalse(childNode.allParentsWrittenToTopology());

        firstParentNode.setHasWrittenToTopology(true);
        assertFalse(childNode.allParentsWrittenToTopology());

        secondParentNode.setHasWrittenToTopology(true);
        assertTrue(childNode.allParentsWrittenToTopology());
    }

    @Test
    public void testToString() {
        final GraphNode parentNode = new ExtendedGraphNode("Test");
        final GraphNode childNode = new ExtendedGraphNode("");

        parentNode.addChild(childNode);
        final String[] parentNodeNames = childNode.parentNodeNames();

        assertEquals(1, parentNodeNames.length);
        assertEquals("Test", parentNodeNames[0]);
    }

    @Test
    public void testCopyParentsCollection() {
        final GraphNode parentNode = new ExtendedGraphNode("");
        final GraphNode childNode = new ExtendedGraphNode("");
        parentNode.addChild(childNode);

        final Collection<GraphNode> childParentNodes = childNode.parentNodes();
        childParentNodes.remove(parentNode);

        assertEquals(1, childNode.parentNodes().size());
        assertTrue(childNode.parentNodes().contains(parentNode));
    }

    private static class ExtendedGraphNode extends GraphNode {
        ExtendedGraphNode(final String nodeName) {
            super(nodeName);
        }

        @Override
        public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {}
    }
}