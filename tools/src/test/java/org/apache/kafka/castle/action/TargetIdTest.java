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

package org.apache.kafka.castle.action;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TargetIdTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testParseId() throws Throwable {
        assertEquals(new TargetId("foo"),  TargetId.parse("foo"));
        testParseAndToString(new TargetId("foo"), "foo");
        assertEquals(new TargetId("bar"), TargetId.parse("bar"));
        testParseAndToString(new TargetId("bar", "node3"), "bar:node3");
    }

    private void testParseAndToString(TargetId targetId, String str) {
        assertEquals(targetId, TargetId.parse(str));
        assertEquals(str, targetId.toString());
    }

    @Test
    public void testActionIdEquals() throws Throwable {
        assertEquals(new TargetId("foo", "node1"),
            new TargetId("foo", "node1"));
        assertEquals(new TargetId("baz", "node3"),
            new TargetId("baz", "node3"));
        assertEquals(new TargetId("baz"),
            new TargetId("baz"));
    }

    @Test
    public void testHasGlobalScope() throws Throwable {
        assertFalse(new TargetId("foo", "node1").hasGlobalScope());
        assertTrue(new TargetId("foo").hasGlobalScope());
        assertTrue(new TargetId("").hasGlobalScope());
    }

    @Test
    public void testToActionId() throws Throwable {
        Set<String> nodeNames = new HashSet<>(Arrays.asList("node0", "node1"));
        TargetId targetId0 = new TargetId("foo", "node0");
        Set<ActionId> actionIds0 = targetId0.toActionIds(nodeNames);
        assertEquals(1, actionIds0.size());
        assertEquals(new ActionId("foo", "node0"), actionIds0.iterator().next());
        TargetId targetId1 = new TargetId("foo");
        Set<ActionId> actionIds1 = targetId1.toActionIds(nodeNames);
        assertEquals(2, actionIds1.size());
        assertTrue(actionIds1.contains(new ActionId("foo", "node0")));
        assertTrue(actionIds1.contains(new ActionId("foo", "node1")));
    }
};
