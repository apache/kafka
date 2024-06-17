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

package org.apache.kafka.image.node;

import org.apache.kafka.image.ClusterImage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


@Timeout(value = 40)
public class ClusterImageNodeTest {
    private final static ClusterImageNode NODE = new ClusterImageNode(ClusterImage.EMPTY);

    @Test
    public void testChildNames() {
        assertEquals(Arrays.asList("brokers", "controllers"), NODE.childNames());
    }

    @Test
    public void testBrokersChild() {
        MetadataNode child = NODE.child("brokers");
        assertNotNull(child);
        assertEquals(ClusterImageBrokersNode.class, child.getClass());
    }

    @Test
    public void testControllersChild() {
        MetadataNode child = NODE.child("controllers");
        assertNotNull(child);
        assertEquals(ClusterImageControllersNode.class, child.getClass());
    }

    @Test
    public void testUnknownChild() {
        assertNull(NODE.child("unknown"));
    }
}
