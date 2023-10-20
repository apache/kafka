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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.metadata.ControllerRegistration;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


@Timeout(value = 40)
public class ClusterImageControllersNodeTest {
    private static final ClusterImage TEST_IMAGE = new ClusterImage(
            Collections.emptyMap(),
            Collections.singletonMap(2, new ControllerRegistration.Builder().
                    setId(2).
                    setIncarnationId(Uuid.fromString("adGo6sTPS0uJshjvdTUmqQ")).
                    setZkMigrationReady(false).
                    setSupportedFeatures(Collections.singletonMap(
                            MetadataVersion.FEATURE_NAME, VersionRange.of(1, 4))).
                    setListeners(Collections.emptyMap()).
                        build()));

    private final static ClusterImageControllersNode NODE = new ClusterImageControllersNode(TEST_IMAGE);

    @Test
    public void testChildNames() {
        assertEquals(Arrays.asList("2"), NODE.childNames());
    }

    @Test
    public void testNode1Child() {
        MetadataNode child = NODE.child("2");
        assertNotNull(child);
        assertEquals("ControllerRegistration(id=2, " +
            "incarnationId=adGo6sTPS0uJshjvdTUmqQ, " +
            "zkMigrationReady=false, " +
            "listeners=[], " +
            "supportedFeatures={metadata.version: 1-4})",
            child.stringify());
    }

    @Test
    public void testUnknownChild() {
        assertNull(NODE.child("1"));
    }
}
