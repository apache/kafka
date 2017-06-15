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
package org.apache.kafka.common.resource;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ResourceTypeTest {
    private static class AclResourceTypeTestInfo {
        private final ResourceType resourceType;
        private final int code;
        private final String name;
        private final boolean unknown;

        AclResourceTypeTestInfo(ResourceType resourceType, int code, String name, boolean unknown) {
            this.resourceType = resourceType;
            this.code = code;
            this.name = name;
            this.unknown = unknown;
        }
    }

    private static final AclResourceTypeTestInfo[] INFOS = {
        new AclResourceTypeTestInfo(ResourceType.UNKNOWN, 0, "unknown", true),
        new AclResourceTypeTestInfo(ResourceType.ANY, 1, "any", false),
        new AclResourceTypeTestInfo(ResourceType.TOPIC, 2, "topic", false),
        new AclResourceTypeTestInfo(ResourceType.GROUP, 3, "group", false),
        new AclResourceTypeTestInfo(ResourceType.CLUSTER, 4, "cluster", false),
        new AclResourceTypeTestInfo(ResourceType.TRANSACTIONAL_ID, 5, "transactional_id", false)
    };

    @Test
    public void testIsUnknown() throws Exception {
        for (AclResourceTypeTestInfo info : INFOS) {
            assertEquals(info.resourceType + " was supposed to have unknown == " + info.unknown,
                info.unknown, info.resourceType.isUnknown());
        }
    }

    @Test
    public void testCode() throws Exception {
        assertEquals(ResourceType.values().length, INFOS.length);
        for (AclResourceTypeTestInfo info : INFOS) {
            assertEquals(info.resourceType + " was supposed to have code == " + info.code,
                info.code, info.resourceType.code());
            assertEquals("AclResourceType.fromCode(" + info.code + ") was supposed to be " +
                info.resourceType, info.resourceType, ResourceType.fromCode((byte) info.code));
        }
        assertEquals(ResourceType.UNKNOWN, ResourceType.fromCode((byte) 120));
    }

    @Test
    public void testName() throws Exception {
        for (AclResourceTypeTestInfo info : INFOS) {
            assertEquals("ResourceType.fromString(" + info.name + ") was supposed to be " +
                info.resourceType, info.resourceType, ResourceType.fromString(info.name));
        }
        assertEquals(ResourceType.UNKNOWN, ResourceType.fromString("something"));
    }

    @Test
    public void testExhaustive() throws Exception {
        assertEquals(INFOS.length, ResourceType.values().length);
        for (int i = 0; i < INFOS.length; i++) {
            assertEquals(INFOS[i].resourceType, ResourceType.values()[i]);
        }
    }
}
