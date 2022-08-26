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

package org.apache.kafka.common.acl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AclPermissionTypeTest {
    private static class AclPermissionTypeTestInfo {
        private final AclPermissionType ty;
        private final int code;
        private final String name;
        private final boolean unknown;

        AclPermissionTypeTestInfo(AclPermissionType ty, int code, String name, boolean unknown) {
            this.ty = ty;
            this.code = code;
            this.name = name;
            this.unknown = unknown;
        }
    }

    private static final AclPermissionTypeTestInfo[] INFOS = {
        new AclPermissionTypeTestInfo(AclPermissionType.UNKNOWN, 0, "unknown", true),
        new AclPermissionTypeTestInfo(AclPermissionType.ANY, 1, "any", false),
        new AclPermissionTypeTestInfo(AclPermissionType.DENY, 2, "deny", false),
        new AclPermissionTypeTestInfo(AclPermissionType.ALLOW, 3, "allow", false)
    };

    @Test
    public void testIsUnknown() throws Exception {
        for (AclPermissionTypeTestInfo info : INFOS) {
            assertEquals(info.unknown, info.ty.isUnknown(), info.ty + " was supposed to have unknown == " + info.unknown);
        }
    }

    @Test
    public void testCode() throws Exception {
        assertEquals(AclPermissionType.values().length, INFOS.length);
        for (AclPermissionTypeTestInfo info : INFOS) {
            assertEquals(info.code, info.ty.code(), info.ty + " was supposed to have code == " + info.code);
            assertEquals(info.ty, AclPermissionType.fromCode((byte) info.code),
                "AclPermissionType.fromCode(" + info.code + ") was supposed to be " +  info.ty);
        }
        assertEquals(AclPermissionType.UNKNOWN, AclPermissionType.fromCode((byte) 120));
    }

    @Test
    public void testName() throws Exception {
        for (AclPermissionTypeTestInfo info : INFOS) {
            assertEquals(info.ty, AclPermissionType.fromString(info.name),
                "AclPermissionType.fromString(" + info.name + ") was supposed to be " +  info.ty);
        }
        assertEquals(AclPermissionType.UNKNOWN, AclPermissionType.fromString("something"));
    }

    @Test
    public void testExhaustive() throws Exception {
        assertEquals(INFOS.length, AclPermissionType.values().length);
        for (int i = 0; i < INFOS.length; i++) {
            assertEquals(INFOS[i].ty, AclPermissionType.values()[i]);
        }
    }
}
