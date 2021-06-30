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

public class AclOperationTest {
    private static class AclOperationTestInfo {
        private final AclOperation operation;
        private final int code;
        private final String name;
        private final boolean unknown;

        AclOperationTestInfo(AclOperation operation, int code, String name, boolean unknown) {
            this.operation = operation;
            this.code = code;
            this.name = name;
            this.unknown = unknown;
        }
    }

    private static final AclOperationTestInfo[] INFOS = {
        new AclOperationTestInfo(AclOperation.UNKNOWN, 0, "unknown", true),
        new AclOperationTestInfo(AclOperation.ANY, 1, "any", false),
        new AclOperationTestInfo(AclOperation.ALL, 2, "all", false),
        new AclOperationTestInfo(AclOperation.READ, 3, "read", false),
        new AclOperationTestInfo(AclOperation.WRITE, 4, "write", false),
        new AclOperationTestInfo(AclOperation.CREATE, 5, "create", false),
        new AclOperationTestInfo(AclOperation.DELETE, 6, "delete", false),
        new AclOperationTestInfo(AclOperation.ALTER, 7, "alter", false),
        new AclOperationTestInfo(AclOperation.DESCRIBE, 8, "describe", false),
        new AclOperationTestInfo(AclOperation.CLUSTER_ACTION, 9, "cluster_action", false),
        new AclOperationTestInfo(AclOperation.DESCRIBE_CONFIGS, 10, "describe_configs", false),
        new AclOperationTestInfo(AclOperation.ALTER_CONFIGS, 11, "alter_configs", false),
        new AclOperationTestInfo(AclOperation.IDEMPOTENT_WRITE, 12, "idempotent_write", false)
    };

    @Test
    public void testIsUnknown() throws Exception {
        for (AclOperationTestInfo info : INFOS) {
            assertEquals(info.unknown, info.operation.isUnknown(),
                info.operation + " was supposed to have unknown == " + info.unknown);
        }
    }

    @Test
    public void testCode() throws Exception {
        assertEquals(AclOperation.values().length, INFOS.length);
        for (AclOperationTestInfo info : INFOS) {
            assertEquals(info.code, info.operation.code(), info.operation + " was supposed to have code == " + info.code);
            assertEquals(info.operation, AclOperation.fromCode((byte) info.code),
                "AclOperation.fromCode(" + info.code + ") was supposed to be " +  info.operation);
        }
        assertEquals(AclOperation.UNKNOWN, AclOperation.fromCode((byte) 120));
    }

    @Test
    public void testName() throws Exception {
        for (AclOperationTestInfo info : INFOS) {
            assertEquals(info.operation, AclOperation.fromString(info.name),
                "AclOperation.fromString(" + info.name + ") was supposed to be " +  info.operation);
        }
        assertEquals(AclOperation.UNKNOWN, AclOperation.fromString("something"));
    }

    @Test
    public void testExhaustive() {
        assertEquals(INFOS.length, AclOperation.values().length);
        for (int i = 0; i < INFOS.length; i++) {
            assertEquals(INFOS[i].operation, AclOperation.values()[i]);
        }
    }
}
