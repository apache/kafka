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
package org.apache.kafka.security.authorizer;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.ResourceType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Set;

import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class AclEntryTest {

    // Catches a new ResourceType added without a supportedOperations case, which would otherwise only fail at runtime
    // (e.g. in AuthHelper or AclCommand). authorizationError is not checked here since USER has no authorization error.
    @ParameterizedTest
    @EnumSource(value = ResourceType.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN", "ANY"})
    public void testEveryConcreteResourceTypeHasSupportedOperations(ResourceType resourceType) {
        assertFalse(AclEntry.supportedOperations(resourceType).isEmpty());
    }

    @Test
    public void testClusterMirror() {
        assertEquals(Set.of(READ, CREATE, ALTER, DELETE, DESCRIBE, ALTER_CONFIGS, DESCRIBE_CONFIGS),
            AclEntry.supportedOperations(ResourceType.CLUSTER_MIRROR));
        assertEquals(Errors.CLUSTER_MIRROR_AUTHORIZATION_FAILED, AclEntry.authorizationError(ResourceType.CLUSTER_MIRROR));
    }
}
