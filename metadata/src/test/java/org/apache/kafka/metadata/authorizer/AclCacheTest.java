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

package org.apache.kafka.metadata.authorizer;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AclCacheTest {
    @Test
    public void testAclsByPrincipalIsUpdatedWhenAclsAreAddedAndRemoved() {
        StandardAcl userAcl = new StandardAcl(TOPIC, "topic", LITERAL, "User:alice", "*", READ, ALLOW);
        StandardAcl wildcardAcl = new StandardAcl(TOPIC, "other-topic", LITERAL, "User:*", "*", READ, ALLOW);
        Uuid userAclId = new Uuid(1, 1);
        Uuid wildcardAclId = new Uuid(2, 2);

        AclCache cache = new AclCache()
            .addAcl(userAclId, userAcl)
            .addAcl(wildcardAclId, wildcardAcl);

        assertEquals(Set.of(userAcl), cache.aclsByPrincipal("User:alice"));
        assertEquals(Set.of(wildcardAcl), cache.aclsByPrincipal("User:*"));
        assertTrue(cache.aclsByPrincipal("User:bob").isEmpty());

        cache = cache.removeAcl(userAclId);

        assertTrue(cache.aclsByPrincipal("User:alice").isEmpty());
        assertEquals(Set.of(wildcardAcl), cache.aclsByPrincipal("User:*"));
    }
}
