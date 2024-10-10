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

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.metadata.authorizer.AuthorizerData.WILDCARD;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class AuthorizerDataStaticTests {

    @Test
    public void findResultTest() {
        Set<KafkaPrincipal> principals = new HashSet<>();
        principals.add(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"));
        String host = "localhost";
        Action action = AuthorizerTestUtils.newAction(AclOperation.READ, ResourceType.TOPIC, "myTopic");

        assertNull(AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:bob", WILDCARD, AclOperation.CREATE, ALLOW)),
                "principals do not match");
        assertNull(AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", AclOperation.CREATE, ALLOW)),
                "null host does not match (Should never happen)");

        assertNull(AuthorizerData.findResult(action, principals, "example.com", new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", AclOperation.CREATE, ALLOW)),
                "non-matching host returns null");

        assertEquals(ALLOWED, AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", AclOperation.ALL, ALLOW)),
                "All operation matches ACL permission (ALLOW)");

        assertEquals(DENIED, AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", AclOperation.ALL, DENY)),
                "All operation matches ACL permission (DENY)");

        assertEquals(ALLOWED, AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", AclOperation.READ, ALLOW)),
                "Non ALL operation matches ACL operation returns ACL permission (ALLOWED)");

        assertEquals(DENIED, AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", AclOperation.READ, DENY)),
                "Non ALL operation matches ACL operation returns ACL permission (DENY)");

        assertNull(AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", AclOperation.WRITE, ALLOW)),
                "Non ALL operation non-matching ACL operation returns null");

        action = AuthorizerTestUtils.newAction(AclOperation.DESCRIBE, ResourceType.TOPIC, "myTopic");
        for (AclOperation oper : AclOperation.values()) {
            switch (oper) {
                case ALL:
                    // handled above
                    break;
                case DESCRIBE:
                    assertEquals(DENIED, AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", oper, DENY)),
                            "Describe operation returns ACL permission is DENY");
                    assertEquals(ALLOWED, AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", oper, ALLOW)),
                            "Describe operation returns ACL permission (ALLOW)");
                    break;
                case READ:
                case WRITE:
                case DELETE:
                case ALTER:
                    assertNull(AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", oper, DENY)),
                            () -> format("Implied describe operation (%s) returns null when ACL permission is DENY", oper));
                    assertEquals(ALLOWED, AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", oper, ALLOW)),
                            () -> format("Implied describe operation (%s) returns ACL permission (ALLOW)", oper));
                    break;
                default:
                    assertNull(AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", oper, ALLOW)),
                            () -> format("Non-implied describe operation (%s) returns null", oper));
            }
        }
        action = AuthorizerTestUtils.newAction(AclOperation.DESCRIBE_CONFIGS, ResourceType.TOPIC, "myTopic");
        for (AclOperation oper : AclOperation.values()) {
            switch (oper) {
                case ALL:
                    // handled above
                    break;
                case DESCRIBE_CONFIGS:
                    assertEquals(DENIED, AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", oper, DENY)),
                            "Describe-config operation returns ACL permission (DENY)");
                    assertEquals(ALLOWED, AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", oper, ALLOW)),
                            "Describe-config operation (%s) returns ACL permission (ALLOW)");
                    break;
                case ALTER_CONFIGS:
                    assertNull(AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", oper, DENY)),
                            () -> format("Implied describe-config operation (%s) returns null when ACL permission is DENY", oper));
                    assertEquals(ALLOWED, AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", oper, ALLOW)),
                            () -> format("Implied describe-config operation (%s) returns ACL permission (ALLOW)", oper));
                    break;
                default:
                    assertNull(AuthorizerData.findResult(action, principals, host, new StandardAcl(TOPIC, "foo_", PREFIXED, "User:alice", "localhost", oper, ALLOW)),
                            () -> format("Non-implied describe operation (%s) returns null", oper));
            }
        }
    }
}
