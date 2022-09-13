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
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;

import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.security.auth.KafkaPrincipal.USER_TYPE;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class MatchingRuleTest {
    static final KafkaPrincipal LARRY = new KafkaPrincipal(USER_TYPE, "larry");

    @Test
    public void testDefaultRule() {
        assertEquals(DENIED, DefaultRule.DENIED.result());
        assertEquals(ALLOWED, new DefaultRule(ALLOWED).result());
        assertEquals("DefaultDeny", DefaultRule.DENIED.toString());
        assertEquals("DefaultAllow", new DefaultRule(ALLOWED).toString());
    }

    @Test
    public void testSuperUserRule() {
        assertEquals(ALLOWED, SuperUserRule.INSTANCE.result());
        assertEquals("SuperUser", SuperUserRule.INSTANCE.toString());
    }

    @Test
    public void testMatchingAclRule() {
        StandardAcl acl1 = new StandardAcl(
                TOPIC,
                "bar",
                LITERAL,
                LARRY.toString(),
                "127.0.0.1",
                AclOperation.READ,
                AclPermissionType.ALLOW);
        MatchingAclRule rule = new MatchingAclRule(acl1, ALLOWED);
        assertEquals(ALLOWED, rule.result());
        assertEquals("MatchingAcl(acl=StandardAcl(resourceType=TOPIC, resourceName=bar, " +
            "patternType=LITERAL, principal=User:larry, host=127.0.0.1, operation=READ, " +
                "permissionType=ALLOW))", rule.toString());
    }

    @Test
    public void testAppendResourcePattern() throws Exception {
        Action action = new Action(WRITE,
            new ResourcePattern(TOPIC, "foo", LITERAL), 1, true, true);
        assertEquals("Principal = User:larry is Allowed operation = WRITE from host = 127.0.0.1 " +
            "on resource = Topic:LITERAL:foo for request = Fetch with resourceRefCount = 1 based " +
            "on rule SuperUser", SuperUserRule.INSTANCE.buildAuditMessage(
                LARRY, new MockAuthorizableRequestContext.Builder().
                    setClientAddress(InetAddress.getByName("127.0.0.1")).build(),
                        action));
    }
}
