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
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

import java.net.InetAddress;

import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.security.auth.KafkaPrincipal.USER_TYPE;

public final class AuthorizerTestUtils {

    private AuthorizerTestUtils() {
        // do not instantiate
    }

    static Action newAction(AclOperation aclOperation,
                            ResourceType resourceType,
                            String resourceName) {
        return new Action(aclOperation,
                new ResourcePattern(resourceType, resourceName, LITERAL), 1, false, false);
    }

    static StandardAclWithId withId(StandardAcl acl) {
        return new StandardAclWithId(new Uuid(acl.hashCode(), acl.hashCode()), acl);
    }

    public static AuthorizableRequestContext newRequestContext(String principal) throws Exception {
        return new MockAuthorizableRequestContext.Builder()
                .setPrincipal(new KafkaPrincipal(USER_TYPE, principal))
                .build();
    }

    public static AuthorizableRequestContext newRequestContext(String principal, InetAddress clientAddress) throws Exception {
        return new MockAuthorizableRequestContext.Builder()
                .setPrincipal(new KafkaPrincipal(USER_TYPE, principal))
                .setClientAddress(clientAddress)
                .build();
    }
}
