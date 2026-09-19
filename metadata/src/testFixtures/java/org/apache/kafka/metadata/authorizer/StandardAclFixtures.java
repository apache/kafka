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
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceType;

import java.util.List;

import static org.apache.kafka.metadata.authorizer.StandardAuthorizerData.WILDCARD;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerData.WILDCARD_PRINCIPAL;

public final class StandardAclFixtures {
    public static final List<StandardAcl> TEST_ACLS = List.of(
        new StandardAcl(
            ResourceType.CLUSTER,
            Resource.CLUSTER_NAME,
            PatternType.LITERAL,
            WILDCARD_PRINCIPAL,
            WILDCARD,
            AclOperation.ALTER,
            AclPermissionType.ALLOW
        ),
        new StandardAcl(
            ResourceType.TOPIC,
            "foo_",
            PatternType.PREFIXED,
            WILDCARD_PRINCIPAL,
            WILDCARD,
            AclOperation.READ,
            AclPermissionType.ALLOW
        ),
        new StandardAcl(
            ResourceType.GROUP,
            "mygroup",
            PatternType.LITERAL,
            "User:foo",
            WILDCARD,
            AclOperation.READ,
            AclPermissionType.DENY
        ),
        new StandardAcl(
            ResourceType.GROUP,
            "mygroup",
            PatternType.PREFIXED,
            "User:foo",
            WILDCARD,
            AclOperation.READ,
            AclPermissionType.DENY
        ),
        new StandardAcl(
            ResourceType.GROUP,
            "foo",
            PatternType.PREFIXED,
            "User:foo",
            WILDCARD,
            AclOperation.READ,
            AclPermissionType.DENY
        )
    );

    private StandardAclFixtures() {
    }
}
