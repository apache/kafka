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
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.kafka.metadata.authorizer.StandardAuthorizerConstants.WILDCARD;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerConstants.WILDCARD_PRINCIPAL;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class AclLoaderTest {
    private static final Map<Uuid, StandardAcl> SNAPSHOT1;

    private static final List<Uuid> UUIDS = Arrays.asList(
        Uuid.fromString("hiIec7T4TPmZGwzRa5wxeA"),
        Uuid.fromString("2Q4EztAhTiGkOOLaL5_RnA"),
        Uuid.fromString("KH-t-UmmRHW2c7moaDL_CQ"),
        Uuid.fromString("GUlPfRhlTRK1N9uG3elrlQ"));

    private static final List<StandardAcl> ACLS = Arrays.asList(
            new StandardAcl(
                    ResourceType.TOPIC,
                    "foo_",
                    PatternType.LITERAL,
                    WILDCARD_PRINCIPAL,
                    WILDCARD,
                    AclOperation.READ,
                    AclPermissionType.ALLOW),
            new StandardAcl(
                    ResourceType.GROUP,
                    WILDCARD,
                    PatternType.LITERAL,
                    "User:foo",
                    WILDCARD,
                    AclOperation.READ,
                    AclPermissionType.DENY),
            new StandardAcl(
                    ResourceType.GROUP,
                    "mygroup",
                    PatternType.PREFIXED,
                    "User:foo",
                    WILDCARD,
                    AclOperation.READ,
                    AclPermissionType.DENY),
            new StandardAcl(
                    ResourceType.GROUP,
                    "mygroup",
                    PatternType.PREFIXED,
                    "User:bar",
                    WILDCARD,
                    AclOperation.READ,
                    AclPermissionType.DENY));

    private static final StandardAcl ACL4 = new StandardAcl(
            ResourceType.GROUP,
            "mygroup",
            PatternType.PREFIXED,
            "User:foo",
            WILDCARD,
            AclOperation.READ,
            AclPermissionType.ALLOW);

    static {
        Map<Uuid, StandardAcl> snapshot1 = new HashMap<>();
        for (int i = 0; i < UUIDS.size(); i++) {
            snapshot1.put(UUIDS.get(i), ACLS.get(i));
        }
        SNAPSHOT1 = Collections.unmodifiableMap(snapshot1);
    }

    @Test
    public void testLoadSnapshot() {
        AclLoader loader = new AclLoader(SNAPSHOT1);
        Map<Resource, ResourceAcls> newLiterals = new HashMap<>();
        newLiterals.put(new Resource(ResourceType.TOPIC, "foo_"),
                new ResourceAcls(Arrays.asList(ACLS.get(0))));
        PrefixNode myGroupNode = new PrefixNode(Collections.emptyNavigableMap(), "mygroup",
                new ResourceAcls(Arrays.asList(ACLS.get(2), ACLS.get(3))));
        NavigableMap<String, PrefixNode> rootChildren = new TreeMap<>();
        rootChildren.put("mygroup", myGroupNode);
        ResourceAclsChanges rootResourceAclsChanges = new ResourceAclsChanges();
        rootResourceAclsChanges.newAddition(ACLS.get(1));
        PrefixNode rootNode = new PrefixNode(rootChildren, "",
                ResourceAcls.EMPTY.copyWithChanges(rootResourceAclsChanges));

        Map<ResourceType, PrefixNode> newPrefixed = new HashMap<>();
        newPrefixed.put(ResourceType.GROUP, rootNode);
        assertEquals(new AclLoader.Result(SNAPSHOT1, newLiterals, newPrefixed),
            loader.build());
    }

    @Test
    public void testLoadDelta() {
        AclLoader loader = new AclLoader(SNAPSHOT1);
        AclLoader.Result result = loader.build();
        Map<Uuid, Optional<StandardAcl>> changes = new HashMap<>();
        changes.put(UUIDS.get(2), Optional.empty());
        changes.put(UUIDS.get(3), Optional.of(ACL4));
        AclLoader loader2 = new AclLoader(result.newAclsById(),
                result.newLiterals(),
                result.newPrefixed(),
                changes);
        Map<Uuid, StandardAcl> newAclsById = new HashMap<>();
        newAclsById.put(UUIDS.get(0), ACLS.get(0));
        newAclsById.put(UUIDS.get(1), ACLS.get(1));
        newAclsById.put(UUIDS.get(3), ACL4);
        Map<Resource, ResourceAcls> newLiterals = new HashMap<>();
        newLiterals.put(new Resource(ResourceType.TOPIC, "foo_"),
                new ResourceAcls(Arrays.asList(ACLS.get(0))));
        PrefixNode myGroupNode = new PrefixNode(Collections.emptyNavigableMap(), "mygroup",
                new ResourceAcls(Arrays.asList(ACLS.get(1), ACL4)));
        NavigableMap<String, PrefixNode> rootChildren = new TreeMap<>();
        rootChildren.put("mygroup", myGroupNode);
        ResourceAclsChanges rootResourceAclsChanges = new ResourceAclsChanges();
        rootResourceAclsChanges.newAddition(ACLS.get(1));
        PrefixNode rootNode = new PrefixNode(rootChildren, "",
                ResourceAcls.EMPTY.copyWithChanges(rootResourceAclsChanges));

        Map<ResourceType, PrefixNode> newPrefixed = new HashMap<>();
        newPrefixed.put(ResourceType.GROUP, rootNode);
        assertEquals(new AclLoader.Result(newAclsById, newLiterals, newPrefixed),
                loader2.build());
    }
}
