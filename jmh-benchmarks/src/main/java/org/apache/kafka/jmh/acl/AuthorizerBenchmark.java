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

package org.apache.kafka.jmh.acl;

import kafka.security.authorizer.AclAuthorizer;
import kafka.security.authorizer.AclAuthorizer.VersionedAcls;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.security.authorizer.AclEntry;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.Authorizer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AuthorizerBenchmark {

    public enum AuthorizerType {
        ACL(AclAuthorizer::new),
        KRAFT(StandardAuthorizer::new);

        private final Supplier<Authorizer> supplier;

        AuthorizerType(Supplier<Authorizer> supplier) {
            this.supplier = supplier;
        }

        Authorizer newAuthorizer() {
            return supplier.get();
        }
    }

    @Param({"10000", "50000", "200000"})
    private int resourceCount;
    //no. of. rules per resource
    @Param({"10", "50"})
    private int aclCount;

    @Param({"0", "20", "50", "90", "99", "99.9", "99.99", "100"})
    private double denyPercentage;

    @Param({"ACL", "KRAFT"})
    private AuthorizerType authorizerType;

    private final int hostPreCount = 1000;
    private final String resourceNamePrefix = "foo-bar35_resource-";
    private final KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user");
    private final String authorizeByResourceTypeHostName = "127.0.0.2";
    private final HashMap<ResourcePattern, AclAuthorizer.VersionedAcls> aclToUpdate = new HashMap<>();
    private Authorizer authorizer;
    private List<Action> actions = new ArrayList<>();
    private RequestContext authorizeContext;
    private RequestContext authorizeByResourceTypeContext;

    Random rand = new Random(System.currentTimeMillis());
    double eps = 1e-9;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        authorizer = authorizerType.newAuthorizer();
        prepareAclCache();
        prepareAclToUpdate();
        // By adding `-95` to the resource name prefix, we cause the `TreeMap.from/to` call to return
        // most map entries. In such cases, we rely on the filtering based on `String.startsWith`
        // to return the matching ACLs. Using a more efficient data structure (e.g. a prefix
        // tree) should improve performance significantly).
        actions = Collections.singletonList(new Action(AclOperation.WRITE,
            new ResourcePattern(ResourceType.TOPIC, resourceNamePrefix + 95, PatternType.LITERAL),
            1, true, true));
        authorizeContext = new RequestContext(new RequestHeader(ApiKeys.PRODUCE, Integer.valueOf(1).shortValue(),
            "someclient", 1), "1", InetAddress.getByName("127.0.0.1"), principal,
            ListenerName.normalised("listener"), SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);
        authorizeByResourceTypeContext = new RequestContext(new RequestHeader(ApiKeys.PRODUCE, Integer.valueOf(1).shortValue(),
            "someclient", 1), "1", InetAddress.getByName(authorizeByResourceTypeHostName), principal,
            ListenerName.normalised("listener"), SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);
    }

    private void prepareAclCache() {
        Map<ResourcePattern, Set<AclEntry>> aclEntries = new HashMap<>();
        for (int resourceId = 0; resourceId < resourceCount; resourceId++) {
            ResourcePattern resource = new ResourcePattern(
                (resourceId % 10 == 0) ? ResourceType.GROUP : ResourceType.TOPIC,
                resourceNamePrefix + resourceId,
                (resourceId % 5 == 0) ? PatternType.PREFIXED : PatternType.LITERAL);

            Set<AclEntry> entries = aclEntries.computeIfAbsent(resource, k -> new HashSet<>());

            for (int aclId = 0; aclId < aclCount; aclId++) {
                // The principal in the request context we are using
                // is principal.toString without any suffix
                String principalName = principal.toString() + (aclId == 0 ? "" : aclId);
                AccessControlEntry allowAce = new AccessControlEntry(
                    principalName, "*", AclOperation.READ, AclPermissionType.ALLOW);

                entries.add(new AclEntry(allowAce));

                if (shouldDeny()) {
                    // dominantly deny the resource
                    AccessControlEntry denyAce = new AccessControlEntry(
                        principalName, "*", AclOperation.READ, AclPermissionType.DENY);
                    entries.add(new AclEntry(denyAce));
                }
            }
        }

        ResourcePattern resourcePrefix = new ResourcePattern(ResourceType.TOPIC, resourceNamePrefix,
            PatternType.PREFIXED);
        Set<AclEntry> entriesPrefix = aclEntries.computeIfAbsent(resourcePrefix, k -> new HashSet<>());
        for (int hostId = 0; hostId < hostPreCount; hostId++) {
            AccessControlEntry allowAce = new AccessControlEntry(principal.toString(), "127.0.0." + hostId,
                AclOperation.READ, AclPermissionType.ALLOW);
            entriesPrefix.add(new AclEntry(allowAce));

            if (shouldDeny()) {
                // dominantly deny the resource
                AccessControlEntry denyAce = new AccessControlEntry(principal.toString(), "127.0.0." + hostId,
                    AclOperation.READ, AclPermissionType.DENY);
                entriesPrefix.add(new AclEntry(denyAce));
            }
        }

        ResourcePattern resourceWildcard = new ResourcePattern(ResourceType.TOPIC, ResourcePattern.WILDCARD_RESOURCE,
            PatternType.LITERAL);
        Set<AclEntry> entriesWildcard = aclEntries.computeIfAbsent(resourceWildcard, k -> new HashSet<>());
        // get dynamic entries number for wildcard acl
        for (int hostId = 0; hostId < resourceCount / 10; hostId++) {
            String hostName = "127.0.0" + hostId;
            // AuthorizeByResourceType is optimizing the wildcard deny case.
            // If we didn't skip the host, we would end up having a biased short runtime.
            if (hostName.equals(authorizeByResourceTypeHostName)) {
                continue;
            }

            AccessControlEntry allowAce = new AccessControlEntry(principal.toString(), hostName,
                AclOperation.READ, AclPermissionType.ALLOW);
            entriesWildcard.add(new AclEntry(allowAce));
            if (shouldDeny()) {
                AccessControlEntry denyAce = new AccessControlEntry(principal.toString(), hostName,
                    AclOperation.READ, AclPermissionType.DENY);
                entriesWildcard.add(new AclEntry(denyAce));
            }
        }

        setupAcls(aclEntries);
    }

    private void setupAcls(Map<ResourcePattern, Set<AclEntry>> aclEntries) {
        for (Map.Entry<ResourcePattern, Set<AclEntry>> entryMap : aclEntries.entrySet()) {
            ResourcePattern resourcePattern = entryMap.getKey();
            switch (authorizerType) {
                case ACL:
                    ((AclAuthorizer) authorizer).updateCache(resourcePattern,
                        new VersionedAcls(JavaConverters.asScalaSetConverter(entryMap.getValue()).asScala().toSet(), 1));
                    break;
                case KRAFT:

                    for (AclEntry aclEntry : entryMap.getValue()) {
                        StandardAcl standardAcl = StandardAcl.fromAclBinding(new AclBinding(resourcePattern, aclEntry));
                        ((StandardAuthorizer) authorizer).addAcl(Uuid.randomUuid(), standardAcl);

                    }
                    ((StandardAuthorizer) authorizer).completeInitialLoad();
                    break;
            }
        }
    }

    private void prepareAclToUpdate() {
        scala.collection.mutable.Set<AclEntry> entries = new scala.collection.mutable.HashSet<>();
        for (int i = 0; i < resourceCount; i++) {
            scala.collection.immutable.Set<AclEntry> immutable = new scala.collection.immutable.HashSet<>();
            for (int j = 0; j < aclCount; j++) {
                entries.add(new AclEntry(new AccessControlEntry(
                    principal.toString(), "127.0.0" + j, AclOperation.WRITE, AclPermissionType.ALLOW)));
                immutable = entries.toSet();
            }
            aclToUpdate.put(
                new ResourcePattern(ResourceType.TOPIC, randomResourceName(resourceNamePrefix), PatternType.LITERAL),
                new AclAuthorizer.VersionedAcls(immutable, i));
        }
    }

    private String randomResourceName(String prefix) {
        return prefix + UUID.randomUUID().toString().substring(0, 5);
    }

    private Boolean shouldDeny() {
        return rand.nextDouble() * 100.0 - eps < denyPercentage;
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        authorizer.close();
    }

    @Benchmark
    public void testAclsIterator() {
        authorizer.acls(AclBindingFilter.ANY);
    }

    @Benchmark
    public void testAuthorizer() {
        authorizer.authorize(authorizeContext, actions);
    }

    @Benchmark
    public void testAuthorizeByResourceType() {
        authorizer.authorizeByResourceType(authorizeByResourceTypeContext, AclOperation.READ, ResourceType.TOPIC);
    }

    @Benchmark
    public void testUpdateCache() {
        if (authorizerType == AuthorizerType.ACL) {
            for (Map.Entry<ResourcePattern, VersionedAcls> e : aclToUpdate.entrySet()) {
                ((AclAuthorizer) authorizer).updateCache(e.getKey(), e.getValue());
            }
        }
    }
}
