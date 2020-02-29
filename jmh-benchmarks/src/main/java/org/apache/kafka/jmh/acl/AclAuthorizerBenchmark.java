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
import kafka.security.authorizer.AclEntry;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.mockito.internal.util.reflection.FieldSetter;
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
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.TreeMap;
import scala.math.Ordering;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)

public class AclAuthorizerBenchmark {
    @Param({"5000", "10000", "50000"})
    public static Integer resourceCount;
    //no. of. rules per resource
    @Param({"5", "10"})
    public static Integer aclCount;

    private AclAuthorizer aclAuthorizer = new AclAuthorizer();
    private KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user");

    public static void main(String[] args) throws NoSuchFieldException {
        AclAuthorizerBenchmark aclAuthorizerBenchmark = new AclAuthorizerBenchmark();
        aclAuthorizerBenchmark.setup();
        aclAuthorizerBenchmark.testAclsIterator();
    }

    @Setup(Level.Trial)
    public void setup() throws NoSuchFieldException {
        FieldSetter.setField(aclAuthorizer, AclAuthorizer.class.getDeclaredField("aclCache"),
            prepareAclCache());
    }

    private TreeMap prepareAclCache() {
        Random random = new Random();
        Map<ResourcePattern, java.util.Set<AclEntry>> aclEntries = new HashMap<>();

        for (int i = 0; i < resourceCount; i++) {
            // set TOPIC, GROUP, or CLUSTER
            int resourceCode = random.nextInt(3) + 2;
            ResourceType resourceType = ResourceType.fromCode((byte) resourceCode);

            // set LITERAL or PREFIXED
            int pattenCode = random.nextInt(2) + 3;
            PatternType pattenType = PatternType.fromCode((byte) pattenCode);

            ResourcePattern resource = new ResourcePattern(resourceType, "resource-" + i, pattenType);
            java.util.Set<AclEntry> entries = aclEntries.computeIfAbsent(resource, k -> new HashSet<>());

            for (int j = 0; j < aclCount; j++) {
                AccessControlEntry ace =
                    new AccessControlEntry(principal.toString(), "*", AclOperation.READ, AclPermissionType.ALLOW);
                entries.add(new AclEntry(ace));
            }
        }

        TreeMap<ResourcePattern, AclAuthorizer.VersionedAcls> aclCache = new TreeMap<>(new ResourceOrdering());
        for (Map.Entry<ResourcePattern, java.util.Set<AclEntry>> entry : aclEntries.entrySet()) {
            aclCache = aclCache.$plus(new Tuple2<>(entry.getKey(),
                new AclAuthorizer.VersionedAcls(JavaConverters.asScalaSet(entry.getValue()).toSet(), 1)));
        }
        return aclCache;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        aclAuthorizer.close();
    }

    @Benchmark
    public void testAclsIterator() {
        aclAuthorizer.acls(AclBindingFilter.ANY);
    }

    /*
    @Benchmark
    public void testMatchingAcls() {

        aclAuthorizer.authorize()
        AclBindingFilter filter =
            new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC, "resource-10", PatternType.PREFIXED),
                AccessControlEntryFilter.ANY);
        //new AccessControlEntryFilter("User:ANONYMOUS", "127.0.0.1", AclOperation.READ, AclPermissionType.DENY));
        //Iterable acls = aclAuthorizer.acls(AclBindingFilter.ANY);
        Iterable acls = aclAuthorizer.acls(filter);
        int counter = 0;
        for (Object i : acls) {
            if (((AclBinding)i).pattern().patternType() == PatternType.PREFIXED)
                System.out.println(i);

            counter++;
        }
        System.out.println("acls size: " + counter);
    }

    */

    private class ResourceOrdering implements Ordering<ResourcePattern> {
        @Override
        public int compare(final ResourcePattern a, final ResourcePattern b) {
            int rt = a.resourceType().compareTo(b.resourceType());
            if (rt != 0) {
                return rt;
            } else {
                int rnt = a.patternType().compareTo(b.patternType());
                if (rnt != 0)
                    return rnt;
                else {
                    return a.name().compareTo(b.name()) * -1;
                }
            }
        }
    }
}
