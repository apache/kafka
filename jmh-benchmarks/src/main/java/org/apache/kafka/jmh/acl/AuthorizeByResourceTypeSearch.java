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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
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
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark                                                     (resourceNamePrefix)            Mode  Cnt   Score   Error  Units
 * AuthorizeByResourceTypeSearch.testAuthorizeByResourceType     AuthorizeByResourceTypeSearch-  avgt    7   4.252 ± 0.024  us/op
 * AuthorizeByResourceTypeSearch.testAuthorizeByResourceType     Authorize...Check-              avgt    7   4.301 ± 0.030  us/op
 * AuthorizeByResourceTypeSearch.testAuthorizeByResourceType     Authorize...Difference-         avgt    7   4.592 ± 0.042  us/op
 * <p>
 *
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AuthorizeByResourceTypeSearch {

    @Param({
            "AuthorizeByResourceTypeSearch-",
            "AuthorizeByResourceTypeSearchOneMoreWordLongForDenyPatternCheck-",
            "AuthorizeByResourceTypeSearchOneMoreWordLongForDenyPatternCheckAndWeAddOneMoreWordJustInCaseToShowBenchmarkDifference-",
    })
    String resourceNamePrefix;

    @Param({"4","10"})
    int typeOfPrefixedAndLiteralPattern;

    private KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user");
    private String authorizeByResourceTypeHostName = "127.0.0.2";
    private StandardAuthorizer authorizer;
    private RequestContext authorizeByResourceTypeContext;
    private AclBindingFilter filter;
    private AclOperation op;
    private ResourceType resourceType;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        authorizer = new StandardAuthorizer();
        filter = AclBindingFilter.ANY;
        op = AclOperation.READ;
        resourceType = ResourceType.TOPIC;
        prepareAclCache();
        authorizeByResourceTypeContext = new RequestContext(new RequestHeader(ApiKeys.PRODUCE, Integer.valueOf(1).shortValue(),
                "someclient", 1), "1", InetAddress.getByName(authorizeByResourceTypeHostName), principal,
                ListenerName.normalised("listener"), SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);
    }


    /**
     * What we do in this test:
     * <p>
     * For every Allow Literal Pattern -- iterate on count of typeOfPrefixedAndLiteralPattern
     *      check in denyPatternsLiteral map  -- check on exists then no found
     *      check in denyPatternsPrefixed   -- find in denyPatternsPrefixed prefix
     * return if allow (in this test we don't find)
     * <p>
     * for every Allow Prefix pattern
     *     check in denyPatternsPrefixed   -- find in denyPatternsPrefixed prefix
     * return allow
     * <p>
     * return deny - never reach
     * <p>
     * We iterate
     * typeOfPrefixedAndLiteralPattern + 1 counts.
     * Make (typeOfPrefixedAndLiteralPattern + 1) * 2 search in PatriciaTrie
     */
    private void prepareAclCache() {
        Map<ResourcePattern, Set<AccessControlEntry>> aclEntries = new HashMap<>();

        String prefix = "a";

        List<String> patterns = new ArrayList<>();
        for(int i = 0; i< typeOfPrefixedAndLiteralPattern; i++) {
            patterns.add(resourceNamePrefix + prefix.repeat(i+1));
        }

        String allowPattern = resourceNamePrefix;

        for(String pattern : patterns) {
            // PREFIX DENY
            makeDeny(pattern, aclEntries, PatternType.PREFIXED);
            // ALLOW LITERAL
            makeAllow(pattern, aclEntries, PatternType.LITERAL);
        }

        // Add one Allow
        makeAllow(allowPattern, aclEntries, PatternType.PREFIXED);


        //    makeDeny(denyPattern1, aclEntries);
        setupAcls(aclEntries);
    }

    private void makeDeny(String denyPattern, Map<ResourcePattern, Set<AccessControlEntry>> aclEntries, PatternType patternType) {
        ResourcePattern resource = new ResourcePattern(ResourceType.TOPIC, denyPattern,
                patternType);

        Set<AccessControlEntry> entries = aclEntries.computeIfAbsent(resource, k -> new HashSet<>());

        AccessControlEntry denyAce = new AccessControlEntry(principal.toString(), authorizeByResourceTypeHostName,
                AclOperation.READ, AclPermissionType.DENY);

        entries.add(denyAce);
    }

    private void makeAllow(String denyPattern, Map<ResourcePattern, Set<AccessControlEntry>> aclEntries, PatternType patternType) {
        ResourcePattern resourceAllow = new ResourcePattern(ResourceType.TOPIC, denyPattern,
                patternType);

        Set<AccessControlEntry> entriesAllow = aclEntries.computeIfAbsent(resourceAllow, k -> new HashSet<>());

        AccessControlEntry allowAce = new AccessControlEntry(principal.toString(), authorizeByResourceTypeHostName,
                AclOperation.READ, AclPermissionType.ALLOW);

        entriesAllow.add(allowAce);
    }

    private void setupAcls(Map<ResourcePattern, Set<AccessControlEntry>> aclEntries) {
        for (Map.Entry<ResourcePattern, Set<AccessControlEntry>> entryMap : aclEntries.entrySet()) {
            ResourcePattern resourcePattern = entryMap.getKey();

            for (AccessControlEntry accessControlEntry : entryMap.getValue()) {
                StandardAcl standardAcl = StandardAcl.fromAclBinding(new AclBinding(resourcePattern, accessControlEntry));
                authorizer.addAcl(Uuid.randomUuid(), standardAcl);
            }
            authorizer.completeInitialLoad();

        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        authorizer.withPluginMetrics(new PluginMetricsImpl(new Metrics(), new HashMap<>(1000000)));
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        authorizer.close();
    }

    @Benchmark
    public AuthorizationResult testAuthorizeByResourceType() {
        return authorizer.authorizeByResourceType(authorizeByResourceTypeContext, op, resourceType);
    }

    public static void main(String[] args) {
        Options opt = new OptionsBuilder()
                .include(AuthorizeByResourceTypeSearch.class.getSimpleName())
                .warmupIterations(7)
                .warmupTime(TimeValue.seconds(1))
                .measurementIterations(7)
                .measurementTime(TimeValue.seconds(1))
                .timeUnit(TimeUnit.MICROSECONDS)
                .forks(1)
                .build();
        try {
            new Runner(opt).run();
        } catch (RunnerException e) {
            e.printStackTrace();
        }
    }
}
