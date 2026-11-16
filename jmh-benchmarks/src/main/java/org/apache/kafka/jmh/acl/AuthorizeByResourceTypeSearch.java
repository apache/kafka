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
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.internals.SecurityUtils;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import org.apache.kafka.server.authorizer.internals.CidrUtils;
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
@Warmup(iterations = 7)
@Measurement(iterations = 7)
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
        authorizer = new BaselineAuthorizer();
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

        //  makeDeny(denyPattern1, aclEntries);
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

    @Benchmark
    public AuthorizationResult testAuthorizeByResourceTypeOld() {
        return authorizer.authorizeByResourceType(authorizeByResourceTypeContext, op, resourceType);
    }

    private static class BaselineAuthorizer extends StandardAuthorizer {
        // Keep the original implementation here so both paths can be measured in the same JVM.
        @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
        private AuthorizationResult authorizeByResourceTypeBaseline(AuthorizableRequestContext requestContext, AclOperation op, ResourceType resourceType) {
            SecurityUtils.authorizeByResourceTypeCheckArgs(op, resourceType);

            // Check a hard-coded name to ensure that super users are granted
            // access regardless of DENY ACLs.
            if (authorize(requestContext, Collections.singletonList(new Action(
                    op, new ResourcePattern(resourceType, "hardcode", PatternType.LITERAL),
                    0, true, false)))
                    .get(0) == AuthorizationResult.ALLOWED) {
                return AuthorizationResult.ALLOWED;
            }

            // Filter out all the resource pattern corresponding to the RequestContext,
            // AclOperation, and ResourceType
            ResourcePatternFilter resourceTypeFilter = new ResourcePatternFilter(
                    resourceType, null, PatternType.ANY);
            AclBindingFilter aclFilter = new AclBindingFilter(
                    resourceTypeFilter, AccessControlEntryFilter.ANY);

            EnumMap<PatternType, Set<String>> denyPatterns =
                    new EnumMap<>(PatternType.class) {{
                        put(PatternType.LITERAL, new HashSet<>());
                        put(PatternType.PREFIXED, new HashSet<>());
                    }};
            EnumMap<PatternType, Set<String>> allowPatterns =
                    new EnumMap<>(PatternType.class) {{
                        put(PatternType.LITERAL, new HashSet<>());
                        put(PatternType.PREFIXED, new HashSet<>());
                    }};

            boolean hasWildCardAllow = false;

            KafkaPrincipal principal = new KafkaPrincipal(
                    requestContext.principal().getPrincipalType(),
                    requestContext.principal().getName());
            String hostAddr = requestContext.clientAddress().getHostAddress();

            for (AclBinding binding : acls(aclFilter)) {
                String aclHost = binding.entry().host();
                if (!aclHost.equals(hostAddr) && !aclHost.equals("*") && !CidrUtils.isInRange(hostAddr, aclHost))
                    continue;

                if (!SecurityUtils.parseKafkaPrincipal(binding.entry().principal()).equals(principal)
                        && !binding.entry().principal().equals("User:*"))
                    continue;

                if (binding.entry().operation() != op
                        && binding.entry().operation() != AclOperation.ALL)
                    continue;

                if (binding.entry().permissionType() == AclPermissionType.DENY) {
                    switch (binding.pattern().patternType()) {
                        case LITERAL:
                            // If wildcard deny exists, return deny directly
                            if (binding.pattern().name().equals(ResourcePattern.WILDCARD_RESOURCE))
                                return AuthorizationResult.DENIED;
                            denyPatterns.get(PatternType.LITERAL).add(binding.pattern().name());
                            break;
                        case PREFIXED:
                            denyPatterns.get(PatternType.PREFIXED).add(binding.pattern().name());
                            break;
                        default:
                    }
                    continue;
                }

                if (binding.entry().permissionType() != AclPermissionType.ALLOW)
                    continue;

                switch (binding.pattern().patternType()) {
                    case LITERAL:
                        if (binding.pattern().name().equals(ResourcePattern.WILDCARD_RESOURCE)) {
                            hasWildCardAllow = true;
                            continue;
                        }
                        allowPatterns.get(PatternType.LITERAL).add(binding.pattern().name());
                        break;
                    case PREFIXED:
                        allowPatterns.get(PatternType.PREFIXED).add(binding.pattern().name());
                        break;
                    default:
                }
            }

            if (hasWildCardAllow) {
                return AuthorizationResult.ALLOWED;
            }

            // For any literal allowed, if there's no dominant literal and prefix denied, return allow.
            // For any prefix allowed, if there's no dominant prefix denied, return allow.
            for (Map.Entry<PatternType, Set<String>> entry : allowPatterns.entrySet()) {
                for (String allowStr : entry.getValue()) {
                    if (entry.getKey() == PatternType.LITERAL
                            && denyPatterns.get(PatternType.LITERAL).contains(allowStr))
                        continue;
                    StringBuilder sb = new StringBuilder();
                    boolean hasDominatedDeny = false;
                    for (char ch : allowStr.toCharArray()) {
                        sb.append(ch);
                        if (denyPatterns.get(PatternType.PREFIXED).contains(sb.toString())) {
                            hasDominatedDeny = true;
                            break;
                        }
                    }
                    if (!hasDominatedDeny)
                        return AuthorizationResult.ALLOWED;
                }
            }

            return AuthorizationResult.DENIED;
        }
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
