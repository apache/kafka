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
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Compares the trie implementation with the original character-by-character HashSet lookup.
 * Both methods include ACL collection and filtering and use the same authorizer and ACLs.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 7)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class AuthorizeByResourceTypeBenchmark {
    @Param({"8"})
    public int resourceNameLength;

    @Param({"10000"})
    // Number of independent resource groups, or nested ALLOW/DENY pairs for MIXED_PATTERNS.
    public int resourceCount;

    @Param({"LITERAL", "PREFIXED"})
    public PatternType allowPatternType;

    // Examples omit the shared resource prefix; ALLOW is literal or prefixed "abc".
    // EXACT_DENY: deny prefix "abc" covers the ALLOW -> DENIED.
    // ANCESTOR_DENY: deny prefix "a" covers the ALLOW; "abb" is unrelated -> DENIED.
    // UNRELATED_DENY: deny prefix "abb" does not cover the ALLOW -> ALLOWED.
    // NO_DENIES: only ALLOW ACLs exist -> ALLOWED.
    // MIXED_PATTERNS: N literal ALLOWs at P + "a", P + "aa", ... each have a matching
    // prefixed DENY, while an ALLOW at P survives -> ALLOWED. With allowPatternType=PREFIXED,
    // the baseline must check all N literal ALLOWs before reaching the surviving prefixed ALLOW.
    @Param({"MIXED_PATTERNS"})
    public String scenario;

    private BaselineAuthorizer authorizer;
    private Metrics metrics;
    private RequestContext context;

    @Setup
    public void setup() throws Exception {
        authorizer = new BaselineAuthorizer();
        metrics = new Metrics();
        authorizer.configure(Map.of());
        authorizer.withPluginMetrics(new PluginMetricsImpl(metrics, Map.of()));
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user");
        context = new RequestContext(new RequestHeader(ApiKeys.FIND_COORDINATOR, (short) 1, "benchmark", 1),
            "1", InetAddress.getLoopbackAddress(), principal, ListenerName.normalised("listener"),
            SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);

        if (scenario.equals("MIXED_PATTERNS")) {
            setupMixedPatterns(principal);
        } else {
            setupIndependentPatterns(principal);
        }
        authorizer.completeInitialLoad();

        AuthorizationResult expected = scenario.equals("EXACT_DENY") || scenario.equals("ANCESTOR_DENY")
            ? AuthorizationResult.DENIED : AuthorizationResult.ALLOWED;
        if (trie() != expected || baseline() != expected) {
            throw new IllegalStateException("Unexpected authorization result for " + scenario);
        }
    }

    private void setupMixedPatterns(KafkaPrincipal principal) {
        String prefix = "x".repeat(resourceNameLength);
        addAcl(prefix, allowPatternType, AclPermissionType.ALLOW, principal);
        // As in AuthorizeByResourceTypeSearch, resourceCount grows both the number of
        // pairs and the longest suffix. Total resource-name characters grow quadratically.
        for (int i = 1; i <= resourceCount; i++) {
            String name = prefix + "a".repeat(i);
            addAcl(name, PatternType.LITERAL, AclPermissionType.ALLOW, principal);
            addAcl(name, PatternType.PREFIXED, AclPermissionType.DENY, principal);
        }
    }

    private void setupIndependentPatterns(KafkaPrincipal principal) {
        for (int i = 0; i < resourceCount; i++) {
            String prefix = "x".repeat(resourceNameLength) + "-" + i + "-";
            addAcl(prefix + "abc", allowPatternType, AclPermissionType.ALLOW, principal);
            switch (scenario) {
                case "EXACT_DENY":
                    addAcl(prefix + "abc", PatternType.PREFIXED, AclPermissionType.DENY, principal);
                    break;
                case "ANCESTOR_DENY":
                    addAcl(prefix + "a", PatternType.PREFIXED, AclPermissionType.DENY, principal);
                    addAcl(prefix + "abb", PatternType.PREFIXED, AclPermissionType.DENY, principal);
                    break;
                case "UNRELATED_DENY":
                    addAcl(prefix + "abb", PatternType.PREFIXED, AclPermissionType.DENY, principal);
                    break;
                case "NO_DENIES":
                    break;
                default:
                    throw new IllegalArgumentException("Unknown scenario " + scenario);
            }
        }
    }

    private void addAcl(String name, PatternType type, AclPermissionType permission, KafkaPrincipal principal) {
        AclBinding binding = new AclBinding(new ResourcePattern(ResourceType.GROUP, name, type),
            new AccessControlEntry(principal.toString(), "*", AclOperation.READ, permission));
        authorizer.addAcl(Uuid.randomUuid(), StandardAcl.fromAclBinding(binding));
    }

    @TearDown
    public void tearDown() throws IOException {
        authorizer.close();
        metrics.close();
    }

    @Benchmark
    public AuthorizationResult trie() {
        return authorizer.authorizeByResourceType(context, AclOperation.READ, ResourceType.GROUP);
    }

    @Benchmark
    public AuthorizationResult baseline() {
        return authorizer.authorizeByResourceTypeBaseline(context, AclOperation.READ, ResourceType.GROUP);
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
                .include(AuthorizeByResourceTypeBenchmark.class.getSimpleName())
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
