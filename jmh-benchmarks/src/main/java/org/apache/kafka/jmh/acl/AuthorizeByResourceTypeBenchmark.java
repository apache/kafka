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
import org.apache.kafka.common.acl.AclBinding;
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
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.AuthorizationResult;

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
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Compares the length-aware implementation with the original character-by-character HashSet lookup.
 * Both methods include ACL collection and filtering and use the same authorizer and ACLs.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 7, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 7, timeUnit = TimeUnit.SECONDS, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class AuthorizeByResourceTypeBenchmark {
    // Complete name length; the surviving mixed ALLOW and ancestor DENYs may be shorter.
    @Param({"8","32","128","512"})
    public int resourceNameLength;

    @Param({"1","10","50"})
    // resource name = <sharedPrefixLength> + number + abc(hardcoded suffix)
    // for example
    // sharedPrefixLength = 3; resourceCount = 100;  resourceNameLength = 8
    // you get xxx01abc, xxx02abc ...
    // haredPrefixLength = 1; resourceCount = 100;  resourceNameLength = 8
    // you get x01xxabc, x02xxabc ...
    // More resourceCount, the more you need characters to unique identify acl
    public int sharedPrefixLength;

    @Param({"10","100","1000","10000"})
    // Number of independent resource groups, or ALLOW/DENY pairs for MIXED_PATTERNS.
    public int resourceCount;

    @Param({"LITERAL", "PREFIXED"})
    public PatternType allowPatternType;

    // Examples omit the shared resource prefix; ALLOW is literal or prefixed "abc".
    // EXACT_DENY: deny prefix "abc" covers the ALLOW -> DENIED.
    // ANCESTOR_DENY: deny prefix "a" covers the ALLOW; "abb" is unrelated -> DENIED.
    // UNRELATED_DENY: deny prefix "abb" does not cover the ALLOW -> ALLOWED.
    // NO_DENIES: only ALLOW ACLs exist -> ALLOWED.
    // MIXED_PATTERNS: N literal ALLOWs at distinct names of resourceNameLength each have a matching
    // prefixed DENY, while an ALLOW at P survives -> ALLOWED. With allowPatternType=PREFIXED,
    // the baseline must check all N literal ALLOWs before reaching the surviving prefixed ALLOW.
    @Param({"EXACT_DENY", "ANCESTOR_DENY", "MIXED_PATTERNS", "UNRELATED_DENY", "NO_DENIES"})
    public String scenario;

    private StandardAuthorizer authorizer;
    private Metrics metrics;
    private RequestContext context;

    @Setup
    public void setup() throws Exception {
        if (resourceCount < 1 || sharedPrefixLength < 1) {
            throw new IllegalArgumentException("resourceCount and sharedPrefixLength must be positive");
        }
        int identifierLength = Integer.toString(resourceCount - 1, Character.MAX_RADIX).length();
        int suffixLength = scenario.equals("MIXED_PATTERNS") ? 0 : 3;
        if (resourceNameLength < sharedPrefixLength + identifierLength + suffixLength) {
            throw new IllegalArgumentException("resourceNameLength must fit the shared prefix, "
                + "a unique resource identifier, and the scenario suffix");
        }

        authorizer = new StandardAuthorizer();
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
    }

    private void setupMixedPatterns(KafkaPrincipal principal) {
        String prefix = "x".repeat(sharedPrefixLength);
        addAcl(prefix, allowPatternType, AclPermissionType.ALLOW, principal);
        for (int i = 0; i < resourceCount; i++) {
            String name = resourceNamePrefix(i, 0);
            addAcl(name, PatternType.LITERAL, AclPermissionType.ALLOW, principal);
            addAcl(name, PatternType.PREFIXED, AclPermissionType.DENY, principal);
        }
    }

    private void setupIndependentPatterns(KafkaPrincipal principal) {
        for (int i = 0; i < resourceCount; i++) {
            // prefix should be less than resourceNameLength - 3;
            String prefix = resourceNamePrefix(i, 3);
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

    private String resourceNamePrefix(int index, int suffixLength) {
        int identifierLength = Integer.toString(resourceCount - 1, Character.MAX_RADIX).length();
        String identifier = Integer.toString(index, Character.MAX_RADIX);
        // Put the identifier before the padding so the common prefix does not grow with name length.
        return "x".repeat(sharedPrefixLength)
            + "0".repeat(identifierLength - identifier.length()) + identifier
            + "x".repeat(resourceNameLength - sharedPrefixLength - identifierLength - suffixLength);
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
    public AuthorizationResult optimized() {
        return authorizer.authorizeByResourceType(context, AclOperation.READ, ResourceType.GROUP);
    }

    public static void main(String[] args) throws Exception {
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
