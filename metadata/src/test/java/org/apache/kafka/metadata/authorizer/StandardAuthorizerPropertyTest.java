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
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.kafka.common.security.auth.KafkaPrincipal.USER_TYPE;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerTest.PLAINTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class StandardAuthorizerPropertyTest {

    private static final String VALID_TOPIC_CHARS =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.";

    private interface TopicSuffixesRandom {
        void accept(String topic, Set<String> suffixes, Random random) throws Exception;
    }

    private interface TopicSuffixes {
        void accept(String topic, Set<String> suffixes) throws Exception;
    }

    private static void forTopicSuffixesRandom(TopicSuffixesRandom topicSuffixesRandom) {
        for (int run = 0; run < 5000; run++) {
            long seed = System.nanoTime() + run;
            Random random = new Random(seed);
            String topic;
            do {
                topic = randomTopicString(random, 249);
            } while (!Topic.isValid(topic));
            Set<String> suffixes = randomSuffixes(random);
            try {
                topicSuffixesRandom.accept(topic, suffixes, random);
            } catch (Throwable e) {
                fail("Failed with seed=" + seed + ", topic=" + topic + ", suffixes=" + suffixes, e);
            }
        }
    }

    private static void forTopicSuffixes(TopicSuffixes topicSuffixes) {
        forTopicSuffixesRandom((topic, suffixes, random) -> topicSuffixes.accept(topic, suffixes));
    }

    @Test
    public void matchingPrefixDenyOverridesAllAllowRules() {
        forTopicSuffixesRandom((topic, suffixes, random) -> {
            StandardAuthorizer authorizer = buildAuthorizer();

            // Create one DENY rule which matches and zero or more ALLOW rules which may or
            // may not match. Regardless of the ALLOW rules, the final result should be DENIED.
            String topicPrefix = topic.substring(0, random.nextInt(topic.length()));
            StandardAcl denyRule = buildTopicWriteAcl(topicPrefix, PatternType.PREFIXED, AclPermissionType.DENY);
            authorizer.addAcl(Uuid.randomUuid(), denyRule);
            addRandomPrefixAllowAcls(authorizer, topic, suffixes);

            assertAuthorizationResult(
                authorizer,
                AuthorizationResult.DENIED,
                AclOperation.WRITE,
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL));
        });
    }

    @Test
    public void matchingLiteralDenyOverridesAllAllowRules() {
        forTopicSuffixes((topic, suffixes) -> {
            StandardAuthorizer authorizer = buildAuthorizer();

            // Create one DENY rule which matches and zero or more ALLOW rules which may or
            // may not match. Regardless of the ALLOW rules, the final result should be DENIED.
            StandardAcl denyRule = buildTopicWriteAcl(topic, PatternType.LITERAL, AclPermissionType.DENY);
            authorizer.addAcl(Uuid.randomUuid(), denyRule);
            addRandomPrefixAllowAcls(authorizer, topic, suffixes);

            assertAuthorizationResult(
                authorizer,
                AuthorizationResult.DENIED,
                AclOperation.WRITE,
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL));
        });
    }

    @Test
    public void matchingPrefixAllowWithNoMatchingDenyRules() {
        forTopicSuffixesRandom((topic, suffixes, random) -> {
            StandardAuthorizer authorizer = buildAuthorizer();

            // Create one ALLOW rule which matches and zero or more DENY rules which do not
            // match. The final result should be ALLOWED.
            String topicPrefix = topic.substring(0, random.nextInt(topic.length()));
            StandardAcl allowRule = buildTopicWriteAcl(topicPrefix, PatternType.PREFIXED, AclPermissionType.ALLOW);
            authorizer.addAcl(Uuid.randomUuid(), allowRule);
            addRandomNonMatchingPrefixDenyAcls(authorizer, topic, suffixes);

            assertAuthorizationResult(
                authorizer,
                AuthorizationResult.ALLOWED,
                AclOperation.WRITE,
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL));
        });
    }

    @Test
    public void matchingLiteralAllowWithNoMatchingDenyRules() {
        forTopicSuffixes((topic, suffixes) -> {
            StandardAuthorizer authorizer = buildAuthorizer();

            // Create one ALLOW rule which matches and zero or more DENY rules which do not
            // match. The final result should be ALLOWED.
            StandardAcl allowRule = buildTopicWriteAcl(topic, PatternType.LITERAL, AclPermissionType.ALLOW);
            authorizer.addAcl(Uuid.randomUuid(), allowRule);
            addRandomNonMatchingPrefixDenyAcls(authorizer, topic, suffixes);

            assertAuthorizationResult(
                authorizer,
                AuthorizationResult.ALLOWED,
                AclOperation.WRITE,
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL));
        });
    }

    private static StandardAuthorizer buildAuthorizer() {
        StandardAuthorizer authorizer = new StandardAuthorizer();
        authorizer.start(new AuthorizerTestServerInfo(List.of(PLAINTEXT)));
        authorizer.withPluginMetrics(new PluginMetricsImpl(new Metrics(), Map.of()));
        authorizer.completeInitialLoad();
        return authorizer;
    }

    private static String randomTopicString(Random random, int maxLength) {
        int length = random.nextInt(maxLength) + 1;
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(VALID_TOPIC_CHARS.charAt(random.nextInt(VALID_TOPIC_CHARS.length())));
        }
        return sb.toString();
    }

    private static Set<String> randomSuffixes(Random random) {
        int size = random.nextInt(11);
        Set<String> suffixes = new HashSet<>();
        for (int i = 0; i < size; i++) {
            suffixes.add(randomTopicString(random, 10));
        }
        return suffixes;
    }

    private static void assertAuthorizationResult(
        StandardAuthorizer authorizer,
        AuthorizationResult expectedResult,
        AclOperation operation,
        ResourcePattern pattern
    ) throws Exception {
        Action action = new Action(operation, pattern, 1, false, false);
        List<AuthorizationResult> results = authorizer.authorize(
            newRequestContext(),
            List.of(action)
        );

        assertEquals(1, results.size());
        AuthorizationResult actualResult = results.get(0);

        try {
            assertEquals(expectedResult, actualResult);
        } catch (AssertionError e) {
            System.out.println("Assertion FAILED: Operation " + operation + " on " +
                    pattern + " is " + actualResult + ". Current ACLS:");
            Iterable<AclBinding> allAcls = authorizer.acls(new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
                new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY)
            ));
            allAcls.forEach(System.out::println);
            throw e;
        }
    }

    private static AuthorizableRequestContext newRequestContext() throws Exception {
        return new MockAuthorizableRequestContext.Builder()
            .setPrincipal(new KafkaPrincipal(USER_TYPE, "user"))
            .build();
    }

    private static StandardAcl buildTopicWriteAcl(
        String resourceName,
        PatternType patternType,
        AclPermissionType permissionType
    ) {
        return new StandardAcl(
            ResourceType.TOPIC,
            resourceName,
            patternType,
            "User:*",
            "*",
            AclOperation.WRITE,
            permissionType
        );
    }

    private static boolean isPrefix(
        String value,
        String prefix
    ) {
        if (prefix.length() > value.length()) {
            return false;
        } else {
            String matchingPrefix = value.substring(0, prefix.length());
            return matchingPrefix.equals(prefix);
        }
    }

    private static void addRandomNonMatchingPrefixDenyAcls(
        StandardAuthorizer authorizer,
        String topic,
        Set<String> randomSuffixes
    ) {
        addRandomPrefixRules(
            authorizer,
            topic,
            randomSuffixes,
            AclPermissionType.DENY,
            pattern -> !pattern.isEmpty() && !isPrefix(topic, pattern)
        );
    }

    private static void addRandomPrefixAllowAcls(
        StandardAuthorizer authorizer,
        String topic,
        Set<String> randomSuffixes
    ) {
        addRandomPrefixRules(
            authorizer,
            topic,
            randomSuffixes,
            AclPermissionType.ALLOW,
            pattern -> !pattern.isEmpty()
        );
    }

    private static void addRandomPrefixRules(
        StandardAuthorizer authorizer,
        String topic,
        Set<String> randomSuffixes,
        AclPermissionType permissionType,
        Predicate<String> patternFilter
    ) {
        Set<String> prefixPatterns = new HashSet<>();

        for (int i = 0; i < topic.length(); i++) {
            String prefix = topic.substring(0, i);
            for (String randomSuffix : randomSuffixes) {
                String pattern = prefix + randomSuffix;
                if (patternFilter.test(pattern)) {
                    prefixPatterns.add(pattern);
                }
            }
        }

        for (String randomResourcePattern : prefixPatterns) {
            authorizer.addAcl(Uuid.randomUuid(), buildTopicWriteAcl(
                randomResourcePattern,
                PatternType.PREFIXED,
                permissionType
            ));
        }
    }

}
