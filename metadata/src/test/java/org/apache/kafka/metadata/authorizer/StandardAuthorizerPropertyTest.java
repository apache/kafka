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

import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.Chars;
import net.jqwik.api.constraints.NumericChars;
import net.jqwik.api.constraints.Size;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.kafka.common.security.auth.KafkaPrincipal.USER_TYPE;
import static org.apache.kafka.metadata.authorizer.StandardAuthorizerTest.PLAINTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class StandardAuthorizerPropertyTest {

    @Target({ ElementType.ANNOTATION_TYPE, ElementType.PARAMETER, ElementType.TYPE_USE })
    @Retention(RetentionPolicy.RUNTIME)
    @AlphaChars @NumericChars @Chars({ '_', '-', '.' })
    public @interface ValidTopicChars { }

    @Property(tries = 5000)
    public void matchingPrefixDenyOverridesAllAllowRules(
        @ForAll Random random,
        @ForAll @ValidTopicChars String topic,
        @ForAll @Size(max = 10) Set<@ValidTopicChars String> randomSuffixes
    ) throws Exception {
        Assume.that(Topic.isValid(topic));
        StandardAuthorizer authorizer = buildAuthorizer();

        // Create one DENY rule which matches and zero or more ALLOW rules which may or
        // may not match. Regardless of the ALLOW rules, the final result should be DENIED.

        String topicPrefix = topic.substring(0, random.nextInt(topic.length()));
        StandardAcl denyRule = buildTopicWriteAcl(topicPrefix, PatternType.PREFIXED, AclPermissionType.DENY);
        authorizer.addAcl(Uuid.randomUuid(), denyRule);
        addRandomPrefixAllowAcls(authorizer, topic, randomSuffixes);

        assertAuthorizationResult(
            authorizer,
            AuthorizationResult.DENIED,
            AclOperation.WRITE,
            new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
        );
    }

    @Property(tries = 5000)
    public void matchingLiteralDenyOverridesAllAllowRules(
        @ForAll @ValidTopicChars String topic,
        @ForAll @Size(max = 10) Set<@ValidTopicChars String> randomSuffixes
    ) throws Exception {
        Assume.that(Topic.isValid(topic));
        StandardAuthorizer authorizer = buildAuthorizer();

        // Create one DENY rule which matches and zero or more ALLOW rules which may or
        // may not match. Regardless of the ALLOW rules, the final result should be DENIED.

        StandardAcl denyRule = buildTopicWriteAcl(topic, PatternType.LITERAL, AclPermissionType.DENY);
        authorizer.addAcl(Uuid.randomUuid(), denyRule);
        addRandomPrefixAllowAcls(authorizer, topic, randomSuffixes);

        assertAuthorizationResult(
            authorizer,
            AuthorizationResult.DENIED,
            AclOperation.WRITE,
            new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
        );
    }

    @Property(tries = 5000)
    public void matchingPrefixAllowWithNoMatchingDenyRules(
        @ForAll Random random,
        @ForAll @ValidTopicChars String topic,
        @ForAll @Size(max = 10) Set<@ValidTopicChars String> randomSuffixes
    ) throws Exception {
        Assume.that(Topic.isValid(topic));
        StandardAuthorizer authorizer = buildAuthorizer();

        // Create one ALLOW rule which matches and zero or more DENY rules which do not
        // match. The final result should be ALLOWED.

        String topicPrefix = topic.substring(0, random.nextInt(topic.length()));
        StandardAcl denyRule = buildTopicWriteAcl(topicPrefix, PatternType.PREFIXED, AclPermissionType.ALLOW);
        authorizer.addAcl(Uuid.randomUuid(), denyRule);

        addRandomNonMatchingPrefixDenyAcls(authorizer, topic, randomSuffixes);

        assertAuthorizationResult(
            authorizer,
            AuthorizationResult.ALLOWED,
            AclOperation.WRITE,
            new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
        );
    }

    @Property(tries = 5000)
    public void matchingLiteralAllowWithNoMatchingDenyRules(
        @ForAll @ValidTopicChars String topic,
        @ForAll @Size(max = 10) Set<@ValidTopicChars String> randomSuffixes
    ) throws Exception {
        Assume.that(Topic.isValid(topic));
        StandardAuthorizer authorizer = buildAuthorizer();

        // Create one ALLOW rule which matches and zero or more DENY rules which do not
        // match. The final result should be ALLOWED.

        StandardAcl denyRule = buildTopicWriteAcl(topic, PatternType.LITERAL, AclPermissionType.ALLOW);
        authorizer.addAcl(Uuid.randomUuid(), denyRule);

        addRandomNonMatchingPrefixDenyAcls(authorizer, topic, randomSuffixes);

        assertAuthorizationResult(
            authorizer,
            AuthorizationResult.ALLOWED,
            AclOperation.WRITE,
            new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
        );
    }

    private StandardAuthorizer buildAuthorizer() {
        StandardAuthorizer authorizer = new StandardAuthorizer();
        authorizer.start(new StandardAuthorizerTest.AuthorizerTestServerInfo(Collections.singletonList(PLAINTEXT)));
        authorizer.completeInitialLoad();
        return authorizer;
    }

    private void assertAuthorizationResult(
        StandardAuthorizer authorizer,
        AuthorizationResult expectedResult,
        AclOperation operation,
        ResourcePattern pattern
    ) throws Exception {
        Action action = new Action(operation, pattern, 1, false, false);
        List<AuthorizationResult> results = authorizer.authorize(
            newRequestContext(),
            Collections.singletonList(action)
        );

        assertEquals(1, results.size());
        AuthorizationResult actualResult = results.get(0);

        try {
            assertEquals(expectedResult, actualResult);
        } catch (Throwable e) {
            printCounterExample(authorizer, operation, pattern, actualResult);
            throw e;
        }
    }

    private void printCounterExample(
        StandardAuthorizer authorizer,
        AclOperation operation,
        ResourcePattern resourcePattern,
        AuthorizationResult result
    ) {
        System.out.println("Assertion FAILED: Operation " + operation + " on " +
            resourcePattern + " is " + result + ". Current ACLS:");

        Iterable<AclBinding> allAcls = authorizer.acls(new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
            new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY)
        ));

        allAcls.forEach(System.out::println);
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

    private boolean isPrefix(
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

    private void addRandomNonMatchingPrefixDenyAcls(
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

    private void addRandomPrefixAllowAcls(
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
    
    private void addRandomPrefixRules(
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
