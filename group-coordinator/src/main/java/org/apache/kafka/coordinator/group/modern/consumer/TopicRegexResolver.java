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
package org.apache.kafka.coordinator.group.modern.consumer;

import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.Utils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;

public class TopicRegexResolver {

    private final Supplier<Optional<Plugin<Authorizer>>> authorizerPluginSupplier;
    private final Time time;

    public TopicRegexResolver(
        Supplier<Optional<Plugin<Authorizer>>> authorizerPluginSupplier,
        Time time
    ) {
        this.authorizerPluginSupplier = authorizerPluginSupplier;
        this.time = time;
    }

    /**
     * Resolves the provided regular expressions.
     *
     * @param context           The request context.
     * @param groupId           The group id.
     * @param log               The logger to use.
     * @param metadataImage     The metadata image to use for the resolution.
     * @param regexes           The list of regular expressions that must be resolved.
     * @return The list of resolved regular expressions.
     *
     * public for benchmarks.
     */
    public Map<String, ResolvedRegularExpression> resolveRegularExpressions(
        AuthorizableRequestContext context,
        String groupId,
        Logger log,
        CoordinatorMetadataImage metadataImage,
        Set<String> regexes
    ) {
        long startTimeMs = time.milliseconds();
        log.debug("[GroupId {}] Refreshing regular expressions: {}", groupId, regexes);

        Map<String, Set<String>> resolvedRegexes = new HashMap<>(regexes.size());
        List<Pattern> compiledRegexes = new ArrayList<>(regexes.size());
        for (String regex : regexes) {
            resolvedRegexes.put(regex, new HashSet<>());
            try {
                compiledRegexes.add(Pattern.compile(regex));
            } catch (PatternSyntaxException ex) {
                // This should not happen because the regular expressions are validated
                // when received from the members. If for some reason, it would
                // happen, we log it and ignore it.
                log.error("[GroupId {}] Couldn't parse regular expression '{}' due to `{}`. Ignoring it.",
                        groupId, regex, ex.getDescription());
            }
        }

        for (String topicName : metadataImage.topicNames()) {
            for (Pattern regex : compiledRegexes) {
                if (regex.matcher(topicName).matches()) {
                    resolvedRegexes.get(regex.pattern()).add(topicName);
                }
            }
        }

        filterTopicDescribeAuthorizedTopics(
            context,
            resolvedRegexes
        );

        long version = metadataImage.version();
        Map<String, ResolvedRegularExpression> result = new HashMap<>(resolvedRegexes.size());
        for (Map.Entry<String, Set<String>> resolvedRegex : resolvedRegexes.entrySet()) {
            result.put(
                resolvedRegex.getKey(),
                new ResolvedRegularExpression(resolvedRegex.getValue(), version, startTimeMs)
            );
        }

        log.info("[GroupId {}] Scanned {} topics to refresh regular expressions {} in {}ms.",
            groupId, metadataImage.topicNames().size(), resolvedRegexes.keySet(),
            time.milliseconds() - startTimeMs);

        return result;
    }

    /**
     * This method filters the topics in the resolved regexes
     * that the member is authorized to describe.
     *
     * @param context           The request context.
     * @param resolvedRegexes   The map of the regex pattern and its set of matched topics.
     */
    private void filterTopicDescribeAuthorizedTopics(
        AuthorizableRequestContext context,
        Map<String, Set<String>> resolvedRegexes
    ) {
        if (authorizerPluginSupplier.get().isEmpty()) return;

        var authorizer = authorizerPluginSupplier.get().get().get();

        Map<String, Integer> topicNameCount = new HashMap<>();
        resolvedRegexes.values().forEach(topicNames ->
            topicNames.forEach(topicName ->
                topicNameCount.compute(topicName, Utils::incValue)
            )
        );

        List<Action> actions = topicNameCount.entrySet().stream().map(entry -> {
            ResourcePattern resource = new ResourcePattern(TOPIC, entry.getKey(), LITERAL);
            return new Action(DESCRIBE, resource, entry.getValue(), true, false);
        }).collect(Collectors.toList());

        List<AuthorizationResult> authorizationResults = authorizer.authorize(context, actions);
        Set<String> deniedTopics = new HashSet<>();
        IntStream.range(0, actions.size()).forEach(i -> {
            if (authorizationResults.get(i) == AuthorizationResult.DENIED) {
                String deniedTopic = actions.get(i).resourcePattern().name();
                deniedTopics.add(deniedTopic);
            }
        });

        resolvedRegexes.forEach((__, topicNames) -> topicNames.removeAll(deniedTopics));
    }
}
