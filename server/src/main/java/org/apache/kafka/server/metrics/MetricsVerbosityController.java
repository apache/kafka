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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.server.util.Json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Lightweight parser and matcher for the KIP-977 metrics.verbosity configuration.
 * Reads the current config string from the provided config object and caches the latest parsed rules
 * keyed by the raw config string. If the string changes (via dynamic update), rules are re-parsed.
 */
public final class MetricsVerbosityController {
    private static volatile String cachedConfigRaw = null;
    private static volatile List<CompiledRule> cachedRules = Collections.emptyList();

    private MetricsVerbosityController() {}

    public static boolean shouldEmitPartitionMetric(AbstractConfig config, String metricName, String topic) {
        String raw = Optional.ofNullable(config.getString(MetricConfigs.METRICS_VERBOSITY_CONFIG)).orElse("[]");
        if (!Objects.equals(raw, cachedConfigRaw)) {
            cachedRules = parseRules(raw);
            cachedConfigRaw = raw;
        }
        // No rules or all low => do not emit
        if (cachedRules.isEmpty()) return false;
        for (CompiledRule rule : cachedRules) {
            if (rule.levelHigh && rule.namePattern.matcher(metricName).matches() && rule.topicMatches(topic)) {
                return true;
            }
        }
        return false;
    }

    private static List<CompiledRule> parseRules(String raw) {
        try {
            Rule[] rules = Json.parseStringAs(raw, Rule[].class);
            if (rules == null || rules.length == 0) return Collections.emptyList();
            List<CompiledRule> compiled = new ArrayList<>();
            for (Rule r : rules) {
                if (r == null) continue;
                boolean levelHigh = "high".equalsIgnoreCase(r.level);
                if (!levelHigh) {
                    // Only high unlocks partition fan-out per KIP-977. Low entries contribute nothing.
                    continue;
                }
                Pattern namePattern = Pattern.compile(r.names == null || r.names.isBlank() ? ".*" : r.names);
                List<Pattern> topicPatterns = new ArrayList<>();
                if (r.filters != null) {
                    for (Filter f : r.filters) {
                        if (f == null) continue;
                        if (f.topicPattern != null && !f.topicPattern.isBlank()) {
                            topicPatterns.add(Pattern.compile(f.topicPattern));
                        }
                        if (f.topics != null && !f.topics.isEmpty()) {
                            // Convert literal topics list to exact-match patterns
                            topicPatterns.addAll(f.topics.stream().map(Pattern::quote).map(Pattern::compile).collect(Collectors.toList()));
                        }
                    }
                }
                compiled.add(new CompiledRule(levelHigh, namePattern, topicPatterns));
            }
            return compiled;
        } catch (Exception e) {
            // On malformed JSON, default to low (no fan-out)
            return Collections.emptyList();
        }
    }

    private static final class CompiledRule {
        final boolean levelHigh;
        final Pattern namePattern;
        final List<Pattern> topicPatterns;

        CompiledRule(boolean levelHigh, Pattern namePattern, List<Pattern> topicPatterns) {
            this.levelHigh = levelHigh;
            this.namePattern = namePattern;
            this.topicPatterns = topicPatterns == null ? Collections.emptyList() : topicPatterns;
        }

        boolean topicMatches(String topic) {
            if (topicPatterns.isEmpty()) return false; // require explicit topics per KIP examples
            for (Pattern p : topicPatterns) {
                if (p.matcher(topic).matches()) return true;
            }
            return false;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class Rule {
        @JsonProperty("level")
        public String level;
        @JsonProperty("names")
        public String names;
        @JsonProperty("filters")
        public List<Filter> filters;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class Filter {
        @JsonProperty("topics")
        public List<String> topics;
        @JsonProperty("topicPattern")
        public String topicPattern;
    }
}


