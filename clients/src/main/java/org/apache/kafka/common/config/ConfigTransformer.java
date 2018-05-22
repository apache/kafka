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
package org.apache.kafka.common.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class wraps a set of {@link ConfigProvider} instances and uses them to perform
 * transformations.
 */
public class ConfigTransformer {
    private static final Pattern DEFAULT_PATTERN = Pattern.compile("\\$\\{(.*?):((.*?):)?(.*?)\\}");
    private static final String EMPTY_PATH = "";

    private final Map<String, ConfigProvider> configProviders;

    /**
     * Creates a ConfigTransformer with the default pattern, of the form <code>${provider:[path:]key}</code>.
     *
     * @param configProviders the set of {@link ConfigProvider} instances.
     */
    public ConfigTransformer(Map<String, ConfigProvider> configProviders) {
        this.configProviders = configProviders;
    }

    /**
     * Transforms the given configuration data by using the {@link ConfigProvider} instances to
     * look up values to replace the variables in the pattern.
     *
     * @param configs the configuration values to be transformed
     * @return an instance of {@link ConfigTransformerResult}
     */
    public ConfigTransformerResult transform(Map<String, String> configs) {
        Map<String, Map<String, Set<String>>> keysByProvider = new HashMap<>();
        Map<String, Map<String, Map<String, String>>> lookupsByProvider = new HashMap<>();

        // Collect the variables from the given configs that need transformation
        for (Map.Entry<String, String> config : configs.entrySet()) {
            List<ConfigVariable> vars = getVars(config.getKey(), config.getValue(), DEFAULT_PATTERN);
            for (ConfigVariable var : vars) {
                Map<String, Set<String>> keysByPath = keysByProvider.computeIfAbsent(var.providerName, k -> new HashMap<>());
                Set<String> keys = keysByPath.computeIfAbsent(var.path, k -> new HashSet<>());
                keys.add(var.variable);
            }
        }

        // Retrieve requested variables from the ConfigProviders
        Map<String, Long> ttls = new HashMap<>();
        for (Map.Entry<String, Map<String, Set<String>>> entry : keysByProvider.entrySet()) {
            String providerName = entry.getKey();
            ConfigProvider provider = configProviders.get(providerName);
            Map<String, Set<String>> keysByPath = entry.getValue();
            if (provider != null && keysByPath != null) {
                for (Map.Entry<String, Set<String>> pathWithKeys : keysByPath.entrySet()) {
                    String path = pathWithKeys.getKey();
                    Set<String> keys = new HashSet<>(pathWithKeys.getValue());
                    ConfigData configData = provider.get(path, keys);
                    Map<String, String> data = configData.data();
                    long ttl = configData.ttl();
                    if (ttl >= 0 && ttl < Long.MAX_VALUE) {
                        ttls.put(path, ttl);
                    }
                    Map<String, Map<String, String>> keyValuesByPath =
                            lookupsByProvider.computeIfAbsent(providerName, k -> new HashMap<>());
                    keyValuesByPath.put(path, data);
                }
            }
        }

        // Perform the transformations by performing variable replacements
        Map<String, String> data = new HashMap<>(configs);
        for (Map.Entry<String, String> config : configs.entrySet()) {
            data.put(config.getKey(), replace(lookupsByProvider, config.getValue(), DEFAULT_PATTERN));
        }
        return new ConfigTransformerResult(data, ttls);
    }

    private static List<ConfigVariable> getVars(String key, String value, Pattern pattern) {
        List<ConfigVariable> configVars = new ArrayList<>();
        Matcher matcher = pattern.matcher(value);
        while (matcher.find()) {
            configVars.add(new ConfigVariable(matcher));
        }
        return configVars;
    }

    private static String replace(Map<String, Map<String, Map<String, String>>> lookupsByProvider,
                                  String value,
                                  Pattern pattern) {
        Matcher matcher = pattern.matcher(value);
        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (matcher.find()) {
            ConfigVariable configVar = new ConfigVariable(matcher);
            Map<String, Map<String, String>> lookupsByPath = lookupsByProvider.get(configVar.providerName);
            if (lookupsByPath != null) {
                Map<String, String> keyValues = lookupsByPath.get(configVar.path);
                String replacement = keyValues.get(configVar.variable);
                builder.append(value, i, matcher.start());
                if (replacement == null) {
                    // No replacements will be performed; just return the original value
                    builder.append(matcher.group(0));
                } else {
                    builder.append(replacement);
                }
                i = matcher.end();
            }
        }
        builder.append(value, i, value.length());
        return builder.toString();
    }

    private static class ConfigVariable {
        final String providerName;
        final String path;
        final String variable;

        ConfigVariable(Matcher matcher) {
            this.providerName = matcher.group(1);
            this.path = matcher.group(3) != null ? matcher.group(3) : EMPTY_PATH;
            this.variable = matcher.group(4);
        }

        public String toString() {
            return "(" + providerName + ":" + (path != null ? path + ":" : "") + variable + ")";
        }
    }
}
