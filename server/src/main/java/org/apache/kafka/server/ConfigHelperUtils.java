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
package org.apache.kafka.server;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class ConfigHelperUtils {

    /**
     * Creates a DescribeConfigsResult from a Map of configs.
     */
    public static <V> DescribeConfigsResponseData.DescribeConfigsResult createResponseConfig(
            DescribeConfigsRequestData.DescribeConfigsResource resource,
            Map<String, V> config,
            BiFunction<String, Object, DescribeConfigsResponseData.DescribeConfigsResourceResult> createConfigEntry) {

        return toDescribeConfigsResult(
                config.entrySet().stream()
                        .map(entry -> Map.entry(entry.getKey(), entry.getValue())),
                resource,
                createConfigEntry
        );
    }

    /**
     * Creates a DescribeConfigsResult from an AbstractConfig.
     * This method merges the config's originals (excluding nulls and keys present in nonInternalValues, which take priority).
     */
    public static DescribeConfigsResponseData.DescribeConfigsResult createResponseConfig(
            DescribeConfigsRequestData.DescribeConfigsResource resource,
            AbstractConfig config,
            BiFunction<String, Object, DescribeConfigsResponseData.DescribeConfigsResourceResult> createConfigEntry) {

        // Cast from Map<String, ?> to Map<String, Object> to eliminate wildcard types. Cached to avoid multiple calls.
        @SuppressWarnings("unchecked")
        Map<String, Object> nonInternalValues = (Map<String, Object>) config.nonInternalValues();
        Stream<Entry<String, Object>> allEntries = Stream.concat(
                config.originals().entrySet().stream()
                        .filter(entry -> entry.getValue() != null && !nonInternalValues.containsKey(entry.getKey()))
                        .map(entry -> Map.entry(entry.getKey(), entry.getValue())),
                nonInternalValues.entrySet().stream()
        );
        return toDescribeConfigsResult(allEntries, resource, createConfigEntry);
    }

    /**
     * Internal helper that builds a DescribeConfigsResult from a stream of config entries.
     */
    private static DescribeConfigsResponseData.DescribeConfigsResult toDescribeConfigsResult(
            Stream<Entry<String, Object>> configStream,
            DescribeConfigsRequestData.DescribeConfigsResource resource,
            BiFunction<String, Object, DescribeConfigsResponseData.DescribeConfigsResourceResult> createConfigEntry) {

        var configKeys = resource.configurationKeys();
        List<DescribeConfigsResponseData.DescribeConfigsResourceResult> configEntries =
                configStream
                        .filter(entry -> configKeys == null ||
                                configKeys.isEmpty() ||
                                configKeys.contains(entry.getKey()))
                        .map(entry -> createConfigEntry.apply(entry.getKey(), entry.getValue()))
                        .toList();

        return new DescribeConfigsResponseData.DescribeConfigsResult()
                .setErrorCode(Errors.NONE.code())
                .setConfigs(configEntries);
    }
}