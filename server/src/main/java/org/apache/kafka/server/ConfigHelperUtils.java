/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
     * Creates a DescribeConfigsResult from a Map of configs and a function to create config entries.
     */
    public static DescribeConfigsResponseData.DescribeConfigsResult createResponseConfig(
            DescribeConfigsRequestData.DescribeConfigsResource resource,
            Map<String, ?> configs,
            BiFunction<String, Object, DescribeConfigsResponseData.DescribeConfigsResourceResult> createConfigEntry) {

        List<DescribeConfigsResponseData.DescribeConfigsResourceResult> configEntries =
                buildConfigEntries(configs.entrySet().stream().map(e -> (Entry<String, ?>) e), resource, (k, v) -> createConfigEntry.apply(k, v));

        return new DescribeConfigsResponseData.DescribeConfigsResult()
                .setErrorCode(Errors.NONE.code())
                .setConfigs(configEntries);
    }

    /**
     * Overloaded method that takes an AbstractConfig and extracts all configs from it.
     * This method combines originals (filtered for non-null values) and nonInternalValues using streams.
     * nonInternalValues take priority over originals - if a key exists in both, nonInternalValues wins.
     */
    public static DescribeConfigsResponseData.DescribeConfigsResult createResponseConfig(
            DescribeConfigsRequestData.DescribeConfigsResource resource,
            AbstractConfig config,
            BiFunction<String, Object, DescribeConfigsResponseData.DescribeConfigsResourceResult> createConfigEntry) {

        Map<String, ?> nonInternalValues = config.nonInternalValues(); // cache to avoid multiple calls
        List<DescribeConfigsResponseData.DescribeConfigsResourceResult> configEntries =
                buildConfigEntries(
                        Stream.concat(
                                config.originals().entrySet().stream()
                                        .filter(entry -> entry.getValue() != null)
                                        .filter(entry -> !nonInternalValues.containsKey(entry.getKey())) // skip keys in nonInternalValues
                                        .map(e -> (Entry<String, ?>) e),
                                nonInternalValues.entrySet().stream()
                                        .map(e -> (Entry<String, ?>) e)
                        ),
                        resource,
                        createConfigEntry
                );

        return new DescribeConfigsResponseData.DescribeConfigsResult()
                .setErrorCode(Errors.NONE.code())
                .setConfigs(configEntries);
    }

    /**
     * Helper method that builds config entries from a stream of config entries.
     */
    private static List<DescribeConfigsResponseData.DescribeConfigsResourceResult> buildConfigEntries(
            Stream<Entry<String, ?>> configStream,
            DescribeConfigsRequestData.DescribeConfigsResource resource,
            BiFunction<String, Object, DescribeConfigsResponseData.DescribeConfigsResourceResult> createConfigEntry) {

        return configStream
                .filter(entry -> resource.configurationKeys() == null ||
                        resource.configurationKeys().isEmpty() ||
                        resource.configurationKeys().contains(entry.getKey()))
                .map(entry -> createConfigEntry.apply(entry.getKey(), entry.getValue()))
                .toList();
    }
}