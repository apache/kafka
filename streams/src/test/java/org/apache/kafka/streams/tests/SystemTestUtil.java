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

package org.apache.kafka.streams.tests;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Class for common convenience methods for working on
 * System tests
 */

public class SystemTestUtil {

    private static final int KEY = 0;
    private static final int VALUE = 1;

    /**
     * Takes a string with keys and values separated by '=' and each key value pair
     * separated by ',' for example max.block.ms=5000,retries=6,request.timeout.ms=6000
     *
     * This class makes it easier to pass configs from the system test in python to the Java test.
     *
     * @param formattedConfigs the formatted config string
     * @return HashMap with keys and values inserted
     */
    public static Map<String, String> parseConfigs(final String formattedConfigs) {
        Objects.requireNonNull(formattedConfigs, "Formatted config String can't be null");

        if (formattedConfigs.indexOf('=') == -1) {
            throw new IllegalStateException(String.format("Provided string [ %s ] does not have expected key-value separator of '='", formattedConfigs));
        }

        final String[] parts = formattedConfigs.split(",");
        final Map<String, String> configs = new HashMap<>();
        for (final String part : parts) {
            final String[] keyValue = part.split("=");
            if (keyValue.length > 2) {
                throw new IllegalStateException(
                    String.format("Provided string [ %s ] does not have expected key-value pair separator of ','", formattedConfigs));
            }
            configs.put(keyValue[KEY], keyValue[VALUE]);
        }
        return configs;
    }
}
