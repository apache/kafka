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
package org.apache.kafka.server.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;


/**
 * Represents a synonym for a configuration plus a conversion function. The conversion
 * function is necessary for cases where the synonym is denominated in different units
 * (hours versus milliseconds, etc.)
 */
public class ConfigSynonym {
    private static final Logger log = LoggerFactory.getLogger(ConfigSynonym.class);

    public static final Function<String, String> HOURS_TO_MILLISECONDS = input -> {
        int hours = valueToInt(input, 0, "hoursToMilliseconds");
        return String.valueOf(TimeUnit.MILLISECONDS.convert(hours, TimeUnit.HOURS));
    };

    public static final Function<String, String> MINUTES_TO_MILLISECONDS = input -> {
        int hours = valueToInt(input, 0, "minutesToMilliseconds");
        return String.valueOf(TimeUnit.MILLISECONDS.convert(hours, TimeUnit.MINUTES));
    };

    private static int valueToInt(String input, int defaultValue, String what) {
        if (input == null) return defaultValue;
        String trimmedInput = input.trim();
        if (trimmedInput.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(trimmedInput);
        } catch (Exception e) {
            log.error("{} failed: unable to parse '{}' as an integer.", what, trimmedInput, e);
            return defaultValue;
        }
    }

    private final String name;
    private final Function<String, String> converter;

    public ConfigSynonym(String name, Function<String, String> converter) {
        this.name = name;
        this.converter = converter;
    }

    public ConfigSynonym(String name) {
        this(name, Function.identity());
    }

    public String name() {
        return name;
    }

    public Function<String, String> converter() {
        return converter;
    }
}
