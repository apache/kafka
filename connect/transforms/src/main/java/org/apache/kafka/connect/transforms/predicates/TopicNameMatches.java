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
package org.apache.kafka.connect.transforms.predicates;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.util.RegexValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * A predicate which is true for records with a topic name that matches the configured regular expression.
 * @param <R> The type of connect record.
 */
public class TopicNameMatches<R extends ConnectRecord<R>> implements Predicate<R> {

    private static final String PATTERN_CONFIG = "pattern";

    public static final String OVERVIEW_DOC = "A predicate which is true for records with a topic name that matches the configured regular expression.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PATTERN_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.CompositeValidator.of(new ConfigDef.NonEmptyString(), new RegexValidator()),
            ConfigDef.Importance.MEDIUM,
            "A Java regular expression for matching against the name of a record's topic.");
    private Pattern pattern;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public boolean test(R record) {
        return record.topic() != null && pattern.matcher(record.topic()).matches();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig simpleConfig = new SimpleConfig(config(), configs);
        Pattern result;
        String value = simpleConfig.getString(PATTERN_CONFIG);
        try {
            result = Pattern.compile(value);
        } catch (PatternSyntaxException e) {
            throw new ConfigException(PATTERN_CONFIG, value, "entry must be a Java-compatible regular expression: " + e.getMessage());
        }
        this.pattern = result;
    }

    @Override
    public String toString() {
        return "TopicNameMatches{" +
                "pattern=" + pattern +
                '}';
    }
}
