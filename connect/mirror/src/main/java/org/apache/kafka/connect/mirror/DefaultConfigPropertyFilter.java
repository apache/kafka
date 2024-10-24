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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;
import java.util.regex.Pattern;

/** Filters excluded property names or regexes. */
public class DefaultConfigPropertyFilter implements ConfigPropertyFilter {
    
    public static final String CONFIG_PROPERTIES_EXCLUDE_CONFIG = "config.properties.exclude";
    public static final String USE_DEFAULTS_FROM = "use.defaults.from";
    private static final String USE_DEFAULTS_FROM_DOC = "Which cluster's defaults (source or target) to use "
                                                        + "when syncing topic configurations that have default values.";
    private static final String USE_DEFAULTS_FROM_DEFAULT = "target";

    private static final String CONFIG_PROPERTIES_EXCLUDE_DOC = "List of topic configuration properties and/or regexes "
                                                                + "that should not be replicated.";
    public static final String CONFIG_PROPERTIES_EXCLUDE_DEFAULT = "follower\\.replication\\.throttled\\.replicas, "
                                                                   + "leader\\.replication\\.throttled\\.replicas, "
                                                                   + "message\\.timestamp\\.difference\\.max\\.ms, "
                                                                   + "message\\.timestamp\\.type, "
                                                                   + "unclean\\.leader\\.election\\.enable, "
                                                                   + "min\\.insync\\.replicas,";
    private Pattern excludePattern = MirrorUtils.compilePatternList(CONFIG_PROPERTIES_EXCLUDE_DEFAULT);
    private String useDefaultsFrom = USE_DEFAULTS_FROM_DEFAULT;

    @Override
    public void configure(Map<String, ?> props) {
        ConfigPropertyFilterConfig config = new ConfigPropertyFilterConfig(props);
        excludePattern = config.excludePattern();
        useDefaultsFrom = config.useDefaultsFrom();
    }

    private boolean excluded(String prop) {
        return excludePattern != null && excludePattern.matcher(prop).matches();
    }

    @Override
    public boolean shouldReplicateConfigProperty(String prop) {
        return !excluded(prop);
    }

    @Override
    public boolean shouldReplicateSourceDefault(String prop) {
        return useDefaultsFrom.equals("source");
    }

    static class ConfigPropertyFilterConfig extends AbstractConfig {

        static final ConfigDef DEF = new ConfigDef()
            .define(CONFIG_PROPERTIES_EXCLUDE_CONFIG,
                    Type.LIST,
                    CONFIG_PROPERTIES_EXCLUDE_DEFAULT,
                    Importance.HIGH,
                    CONFIG_PROPERTIES_EXCLUDE_DOC)
            .define(USE_DEFAULTS_FROM,
                    Type.STRING,
                    USE_DEFAULTS_FROM_DEFAULT,
                    Importance.MEDIUM,
                    USE_DEFAULTS_FROM_DOC);


        ConfigPropertyFilterConfig(Map<String, ?> props) {
            super(DEF, props, false);
        }

        Pattern excludePattern() {
            return MirrorUtils.compilePatternList(getList(CONFIG_PROPERTIES_EXCLUDE_CONFIG));
        }

        String useDefaultsFrom() {
            return getString(USE_DEFAULTS_FROM);
        }
    }
}
