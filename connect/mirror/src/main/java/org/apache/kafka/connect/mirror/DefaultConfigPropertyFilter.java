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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;
import java.util.regex.Pattern;

/** Uses a blacklist of property names or regexes. */
public class DefaultConfigPropertyFilter implements ConfigPropertyFilter, Configurable {
    
    public static final String CONFIG_PROPERTIES_BLACKLIST_CONFIG = "config.properties.blacklist";
    private static final String CONFIG_PROPERTIES_BLACKLIST_DOC = "List of topic configuration properties and/or regexes "
        + "that should not be replicated.";
    public static final String CONFIG_PROPERTIES_BLACKLIST_DEFAULT = "segment\\.bytes";

    private Pattern blacklistPattern;

    @Override
    public void configure(Map<String, ?> props) {
        ConfigPropertyFilterConfig config = new ConfigPropertyFilterConfig(props);
        blacklistPattern = config.blacklistPattern();
    }

    private boolean blacklisted(String prop) {
        return blacklistPattern != null && blacklistPattern.matcher(prop).matches();
    }

    @Override
    public boolean shouldReplicateConfigProperty(String prop) {
        return !blacklisted(prop);
    }

    static class ConfigPropertyFilterConfig extends AbstractConfig {

        static final ConfigDef DEF = new ConfigDef()
            .define(CONFIG_PROPERTIES_BLACKLIST_CONFIG,
                Type.LIST,
                CONFIG_PROPERTIES_BLACKLIST_DEFAULT,
                Importance.HIGH,
                CONFIG_PROPERTIES_BLACKLIST_DOC);

        ConfigPropertyFilterConfig(Map<?, ?> props) {
            super(DEF, props);
        }

        Pattern blacklistPattern() {
            return MirrorUtils.compilePatternList(getList(CONFIG_PROPERTIES_BLACKLIST_CONFIG));
        }
    }
}
