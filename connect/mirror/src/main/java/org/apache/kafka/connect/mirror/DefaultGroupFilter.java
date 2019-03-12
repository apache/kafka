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

/** Uses a whitelist and blacklist. */
public class DefaultGroupFilter implements GroupFilter, Configurable {

    private Pattern whitelistPattern;
    private Pattern blacklistPattern;

    @Override
    public void configure(Map<String, ?> props) {
        GroupFilterConfig config = new GroupFilterConfig(props);
        whitelistPattern = config.whitelistPattern();
        blacklistPattern = config.blacklistPattern();
    }

    private boolean whitelisted(String group) {
        return whitelistPattern != null && whitelistPattern.matcher(group).matches();
    }

    private boolean blacklisted(String group) {
        return blacklistPattern != null && blacklistPattern.matcher(group).matches();
    }

    @Override
    public boolean shouldReplicateGroup(String group) {
        return whitelisted(group) && !blacklisted(group);
    }

    private static class GroupFilterConfig extends AbstractConfig {

        public static final String GROUPS_WHITELIST = "groups";
        private static final String GROUPS_WHITELIST_DOC = "List of consumer group names and/or regexes to replicate.";
        public static final String GROUPS_WHITELIST_DEFAULT = ".*";

        public static final String GROUPS_BLACKLIST = "groups.blacklist";
        private static final String GROUPS_BLACKLIST_DOC = "List of consumer group names and/or regexes that should not be replicated.";
        public static final String GROUPS_BLACKLIST_DEFAULT = "console-consumer-.*, connect-.*";

        static final ConfigDef DEF = new ConfigDef()
            .define(GROUPS_WHITELIST,
                Type.LIST,
                GROUPS_WHITELIST_DEFAULT,
                Importance.HIGH,
                GROUPS_WHITELIST_DOC) 
            .define(GROUPS_BLACKLIST,
                Type.LIST,
                GROUPS_BLACKLIST_DEFAULT,
                Importance.HIGH,
                GROUPS_BLACKLIST_DOC);

        GroupFilterConfig(Map<?, ?> props) {
            super(DEF, props);
        }

        Pattern whitelistPattern() {
            return MirrorUtils.compilePatternList(getList(GROUPS_WHITELIST));
        }

        Pattern blacklistPattern() {
            return MirrorUtils.compilePatternList(getList(GROUPS_BLACKLIST));
        }
    }
}
