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

/** Uses an include and exclude pattern. */
public class DefaultGroupFilter implements GroupFilter {

    public static final String GROUPS_INCLUDE_CONFIG = "groups";
    private static final String GROUPS_INCLUDE_DOC = "List of consumer group names and/or regexes to replicate.";
    public static final String GROUPS_INCLUDE_DEFAULT = ".*";

    public static final String GROUPS_EXCLUDE_CONFIG = "groups.exclude";

    private static final String GROUPS_EXCLUDE_DOC = "List of consumer group names and/or regexes that should not be replicated.";
    public static final String GROUPS_EXCLUDE_DEFAULT = "console-consumer-.*, connect-.*, __.*";

    private Pattern includePattern;
    private Pattern excludePattern;

    @Override
    public void configure(Map<String, ?> props) {
        GroupFilterConfig config = new GroupFilterConfig(props);
        includePattern = config.includePattern();
        excludePattern = config.excludePattern();
    }

    private boolean included(String group) {
        return includePattern != null && includePattern.matcher(group).matches();
    }

    private boolean excluded(String group) {
        return excludePattern != null && excludePattern.matcher(group).matches();
    }

    @Override
    public boolean shouldReplicateGroup(String group) {
        return included(group) && !excluded(group);
    }

    static class GroupFilterConfig extends AbstractConfig {

        static final ConfigDef DEF = new ConfigDef()
            .define(GROUPS_INCLUDE_CONFIG,
                    Type.LIST,
                    GROUPS_INCLUDE_DEFAULT,
                    Importance.HIGH,
                    GROUPS_INCLUDE_DOC)
            .define(GROUPS_EXCLUDE_CONFIG,
                    Type.LIST,
                    GROUPS_EXCLUDE_DEFAULT,
                    Importance.HIGH,
                    GROUPS_EXCLUDE_DOC);

        GroupFilterConfig(Map<String, ?> props) {
            super(DEF, props, false);
        }

        Pattern includePattern() {
            return MirrorUtils.compilePatternList(getList(GROUPS_INCLUDE_CONFIG));
        }

        Pattern excludePattern() {
            return MirrorUtils.compilePatternList(getList(GROUPS_EXCLUDE_CONFIG));
        }
    }
}
