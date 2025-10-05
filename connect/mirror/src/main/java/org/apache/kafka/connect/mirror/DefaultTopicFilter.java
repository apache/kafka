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
public class DefaultTopicFilter implements TopicFilter {
    
    public static final String TOPICS_INCLUDE_CONFIG = "topics";
    private static final String TOPICS_INCLUDE_DOC = "List of topics and/or regexes to replicate.";
    public static final String TOPICS_INCLUDE_DEFAULT = ".*";

    public static final String TOPICS_EXCLUDE_CONFIG = "topics.exclude";
    private static final String TOPICS_EXCLUDE_DOC = "List of topics and/or regexes that should not be replicated.";
    public static final String TOPICS_EXCLUDE_DEFAULT = "mm2.*\\.internal, .*\\.replica, __.*";

    private Pattern includePattern;
    private Pattern excludePattern;

    @Override
    public void configure(Map<String, ?> props) {
        TopicFilterConfig config = new TopicFilterConfig(props);
        includePattern = config.includePattern();
        excludePattern = config.excludePattern();
    }

    private boolean included(String topic) {
        return includePattern != null && includePattern.matcher(topic).matches();
    }

    private boolean excluded(String topic) {
        return excludePattern != null && excludePattern.matcher(topic).matches();
    }

    @Override
    public boolean shouldReplicateTopic(String topic) {
        return included(topic) && !excluded(topic);
    }

    static class TopicFilterConfig extends AbstractConfig {

        static final ConfigDef DEF = new ConfigDef()
            .define(TOPICS_INCLUDE_CONFIG,
                    Type.LIST,
                    TOPICS_INCLUDE_DEFAULT,
                    Importance.HIGH,
                    TOPICS_INCLUDE_DOC)
            .define(TOPICS_EXCLUDE_CONFIG,
                    Type.LIST,
                    TOPICS_EXCLUDE_DEFAULT,
                    Importance.HIGH,
                    TOPICS_EXCLUDE_DOC);

        TopicFilterConfig(Map<String, ?> props) {
            super(DEF, props, false);
        }

        Pattern includePattern() {
            return MirrorUtils.compilePatternList(getList(TOPICS_INCLUDE_CONFIG));
        }

        Pattern excludePattern() {
            return MirrorUtils.compilePatternList(getList(TOPICS_EXCLUDE_CONFIG));
        }
    }
}
