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
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;
import java.util.regex.Pattern;

/** Uses a whitelist and blacklist. */
public class DefaultTopicFilter implements TopicFilter {
    
    public static final String TOPICS_WHITELIST_CONFIG = "topics";
    private static final String TOPICS_WHITELIST_DOC = "List of topics and/or regexes to replicate.";
    public static final String TOPICS_WHITELIST_DEFAULT = ".*";

    public static final String TOPICS_BLACKLIST_CONFIG = "topics.blacklist";
    private static final String TOPICS_BLACKLIST_DOC = "List of topics and/or regexes that should not be replicated.";
    public static final String TOPICS_BLACKLIST_DEFAULT = ".*[\\-\\.]internal, .*\\.replica, __.*";

    private Pattern whitelistPattern;
    private Pattern blacklistPattern;

    @Override
    public void configure(Map<String, ?> props) {
        TopicFilterConfig config = new TopicFilterConfig(props);
        whitelistPattern = config.whitelistPattern();
        blacklistPattern = config.blacklistPattern();
    }

    @Override
    public void close() {
    }

    private boolean whitelisted(String topic) {
        return whitelistPattern != null && whitelistPattern.matcher(topic).matches();
    }

    private boolean blacklisted(String topic) {
        return blacklistPattern != null && blacklistPattern.matcher(topic).matches();
    }

    @Override
    public boolean shouldReplicateTopic(String topic) {
        return whitelisted(topic) && !blacklisted(topic);
    }

    static class TopicFilterConfig extends AbstractConfig {

        static final ConfigDef DEF = new ConfigDef()
            .define(TOPICS_WHITELIST_CONFIG,
                Type.LIST,
                TOPICS_WHITELIST_DEFAULT,
                Importance.HIGH,
                TOPICS_WHITELIST_DOC) 
            .define(TOPICS_BLACKLIST_CONFIG,
                Type.LIST,
                TOPICS_BLACKLIST_DEFAULT,
                Importance.HIGH,
                TOPICS_BLACKLIST_DOC);

        TopicFilterConfig(Map<?, ?> props) {
            super(DEF, props, false);
        }

        Pattern whitelistPattern() {
            return MirrorUtils.compilePatternList(getList(TOPICS_WHITELIST_CONFIG));
        }

        Pattern blacklistPattern() {
            return MirrorUtils.compilePatternList(getList(TOPICS_BLACKLIST_CONFIG));
        }
    }
}
