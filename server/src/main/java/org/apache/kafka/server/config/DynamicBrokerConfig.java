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

import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DynamicBrokerConfig {
    public final static Set<String> LISTENER_MECHANISM_CONFIGS = Utils.mkSet(
            KafkaConfig.SASL_JAAS_CONFIG_PROP,
            KafkaConfig.SASL_LOGIN_CALLBACK_HANDLER_CLASS_PROP,
            KafkaConfig.SASL_LOGIN_CLASS_PROP,
            KafkaConfig.SASL_SERVER_CALLBACK_HANDLER_CLASS_PROP,
            KafkaConfig.CONNECTIONS_MAX_REAUTH_MS_PROP
    );

    public final static Pattern LISTENER_CONFIG_REGEX = Pattern.compile("listener\\.name\\.[^.]*\\.(.*)");

    public static List<String> brokerConfigSynonyms(String name, boolean matchListenerOverride) {
        if (Arrays.asList(KafkaConfig.LOG_ROLL_TIME_MILLIS_PROP, KafkaConfig.LOG_ROLL_TIME_HOURS_PROP).contains(name)) {
            return Arrays.asList(KafkaConfig.LOG_ROLL_TIME_MILLIS_PROP, KafkaConfig.LOG_ROLL_TIME_HOURS_PROP);
        } else if (Arrays.asList(KafkaConfig.LOG_ROLL_TIME_JITTER_MILLIS_PROP, KafkaConfig.LOG_ROLL_TIME_JITTER_HOURS_PROP).contains(name)) {
            return Arrays.asList(KafkaConfig.LOG_ROLL_TIME_JITTER_MILLIS_PROP, KafkaConfig.LOG_ROLL_TIME_JITTER_HOURS_PROP);
        } else if (name.equals(KafkaConfig.LOG_FLUSH_INTERVAL_MS_PROP)) {
            return Arrays.asList(KafkaConfig.LOG_FLUSH_INTERVAL_MS_PROP, KafkaConfig.LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP);
        } else if (Arrays.asList(
                KafkaConfig.LOG_RETENTION_TIME_MILLIS_PROP,
                KafkaConfig.LOG_RETENTION_TIME_MINUTES_PROP,
                KafkaConfig.LOG_RETENTION_TIME_HOURS_PROP
        ).contains(name)) {
            return Arrays.asList(
                    KafkaConfig.LOG_RETENTION_TIME_MILLIS_PROP,
                    KafkaConfig.LOG_RETENTION_TIME_MINUTES_PROP,
                    KafkaConfig.LOG_RETENTION_TIME_HOURS_PROP
            );
        } else if (matchListenerOverride && name.matches(LISTENER_CONFIG_REGEX.pattern())) {
            Matcher matcher = LISTENER_CONFIG_REGEX.matcher(name);
            if (matcher.matches()) {
                String baseName = matcher.group(1);
                Optional<String> mechanismConfig = LISTENER_MECHANISM_CONFIGS.stream()
                        .filter(config -> baseName.endsWith(config))
                        .findFirst();
                return Arrays.asList(name, mechanismConfig.orElse(baseName));
            } else {
                return Collections.singletonList(name);
            }
        } else {
            return Collections.singletonList(name);
        }
    }
}
