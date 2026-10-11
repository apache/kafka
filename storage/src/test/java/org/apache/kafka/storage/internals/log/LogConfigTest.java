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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.config.ServerTopicConfigSynonyms;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LogConfigTest {

    @Test
    public void testExtractAllBrokerLogDefaults() {
        Map<String, Object> logProps = LogConfig.extractLogConfigMap(brokerConfig(Map.of()));
        assertEquals(ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.keySet(), logProps.keySet());
        assertEquals(LogConfig.DEFAULT_RETENTION_MS, logProps.get(TopicConfig.RETENTION_MS_CONFIG));
    }

    @ParameterizedTest
    @CsvSource({
        ", , 1, 3600000",
        ", 2, 1, 120000",
        "3, 2, 1, 3",
        ", , -2, -1",
        ", -2, 1, -1",
        "-2, 2, 1, -1",
        ", , 2147483647, 7730941129200000"
    })
    public void testRetentionPrecedenceAndUnlimitedValue(Long millis, Integer minutes, int hours, long expected) {
        Map<String, Object> props = new HashMap<>();
        props.put(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, hours);
        if (minutes != null) props.put(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, minutes);
        if (millis != null) props.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, millis);

        Map<String, Object> logProps = LogConfig.extractLogConfigMap(brokerConfig(props));
        assertEquals(expected, logProps.get(TopicConfig.RETENTION_MS_CONFIG));
    }

    @Test
    public void testRollJitterAndFlushFallbacks() {
        Map<String, Object> props = new HashMap<>();
        props.put(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, 2);
        props.put(ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG, 1);
        props.put(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG, 1234L);

        Map<String, Object> logProps = LogConfig.extractLogConfigMap(brokerConfig(props));
        assertEquals(7200000L, logProps.get(TopicConfig.SEGMENT_MS_CONFIG));
        assertEquals(3600000L, logProps.get(TopicConfig.SEGMENT_JITTER_MS_CONFIG));
        assertEquals(1234L, logProps.get(TopicConfig.FLUSH_MS_CONFIG));

        props.put(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG, 3000L);
        props.put(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG, 2000L);
        props.put(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG, 1000L);
        logProps = LogConfig.extractLogConfigMap(brokerConfig(props));
        assertEquals(3000L, logProps.get(TopicConfig.SEGMENT_MS_CONFIG));
        assertEquals(2000L, logProps.get(TopicConfig.SEGMENT_JITTER_MS_CONFIG));
        assertEquals(1000L, logProps.get(TopicConfig.FLUSH_MS_CONFIG));
    }

    private AbstractConfig brokerConfig(Map<String, Object> props) {
        Map<String, Object> brokerProps = new HashMap<>(props);
        brokerProps.put("process.roles", "broker");
        brokerProps.put("node.id", 0);
        brokerProps.put("controller.listener.names", "CONTROLLER");
        return new AbstractConfig(AbstractKafkaConfig.CONFIG_DEF, brokerProps, false);
    }
}
