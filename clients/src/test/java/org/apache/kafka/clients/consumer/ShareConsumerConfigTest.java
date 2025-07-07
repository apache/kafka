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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ShareConsumerConfigTest {

    @Test
    public void testUnsupportedShareConsumerConfigs() {
        verifyUnsupportedShareConsumerConfig(Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        verifyUnsupportedShareConsumerConfig(Map.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"));
        verifyUnsupportedShareConsumerConfig(Map.of(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"));
        verifyUnsupportedShareConsumerConfig(Map.of(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"));
        verifyUnsupportedShareConsumerConfig(Map.of(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.StickyAssignor"));
        verifyUnsupportedShareConsumerConfig(Map.of(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.apache.kafka.clients.consumer.ConsumerInterceptor"));
        verifyUnsupportedShareConsumerConfig(Map.of(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000"));
        verifyUnsupportedShareConsumerConfig(Map.of(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000"));
        verifyUnsupportedShareConsumerConfig(Map.of(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic"));
        verifyUnsupportedShareConsumerConfig(Map.of(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "null"));
    }

    private void verifyUnsupportedShareConsumerConfig(Map<String, Object> extraConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.putAll(extraConfig);
        assertThrows(ConfigException.class, () -> new ShareConsumerConfig(props));
    }
}