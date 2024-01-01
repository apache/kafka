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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamicBrokerConfigBaseManagerTest {
    @Test
    public void testSynonyms() {
        assertEquals(Arrays.asList("listener.name.secure.ssl.keystore.type", "ssl.keystore.type"),
                DynamicBrokerConfigBaseManager.brokerConfigSynonyms("listener.name.secure.ssl.keystore.type", true));
        assertEquals(Arrays.asList("listener.name.sasl_ssl.plain.sasl.jaas.config", "sasl.jaas.config"),
                DynamicBrokerConfigBaseManager.brokerConfigSynonyms("listener.name.sasl_ssl.plain.sasl.jaas.config", true));
        assertEquals(Arrays.asList("some.config"),
                DynamicBrokerConfigBaseManager.brokerConfigSynonyms("some.config", true));
        assertEquals(Arrays.asList(KafkaConfig.LOG_ROLL_TIME_MILLIS_PROP, KafkaConfig.LOG_ROLL_TIME_HOURS_PROP),
                DynamicBrokerConfigBaseManager.brokerConfigSynonyms(KafkaConfig.LOG_ROLL_TIME_MILLIS_PROP, true));
    }
}
