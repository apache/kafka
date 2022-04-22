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

package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdminClientConfigTest {

    @Test
    public void testInvalidSaslMechanism() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8121");
        configs.put(SaslConfigs.SASL_MECHANISM, null);
        ConfigException ce = assertThrows(ConfigException.class, () -> new AdminClientConfig(configs));
        assertTrue(ce.getMessage().contains(SaslConfigs.SASL_MECHANISM));

        configs.put(SaslConfigs.SASL_MECHANISM, "");
        ce = assertThrows(ConfigException.class, () -> new AdminClientConfig(configs));
        assertTrue(ce.getMessage().contains(SaslConfigs.SASL_MECHANISM));

        configs.put(SaslConfigs.SASL_MECHANISM, " ");
        ce = assertThrows(ConfigException.class, () -> new AdminClientConfig(configs));
        assertTrue(ce.getMessage().contains(SaslConfigs.SASL_MECHANISM));

    }
}
