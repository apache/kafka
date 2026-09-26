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
package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigOrJaasTest extends OAuthBearerTest {

    @Test
    public void testClientIdFromConfigOnly() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, "config-client");
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        JaasOptionsUtils jou = new JaasOptionsUtils(Map.of());

        ConfigOrJaas configOrJaas = new ConfigOrJaas(cu, jou);
        assertEquals("config-client", configOrJaas.clientId());
    }

    @Test
    public void testClientIdFromJaasOnly() {
        ConfigurationUtils cu = new ConfigurationUtils(new HashMap<>());
        Map<String, Object> jaasOptions = new HashMap<>();
        jaasOptions.put("clientId", "jaas-client");
        JaasOptionsUtils jou = new JaasOptionsUtils(jaasOptions);

        ConfigOrJaas configOrJaas = new ConfigOrJaas(cu, jou);
        assertEquals("jaas-client", configOrJaas.clientId());
    }

    @Test
    public void testClientIdFromBothPreferConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, "config-client");
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        Map<String, Object> jaasOptions = new HashMap<>();
        jaasOptions.put("clientId", "jaas-client");
        JaasOptionsUtils jou = new JaasOptionsUtils(jaasOptions);

        ConfigOrJaas configOrJaas = new ConfigOrJaas(cu, jou);
        assertEquals("config-client", configOrJaas.clientId());
    }

    @Test
    public void testClientIdRequiredThrowsWhenAbsent() {
        ConfigurationUtils cu = new ConfigurationUtils(new HashMap<>());
        JaasOptionsUtils jou = new JaasOptionsUtils(Map.of());

        ConfigOrJaas configOrJaas = new ConfigOrJaas(cu, jou);
        assertThrows(ConfigException.class, () -> configOrJaas.clientId(true));
    }

    @Test
    public void testClientIdOptionalReturnsNullWhenAbsent() {
        ConfigurationUtils cu = new ConfigurationUtils(new HashMap<>());
        JaasOptionsUtils jou = new JaasOptionsUtils(Map.of());

        ConfigOrJaas configOrJaas = new ConfigOrJaas(cu, jou);
        assertNull(configOrJaas.clientId(false));
    }

    @Test
    public void testClientSecretFromConfigOnly() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, new Password("config-secret"));
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        JaasOptionsUtils jou = new JaasOptionsUtils(Map.of());

        ConfigOrJaas configOrJaas = new ConfigOrJaas(cu, jou);
        assertEquals("config-secret", configOrJaas.clientSecret());
    }

    @Test
    public void testClientSecretFromJaasOnly() {
        ConfigurationUtils cu = new ConfigurationUtils(new HashMap<>());
        Map<String, Object> jaasOptions = new HashMap<>();
        jaasOptions.put("clientSecret", "jaas-secret");
        JaasOptionsUtils jou = new JaasOptionsUtils(jaasOptions);

        ConfigOrJaas configOrJaas = new ConfigOrJaas(cu, jou);
        assertEquals("jaas-secret", configOrJaas.clientSecret());
    }

    @Test
    public void testClientSecretThrowsWhenAbsent() {
        ConfigurationUtils cu = new ConfigurationUtils(new HashMap<>());
        JaasOptionsUtils jou = new JaasOptionsUtils(Map.of());

        ConfigOrJaas configOrJaas = new ConfigOrJaas(cu, jou);
        assertThrows(ConfigException.class, configOrJaas::clientSecret);
    }

    @Test
    public void testScopeFromConfigOnly() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_SCOPE, "openid profile");
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        JaasOptionsUtils jou = new JaasOptionsUtils(Map.of());

        ConfigOrJaas configOrJaas = new ConfigOrJaas(cu, jou);
        assertEquals("openid profile", configOrJaas.scope());
    }

    @Test
    public void testScopeReturnsNullWhenAbsent() {
        ConfigurationUtils cu = new ConfigurationUtils(new HashMap<>());
        JaasOptionsUtils jou = new JaasOptionsUtils(Map.of());

        ConfigOrJaas configOrJaas = new ConfigOrJaas(cu, jou);
        assertNull(configOrJaas.scope());
    }
}
