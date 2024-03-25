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
package org.apache.kafka.common.network;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChannelBuildersTest {

    @Test
    public void testCreateConfigurableKafkaPrincipalBuilder() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, ConfigurableKafkaPrincipalBuilder.class);
        KafkaPrincipalBuilder builder = ChannelBuilders.createPrincipalBuilder(configs, null, null);
        assertInstanceOf(ConfigurableKafkaPrincipalBuilder.class, builder);
        assertTrue(((ConfigurableKafkaPrincipalBuilder) builder).configured);
    }

    @Test
    public void testChannelBuilderConfigs() {
        Properties props = new Properties();
        props.put("listener.name.listener1.gssapi.sasl.kerberos.service.name", "testkafka");
        props.put("listener.name.listener1.sasl.kerberos.service.name", "testkafkaglobal");
        props.put("plain.sasl.server.callback.handler.class", "callback");
        props.put("listener.name.listener1.gssapi.config1.key", "custom.config1");
        props.put("custom.config2.key", "custom.config2");
        TestSecurityConfig securityConfig = new TestSecurityConfig(props);

        // test configs with listener prefix
        Map<String, Object> configs = ChannelBuilders.channelBuilderConfigs(securityConfig, new ListenerName("listener1"));

        assertNull(configs.get("listener.name.listener1.gssapi.sasl.kerberos.service.name"));
        assertFalse(securityConfig.unused().contains("listener.name.listener1.gssapi.sasl.kerberos.service.name"));

        assertEquals(configs.get("gssapi.sasl.kerberos.service.name"), "testkafka");
        assertFalse(securityConfig.unused().contains("gssapi.sasl.kerberos.service.name"));

        assertEquals(configs.get("sasl.kerberos.service.name"), "testkafkaglobal");
        assertFalse(securityConfig.unused().contains("sasl.kerberos.service.name"));

        assertNull(configs.get("listener.name.listener1.sasl.kerberos.service.name"));
        assertFalse(securityConfig.unused().contains("listener.name.listener1.sasl.kerberos.service.name"));

        assertNull(configs.get("plain.sasl.server.callback.handler.class"));
        assertFalse(securityConfig.unused().contains("plain.sasl.server.callback.handler.class"));

        assertEquals(configs.get("listener.name.listener1.gssapi.config1.key"), "custom.config1");
        assertFalse(securityConfig.unused().contains("listener.name.listener1.gssapi.config1.key"));

        assertEquals(configs.get("custom.config2.key"), "custom.config2");
        assertFalse(securityConfig.unused().contains("custom.config2.key"));

        // test configs without listener prefix
        securityConfig = new TestSecurityConfig(props);
        configs = ChannelBuilders.channelBuilderConfigs(securityConfig, null);

        assertEquals(configs.get("listener.name.listener1.gssapi.sasl.kerberos.service.name"), "testkafka");
        assertFalse(securityConfig.unused().contains("listener.name.listener1.gssapi.sasl.kerberos.service.name"));

        assertNull(configs.get("gssapi.sasl.kerberos.service.name"));
        assertFalse(securityConfig.unused().contains("gssapi.sasl.kerberos.service.name"));

        assertEquals(configs.get("listener.name.listener1.sasl.kerberos.service.name"), "testkafkaglobal");
        assertFalse(securityConfig.unused().contains("listener.name.listener1.sasl.kerberos.service.name"));

        assertNull(configs.get("sasl.kerberos.service.name"));
        assertFalse(securityConfig.unused().contains("sasl.kerberos.service.name"));

        assertEquals(configs.get("plain.sasl.server.callback.handler.class"), "callback");
        assertFalse(securityConfig.unused().contains("plain.sasl.server.callback.handler.class"));

        assertEquals(configs.get("listener.name.listener1.gssapi.config1.key"), "custom.config1");
        assertFalse(securityConfig.unused().contains("listener.name.listener1.gssapi.config1.key"));

        assertEquals(configs.get("custom.config2.key"), "custom.config2");
        assertFalse(securityConfig.unused().contains("custom.config2.key"));
    }

    public static class ConfigurableKafkaPrincipalBuilder implements KafkaPrincipalBuilder, Configurable {
        private boolean configured = false;

        @Override
        public void configure(Map<String, ?> configs) {
            configured = true;
        }

        @Override
        public KafkaPrincipal build(AuthenticationContext context) {
            return null;
        }
    }
}
