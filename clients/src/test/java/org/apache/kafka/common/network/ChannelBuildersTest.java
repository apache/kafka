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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.security.auth.PrincipalBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.easymock.EasyMock;
import org.junit.Test;

import java.net.InetAddress;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChannelBuildersTest {

    @Test
    public void testCreateOldPrincipalBuilder() throws Exception {
        TransportLayer transportLayer = EasyMock.mock(TransportLayer.class);
        Authenticator authenticator = EasyMock.mock(Authenticator.class);

        Map<String, Object> configs = new HashMap<>();
        configs.put(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, OldPrincipalBuilder.class);
        KafkaPrincipalBuilder builder = ChannelBuilders.createPrincipalBuilder(configs, transportLayer, authenticator, null);

        // test old principal builder is properly configured and delegated to
        assertTrue(OldPrincipalBuilder.configured);

        // test delegation
        KafkaPrincipal principal = builder.build(new PlaintextAuthenticationContext(InetAddress.getLocalHost(), SecurityProtocol.PLAINTEXT.name()));
        assertEquals(OldPrincipalBuilder.PRINCIPAL_NAME, principal.getName());
        assertEquals(KafkaPrincipal.USER_TYPE, principal.getPrincipalType());
    }

    @Test
    public void testCreateConfigurableKafkaPrincipalBuilder() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, ConfigurableKafkaPrincipalBuilder.class);
        KafkaPrincipalBuilder builder = ChannelBuilders.createPrincipalBuilder(configs, null, null, null);
        assertTrue(builder instanceof ConfigurableKafkaPrincipalBuilder);
        assertTrue(((ConfigurableKafkaPrincipalBuilder) builder).configured);
    }

    @SuppressWarnings("deprecation")
    public static class OldPrincipalBuilder implements PrincipalBuilder {
        private static boolean configured = false;
        private static final String PRINCIPAL_NAME = "bob";

        @Override
        public void configure(Map<String, ?> configs) {
            configured = true;
        }

        @Override
        public Principal buildPrincipal(TransportLayer transportLayer, Authenticator authenticator) throws KafkaException {
            return new Principal() {
                @Override
                public String getName() {
                    return PRINCIPAL_NAME;
                }
            };
        }

        @Override
        public void close() throws KafkaException {

        }
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
