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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SaslChannelBuilderTest {

    @Test
    public void testCloseBeforeConfigureIsIdempotent() {
        SaslChannelBuilder builder = createChannelBuilder(SecurityProtocol.SASL_PLAINTEXT);
        builder.close();
        assertTrue(builder.loginManagers().isEmpty());
        builder.close();
        assertTrue(builder.loginManagers().isEmpty());
    }

    @Test
    public void testCloseAfterConfigIsIdempotent() {
        SaslChannelBuilder builder = createChannelBuilder(SecurityProtocol.SASL_PLAINTEXT);
        builder.configure(new HashMap<String, Object>());
        assertNotNull(builder.loginManagers().get("PLAIN"));
        builder.close();
        assertTrue(builder.loginManagers().isEmpty());
        builder.close();
        assertTrue(builder.loginManagers().isEmpty());
    }

    @Test
    public void testLoginManagerReleasedIfConfigureThrowsException() {
        SaslChannelBuilder builder = createChannelBuilder(SecurityProtocol.SASL_SSL);
        try {
            // Use invalid config so that an exception is thrown
            builder.configure(Collections.singletonMap(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "1"));
            fail("Exception should have been thrown");
        } catch (KafkaException e) {
            assertTrue(builder.loginManagers().isEmpty());
        }
        builder.close();
        assertTrue(builder.loginManagers().isEmpty());
    }

    private SaslChannelBuilder createChannelBuilder(SecurityProtocol securityProtocol) {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), new HashMap<String, Object>());
        JaasContext jaasContext = new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null);
        Map<String, JaasContext> jaasContexts = Collections.singletonMap("PLAIN", jaasContext);
        return new SaslChannelBuilder(Mode.CLIENT, jaasContexts, securityProtocol, new ListenerName("PLAIN"),
                false, "PLAIN", true, null, null, Time.SYSTEM);
    }

}
