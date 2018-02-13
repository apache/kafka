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
package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertEquals;

public class LoginManagerTest {

    private Password dynamicPlainContext;
    private Password dynamicDigestContext;
    private TestJaasConfig staticContext;

    @Before
    public void setUp() {
        dynamicPlainContext = new Password(PlainLoginModule.class.getName() +
                " required user=\"plainuser\" password=\"plain-secret\";");
        dynamicDigestContext = new Password(TestDigestLoginModule.class.getName() +
                " required user=\"digestuser\" password=\"digest-secret\";");
        staticContext = TestJaasConfig.createConfiguration("SCRAM-SHA-256",
                Collections.singletonList("SCRAM-SHA-256"));
    }

    @Test
    public void testClientLoginManager() throws Exception {
        Map<String, ?> configs = Collections.singletonMap("sasl.jaas.config", dynamicPlainContext);
        JaasContext dynamicContext = JaasContext.loadClientContext(configs);
        JaasContext staticContext = JaasContext.loadClientContext(Collections.<String, Object>emptyMap());

        LoginManager dynamicLogin = LoginManager.acquireLoginManager(dynamicContext, "PLAIN",
                false, configs);
        assertEquals(dynamicPlainContext, dynamicLogin.cacheKey());
        LoginManager staticLogin = LoginManager.acquireLoginManager(staticContext, "SCRAM-SHA-256",
                false, configs);
        assertNotSame(dynamicLogin, staticLogin);
        assertEquals("KafkaClient", staticLogin.cacheKey());

        assertSame(dynamicLogin, LoginManager.acquireLoginManager(dynamicContext, "PLAIN",
                false, configs));
        assertSame(staticLogin, LoginManager.acquireLoginManager(staticContext, "SCRAM-SHA-256",
                false, configs));
    }

    @Test
    public void testServerLoginManager() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put("plain.sasl.jaas.config", dynamicPlainContext);
        configs.put("digest-md5.sasl.jaas.config", dynamicDigestContext);
        ListenerName listenerName = new ListenerName("listener1");
        JaasContext plainJaasContext = JaasContext.loadServerContext(listenerName, "PLAIN", configs);
        JaasContext digestJaasContext = JaasContext.loadServerContext(listenerName, "DIGEST-MD5", configs);
        JaasContext scramJaasContext = JaasContext.loadServerContext(listenerName, "SCRAM-SHA-256", configs);

        LoginManager dynamicPlainLogin = LoginManager.acquireLoginManager(plainJaasContext, "PLAIN",
                false, configs);
        assertEquals(dynamicPlainContext, dynamicPlainLogin.cacheKey());
        LoginManager dynamicDigestLogin = LoginManager.acquireLoginManager(digestJaasContext, "DIGEST-MD5",
                false, configs);
        assertNotSame(dynamicPlainLogin, dynamicDigestLogin);
        assertEquals(dynamicDigestContext, dynamicDigestLogin.cacheKey());
        LoginManager staticScramLogin = LoginManager.acquireLoginManager(scramJaasContext, "SCRAM-SHA-256",
                false, configs);
        assertNotSame(dynamicPlainLogin, staticScramLogin);
        assertEquals("KafkaServer", staticScramLogin.cacheKey());

        assertSame(dynamicPlainLogin, LoginManager.acquireLoginManager(plainJaasContext, "PLAIN",
                false, configs));
        assertSame(dynamicDigestLogin, LoginManager.acquireLoginManager(digestJaasContext, "DIGEST-MD5",
                false, configs));
        assertSame(staticScramLogin, LoginManager.acquireLoginManager(scramJaasContext, "SCRAM-SHA-256",
                false, configs));
    }
}