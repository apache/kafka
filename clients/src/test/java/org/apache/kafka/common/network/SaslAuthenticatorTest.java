/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.common.network;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.net.InetSocketAddress;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;

import static org.junit.Assert.fail;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.SaslMechanism;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.utils.MockTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the Sasl authenticator. These use a test harness that runs a simple socket server that echos back responses.
 */

public class SaslAuthenticatorTest {

    private static final int BUFFER_SIZE = 4 * 1024;

    private NioEchoServer server;
    private Selector selector;
    private ChannelBuilder channelBuilder;
    private NetworkTestUtils.CertStores serverCertStores;
    private NetworkTestUtils.CertStores clientCertStores;
    private Map<String, Object> saslClientConfigs;
    private Map<String, Object> saslServerConfigs;
    
    @Before
    public void setup() throws Exception {
        serverCertStores = new NetworkTestUtils.CertStores(true);
        clientCertStores = new NetworkTestUtils.CertStores(false);
        saslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
        saslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
    }

    @After
    public void teardown() throws Exception {
        if (server != null)
            this.server.close();
        if (selector != null)
            this.selector.close();
    }
    
    @Test
    public void testValidSaslPlain_Ssl() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig.createPlainConfig("myuser", "mypassword", "user_myuser", "mypassword");
        createConfig(saslClientConfigs, false, SaslMechanism.PLAIN);
        createConfig(saslServerConfigs, true, SaslMechanism.PLAIN);
        
        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    @Test
    public void testValidSaslPlain_Plaintext() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_PLAINTEXT;
        TestJaasConfig.createPlainConfig("myuser", "mypassword", "user_myuser", "mypassword");
        createConfig(saslClientConfigs, false, SaslMechanism.PLAIN);
        createConfig(saslServerConfigs, true, SaslMechanism.PLAIN);
        
        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.checkClientConnection(selector, node, 100, 10);
    }
    
    @Test
    public void testInvalidSaslPlain() throws Exception {
        String node = "0";
        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        TestJaasConfig.createPlainConfig("myuser", "invalidpassword", "user_myuser", "mypassword");
        createConfig(saslClientConfigs, false, SaslMechanism.PLAIN);
        createConfig(saslServerConfigs, true, SaslMechanism.PLAIN);
        
        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port);
        selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);

        NetworkTestUtils.waitForChannelClose(selector, node);
    }

    @Test
    public void testMissingUsernameSaslPlain() throws Exception {
        String node = "0";
        TestJaasConfig.createPlainConfig(null, "mypassword", "user_myuser", "mypassword");
        createConfig(saslClientConfigs, false, SaslMechanism.PLAIN);
        createConfig(saslServerConfigs, true, SaslMechanism.PLAIN);

        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port);
        try {
            selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
            fail("SASL/PLAIN channel created without username");
        } catch (KafkaException e) {
            // Expected exception
        }
    }
    
    @Test
    public void testMissingPasswordSaslPlain() throws Exception {
        String node = "0";
        TestJaasConfig.createPlainConfig("myuser", null, "user_myuser", "mypassword");
        createConfig(saslClientConfigs, false, SaslMechanism.PLAIN);
        createConfig(saslServerConfigs, true, SaslMechanism.PLAIN);

        SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
        server = NetworkTestUtils.createEchoServer(securityProtocol, saslServerConfigs);
        createSelector(securityProtocol, saslClientConfigs);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", server.port);
        try {
            selector.connect(node, addr, BUFFER_SIZE, BUFFER_SIZE);
            fail("SASL/PLAIN channel created without password");
        } catch (KafkaException e) {
            // Expected exception
        }
    }

    private void createSelector(SecurityProtocol securityProtocol, Map<String, Object> saslClientConfigs) {
        
        this.channelBuilder = new SaslChannelBuilder(Mode.CLIENT, LoginType.CLIENT, securityProtocol);
        this.channelBuilder.configure(saslClientConfigs);
        this.selector = new Selector(5000, new Metrics(), new MockTime(), "MetricGroup", new LinkedHashMap<String, String>(), channelBuilder);
    }

    
    private void createConfig(Map<String, Object> config, boolean isServer, SaslMechanism mechanism) throws Exception {
        config.put(SslConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, Class.forName(SslConfigs.DEFAULT_PRINCIPAL_BUILDER_CLASS));
        config.put(SaslConfigs.SASL_MECHANISM, mechanism.mechanismName());        
    }
    
    private static class TestJaasConfig extends Configuration {
        
        private HashMap<String, AppConfigurationEntry[]> entryMap = new HashMap<String, AppConfigurationEntry[]>();
        
        public static TestJaasConfig createPlainConfig(String clientUsername, String clientPassword, String...serverUsernamesAndPasswords) {
            TestJaasConfig config = new TestJaasConfig();
            config.createEntry("KafkaClient", PlainLoginModule.class.getName(), "username", clientUsername, "password", clientPassword);
            config.createEntry("KafkaServer", PlainLoginModule.class.getName(), serverUsernamesAndPasswords);
            Configuration.setConfiguration(config);
            return config;
        }
        
        public void createEntry(String name, String loginModule, String...optionNamesAndValues) {
            HashMap<String, Object> options = new HashMap<String, Object>();
            AppConfigurationEntry entry = new AppConfigurationEntry(loginModule, LoginModuleControlFlag.REQUIRED, options);
            for (int i = 0; i < optionNamesAndValues.length; i += 2) {
                String value =  optionNamesAndValues[i + 1];
                if (value != null)
                    options.put(optionNamesAndValues[i], value);
            }
            entryMap.put(name, new AppConfigurationEntry[]{entry});
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            return entryMap.get(name);
        }
    }
}
