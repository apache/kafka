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

package org.apache.kafka.tools;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.test.TestSslUtils;

import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class OAuthCompatibilityToolTest {

    @Test
    public void testParseArgsParsesClientConfig() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--client-config", "/tmp/test/client.properties"
        });

        assertEquals("/tmp/test/client.properties", namespace.getString("client-config"));
    }

    @Test
    public void testParseArgsParsesBrokerConfig() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--broker-config", "/tmp/test/broker.properties"
        });

        assertEquals("/tmp/test/broker.properties", namespace.getString("broker-config"));
    }

    @Test
    public void testParseArgsParsesSaslJaasConfig() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--sasl.jaas.config", "test"
        });

        assertEquals("test", namespace.getString("sasl.jaas.config"));
    }

    @Test
    public void testParseArgsParsesClientId() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--client-id", "testId"
        });

        assertEquals("testId", namespace.getString("clientId"));
    }

    @Test
    public void testParseArgsParsesClientSecret() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--client-secret", "testSecret"
        });

        assertEquals("testSecret", namespace.getString("clientSecret"));
    }

    @Test
    public void testParseArgsThrowsIfArgumentIsEmpty() {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        assertThrows(
                ArgumentParserException.class,
                () -> argsHandler.parseArgs(new String[]{"", ""}));
    }

    @Test
    public void testGetConfigsReturnsInteger() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--sasl.login.connect.timeout.ms", "100"
        });
        Properties properties = new Properties();
        OAuthCompatibilityTool.ConfigHandler clientConfigHandler = new OAuthCompatibilityTool.ConfigHandler(namespace, properties);

        assertEquals(100, clientConfigHandler.getConfigs().get("sasl.login.connect.timeout.ms"));
    }

    @Test
    public void testGetConfigsReturnsLong() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--sasl.login.retry.backoff.ms", "5"
        });
        Properties properties = new Properties();
        OAuthCompatibilityTool.ConfigHandler clientConfigHandler = new OAuthCompatibilityTool.ConfigHandler(namespace, properties);

        assertEquals(5L, clientConfigHandler.getConfigs().get("sasl.login.retry.backoff.ms"));
    }

    @Test
    public void testGetConfigsReturnsClass() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--ssl.engine.factory.class", "org.apache.kafka.test.TestSslUtils$TestSslEngineFactory"
        });
        Properties properties = new Properties();
        OAuthCompatibilityTool.ConfigHandler clientConfigHandler = new OAuthCompatibilityTool.ConfigHandler(namespace, properties);

        assertEquals(TestSslUtils.TestSslEngineFactory.class, clientConfigHandler.getJaasOptions().get("ssl.engine.factory.class"));
    }

    @Test
    public void testGetConfigsThrowsWhenClassNotFound() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--ssl.engine.factory.class", "SomeClass"
        });
        Properties properties = new Properties();
        OAuthCompatibilityTool.ConfigHandler clientConfigHandler = new OAuthCompatibilityTool.ConfigHandler(namespace, properties);

        assertThrows(KafkaException.class, () -> clientConfigHandler.getJaasOptions().get("ssl.engine.factory.class"));
    }

    @Test
    public void testGetConfigsReturnsStringListFromCli() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--sasl.oauthbearer.expected.audience", "test1",
            "--sasl.oauthbearer.expected.audience", "test2"
        });
        Properties properties = new Properties();
        OAuthCompatibilityTool.ConfigHandler clientConfigHandler = new OAuthCompatibilityTool.ConfigHandler(namespace, properties);

        assertTrue(((List<?>) clientConfigHandler.getConfigs().get("sasl.oauthbearer.expected.audience")).contains("test1"));
        assertTrue(((List<?>) clientConfigHandler.getConfigs().get("sasl.oauthbearer.expected.audience")).contains("test2"));
    }

    @Test
    public void testGetConfigsReturnsStringListFromProperties() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{});
        Properties properties = new Properties();
        properties.put("sasl.oauthbearer.expected.audience", "test1,test2");
        OAuthCompatibilityTool.ConfigHandler clientConfigHandler = new OAuthCompatibilityTool.ConfigHandler(namespace, properties);

        assertTrue(((List<?>) clientConfigHandler.getConfigs().get("sasl.oauthbearer.expected.audience")).contains("test1"));
        assertTrue(((List<?>) clientConfigHandler.getConfigs().get("sasl.oauthbearer.expected.audience")).contains("test2"));
    }

    @Test
    public void testGetConfigsReturnsStringListAndCliHasPriority() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--sasl.oauthbearer.expected.audience", "test1",
            "--sasl.oauthbearer.expected.audience", "test2"
        });
        Properties properties = new Properties();
        properties.put("sasl.oauthbearer.expected.audience", "test3");
        OAuthCompatibilityTool.ConfigHandler clientConfigHandler = new OAuthCompatibilityTool.ConfigHandler(namespace, properties);

        assertTrue(((List<?>) clientConfigHandler.getConfigs().get("sasl.oauthbearer.expected.audience")).contains("test1"));
        assertTrue(((List<?>) clientConfigHandler.getConfigs().get("sasl.oauthbearer.expected.audience")).contains("test2"));
    }

    @Test
    public void testGetConfigsIgnoresSaslJaasConfigIfCredentialsAreProvidedViaCli() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--client-id", "testId1",
            "--client-secret", "testSecret1",
            "--scope", "testScope1",
        });
        Properties properties = new Properties();
        properties.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId=\"testId2\" clientSecret=\"testSecret2\" scope=\"testScope2\";");
        OAuthCompatibilityTool.ConfigHandler clientConfigHandler = new OAuthCompatibilityTool.ConfigHandler(namespace, properties);

        assertEquals("testId1", clientConfigHandler.getJaasOptions().get("clientId"));
        assertEquals("testSecret1", clientConfigHandler.getJaasOptions().get("clientSecret"));
        assertEquals("testScope1", clientConfigHandler.getJaasOptions().get("scope"));
    }

    @Test
    public void testGetJaasOptionsContainsSaslJaasConfigIfCredentialsAreNotProvidedViaCli() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{});
        Properties properties = new Properties();
        properties.put(
            "sasl.jaas.config",
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId=\"testId\" clientSecret=\"testSecret\" scope=\"testScope\";");
        OAuthCompatibilityTool.ConfigHandler clientConfigHandler = new OAuthCompatibilityTool.ConfigHandler(namespace, properties);

        assertEquals("testId", clientConfigHandler.getJaasOptions().get("clientId"));
        assertEquals("testSecret", clientConfigHandler.getJaasOptions().get("clientSecret"));
        assertEquals("testScope", clientConfigHandler.getJaasOptions().get("scope"));
    }

    @Test
    public void testGetJaasOptionsContainsUnknownKeyInSaslJaasConfig() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{});
        Properties properties = new Properties();
        properties.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unknownKey=\"test\" clientId=\"testId\" clientSecret=\"testSecret\" scope=\"testScope\";");
        OAuthCompatibilityTool.ConfigHandler clientConfigHandler = new OAuthCompatibilityTool.ConfigHandler(namespace, properties);

        assertEquals("testId", clientConfigHandler.getJaasOptions().get("clientId"));
        assertEquals("testSecret", clientConfigHandler.getJaasOptions().get("clientSecret"));
        assertEquals("testScope", clientConfigHandler.getJaasOptions().get("scope"));
        assertNull(clientConfigHandler.getJaasOptions().get("unknownKey"));
    }

    @Test
    public void testExitsWhenOnlyClientIdProvided() {
        AtomicInteger exitCode = new AtomicInteger(-1);
        Exit.setExitProcedure((code, message) -> {
            exitCode.set(code);
            throw new RuntimeException("exit called");
        });

        try {
            OAuthCompatibilityTool.main(new String[]{"--unkown-argument", "value"});
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            assertEquals(1, exitCode.get());
        } finally {
            Exit.resetExitProcedure();
        }
    }
}