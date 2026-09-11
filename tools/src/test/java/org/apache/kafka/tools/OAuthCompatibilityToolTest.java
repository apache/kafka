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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.utils.internals.Exit;

import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OAuthCompatibilityToolTest {

    private final AtomicInteger exitCode = new AtomicInteger(-1);

    @BeforeEach
    void setUp() {
        Exit.setExitProcedure((code, message) -> {
            exitCode.set(code);
            throw new RuntimeException("exit called");
        });
    }

    @AfterEach
    void tearDown() {
        Exit.resetExitProcedure();
    }

    @Test
    void testParseArgsParsesClientConfig() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--client-config", "/tmp/test/client.properties"
        });

        assertEquals("/tmp/test/client.properties", namespace.getString("client-config"));
    }

    @Test
    void testParseArgsParsesBrokerConfig() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--broker-config", "/tmp/test/broker.properties"
        });

        assertEquals("/tmp/test/broker.properties", namespace.getString("broker-config"));
    }

    @Test
    void testParseArgsThrowsIfArgumentIsEmpty() {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        assertThrows(
            ArgumentParserException.class,
            () -> argsHandler.parseArgs(new String[]{"", ""}));
    }

    @Test
    void testGetConfigsReturnsStringListAndCliHasPriority() throws ArgumentParserException {
        OAuthCompatibilityTool.ArgsHandler argsHandler = new OAuthCompatibilityTool.ArgsHandler();
        Namespace namespace = argsHandler.parseArgs(new String[]{
            "--sasl.oauthbearer.expected.audience", "test1,test2"
        });
        Properties properties = new Properties();
        properties.put("sasl.oauthbearer.expected.audience", "test3");
        OAuthCompatibilityTool.ConfigHandler clientConfigHandler = new OAuthCompatibilityTool.ConfigHandler(namespace, properties);

        ConfigDef cd = new ConfigDef();
        SaslConfigs.addClientSaslSupport(cd);

        List<?> audience = (List<?>) clientConfigHandler.getConfigs(cd).get("sasl.oauthbearer.expected.audience");
        assertTrue(audience.contains("test1"));
        assertTrue(audience.contains("test2"));
    }

    @Test
    void testExitsWhenOnlyUnknownArgumentProvided() {
        assertThrows(RuntimeException.class, () -> OAuthCompatibilityTool.main(new String[]{"--unknown-argument", "value"}));
        assertEquals(1, exitCode.get());
    }

    @Test
    void testExitsWhenArgumentValueIsInvalid() {
        assertThrows(RuntimeException.class, () -> OAuthCompatibilityTool.main(new String[]{"--sasl.login.retry.backoff.ms", "not-a-number"}));
        assertEquals(1, exitCode.get());
    }
}