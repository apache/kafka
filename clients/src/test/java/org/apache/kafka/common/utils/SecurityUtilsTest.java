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
package org.apache.kafka.common.utils;

import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.security.auth.SecurityProviderCreator;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.ssl.mock.TestPlainSaslServerProviderCreator;
import org.apache.kafka.common.security.ssl.mock.TestScramSaslServerProviderCreator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.Provider;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SecurityUtilsTest {

    private final SecurityProviderCreator testScramSaslServerProviderCreator = new TestScramSaslServerProviderCreator();
    private final SecurityProviderCreator testPlainSaslServerProviderCreator = new TestPlainSaslServerProviderCreator();

    private final Provider testScramSaslServerProvider = testScramSaslServerProviderCreator.getProvider();
    private final Provider testPlainSaslServerProvider = testPlainSaslServerProviderCreator.getProvider();

    private void clearTestProviders() {
        Security.removeProvider(testScramSaslServerProvider.getName());
        Security.removeProvider(testPlainSaslServerProvider.getName());
    }

    @BeforeEach
    // Remove the providers if already added
    public void setUp() {
        clearTestProviders();
    }

    // Remove the providers after running test cases
    @AfterEach
    public void tearDown() {
        clearTestProviders();
    }


    @Test
    public void testPrincipalNameCanContainSeparator() {
        String name = "name:with:separator:in:it";
        KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal(KafkaPrincipal.USER_TYPE + ":" + name);
        assertEquals(KafkaPrincipal.USER_TYPE, principal.getPrincipalType());
        assertEquals(name, principal.getName());
    }

    @Test
    public void testParseKafkaPrincipalWithNonUserPrincipalType() {
        String name = "foo";
        String principalType = "Group";
        KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal(principalType + ":" + name);
        assertEquals(principalType, principal.getPrincipalType());
        assertEquals(name, principal.getName());
    }

    private int getProviderIndexFromName(String providerName, Provider[] providers) {
        for (int index = 0; index < providers.length; index++) {
            if (providers[index].getName().equals(providerName)) {
                return index;
            }
        }
        return -1;
    }

    // Tests if the custom providers configured are being added to the JVM correctly. These providers are
    // expected to be added at the start of the list of available providers and with the relative ordering maintained
    @Test
    public void testAddCustomSecurityProvider() {
        String customProviderClasses = testScramSaslServerProviderCreator.getClass().getName() + "," +
                testPlainSaslServerProviderCreator.getClass().getName();
        Map<String, String> configs = new HashMap<>();
        configs.put(SecurityConfig.SECURITY_PROVIDERS_CONFIG, customProviderClasses);
        SecurityUtils.addConfiguredSecurityProviders(configs);

        Provider[] providers = Security.getProviders();
        int testScramSaslServerProviderIndex = getProviderIndexFromName(testScramSaslServerProvider.getName(), providers);
        int testPlainSaslServerProviderIndex = getProviderIndexFromName(testPlainSaslServerProvider.getName(), providers);

        assertEquals(0, testScramSaslServerProviderIndex,
            testScramSaslServerProvider.getName() + " testProvider not found at expected index");
        assertEquals(1, testPlainSaslServerProviderIndex,
            testPlainSaslServerProvider.getName() + " testProvider not found at expected index");
    }
}
