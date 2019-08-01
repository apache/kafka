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
package org.apache.kafka.common.config;

import org.apache.kafka.common.security.ssl.mock.TestPlainSaslServerProvider;
import org.apache.kafka.common.security.ssl.mock.TestScramSaslServerProvider;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.Provider;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;

public class SecurityConfigTest {

    private Provider testScramSaslServerProvider = new TestScramSaslServerProvider();
    private Provider testPlainSaslServerProvider = new TestPlainSaslServerProvider();

    private void clearTestProviders() {
        Security.removeProvider(testScramSaslServerProvider.getName());
        Security.removeProvider(testPlainSaslServerProvider.getName());
    }

    @Before
    // Remove the providers if already added
    public void setUp() {
        clearTestProviders();
    }

    // Remove the providers after running test cases
    @After
    public void tearDown() {
        clearTestProviders();
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
        String customProviderClasses = testScramSaslServerProvider.getClass().getName() + "," + testPlainSaslServerProvider.getClass().getName();
        Map<String, String> configs = new HashMap<>();
        configs.put(SecurityConfig.SECURITY_PROVIDER_CLASS_CONFIG, customProviderClasses);
        SecurityConfig.addConfiguredSecurityProviders(configs);

        Provider[] providers = Security.getProviders();
        int providerIndex = getProviderIndexFromName(testScramSaslServerProvider.getName(), providers);
        int provider1Index = getProviderIndexFromName(testPlainSaslServerProvider.getName(), providers);

        // validations
        MatcherAssert.assertThat(testScramSaslServerProvider.getName() + " testProvider not found at expected index", providerIndex == 0);
        MatcherAssert.assertThat(testPlainSaslServerProvider.getName() + " testProvider not found at expected index", provider1Index == 1);
    }
}
