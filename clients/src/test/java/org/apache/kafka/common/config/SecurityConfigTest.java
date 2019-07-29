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

import org.apache.kafka.common.security.ssl.mock.TestProvider;
import org.apache.kafka.common.security.ssl.mock.TestProvider1;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.Provider;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;

public class SecurityConfigTest {

    private Provider provider = new TestProvider();
    private Provider provider1 = new TestProvider1();

    private void clearTestProviders() {
        Security.removeProvider(provider.getName());
        Security.removeProvider(provider1.getName());
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
        String customProviderClasses = provider.getClass().getName() + "," + provider1.getClass().getName();
        Map<String, String> configs = new HashMap<>();
        configs.put(SecurityConfig.SECURITY_PROVIDER_CLASS_CONFIG, customProviderClasses);
        SecurityConfig.addConfiguredSecurityProviders(configs);

        Provider[] providers = Security.getProviders();
        int providerIndex = getProviderIndexFromName(provider.getName(), providers);
        int provider1Index = getProviderIndexFromName(provider1.getName(), providers);

        // validations
        MatcherAssert.assertThat(provider.getName() + " provider not found at expected index", providerIndex == 0);
        MatcherAssert.assertThat(provider1.getName() + " provider not found at expected index", provider1Index == 1);
    }
}
