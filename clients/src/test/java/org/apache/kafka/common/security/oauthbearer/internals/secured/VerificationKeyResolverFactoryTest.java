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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class VerificationKeyResolverFactoryTest extends OAuthBearerTest {

    @AfterEach
    public void tearDown() throws Exception {
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @Test
    public void testConfigureRefreshingFileVerificationKeyResolver() throws Exception {
        String file = tempFile("{}").toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, file);
        Map<String, ?> configs = Collections.singletonMap(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, file);
        CloseableVerificationKeyResolver verificationKeyResolver = VerificationKeyResolverFactory.create(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());
        assertThrowsWithMessage(ConfigException.class, () -> verificationKeyResolver.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()), "The JSON JWKS content does not include the keys member");
    }

    @Test
    public void testConfigureRefreshingFileVerificationKeyResolverWithInvalidDirectory() {
        // Should fail because the parent path doesn't exist.
        String file = new File("/tmp/this-directory-does-not-exist/foo.json").toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, file);
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, file);
        CloseableVerificationKeyResolver verificationKeyResolver = VerificationKeyResolverFactory.create(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());
        assertThrowsWithMessage(ConfigException.class, () -> verificationKeyResolver.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()), "that doesn't exist");
    }

    @Test
    public void testGetSharesResolverUntilLastClose() throws Exception {
        String file = tempFile("{\"keys\":[]}").toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, file);
        Map<String, ?> configs = Collections.singletonMap(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, file);

        CloseableVerificationKeyResolver first = VerificationKeyResolverFactory.get(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());
        CloseableVerificationKeyResolver second = VerificationKeyResolverFactory.get(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());
        assertSame(first, second);

        first.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());
        first.close();

        // The reference acquired by the second get() is still open, so the resolver stays cached.
        try (CloseableVerificationKeyResolver third = VerificationKeyResolverFactory.get(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries())) {
            assertSame(second, third);
        }

        second.close();

        // Closing the last reference removes the cache entry, so the next get() creates a new resolver
        // rather than returning the closed one.
        try (CloseableVerificationKeyResolver fourth = VerificationKeyResolverFactory.get(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries())) {
            assertNotSame(second, fourth);
        }
    }

    @Test
    public void testConfiguresDelegateOnceAndClosesItWithLastReference() throws Exception {
        CloseableVerificationKeyResolver delegate = mock(CloseableVerificationKeyResolver.class);
        Map<String, ?> configs = Collections.singletonMap(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, "https://jwks.example/configure-once");
        CloseableVerificationKeyResolver first = VerificationKeyResolverFactory.get(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries(), () -> delegate);
        CloseableVerificationKeyResolver second = VerificationKeyResolverFactory.get(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries(), () -> delegate);

        first.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());
        second.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());
        verify(delegate, times(1)).configure(any(), any(), any());

        first.close();
        verify(delegate, never()).close();

        second.close();
        verify(delegate, times(1)).close();
    }

    @Test
    public void testRetriesConfiguringDelegateAfterFailure() throws Exception {
        CloseableVerificationKeyResolver delegate = mock(CloseableVerificationKeyResolver.class);
        KafkaException error = new KafkaException("Could not refresh JWKS");
        doThrow(error).doNothing().when(delegate).configure(any(), any(), any());
        Map<String, ?> configs = Collections.singletonMap(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, "https://jwks.example/configure-retry");
        CloseableVerificationKeyResolver first = VerificationKeyResolverFactory.get(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries(), () -> delegate);
        CloseableVerificationKeyResolver second = VerificationKeyResolverFactory.get(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries(), () -> delegate);

        assertSame(error, assertThrows(KafkaException.class, () -> first.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries())));
        first.close();

        // The failed attempt did not configure the delegate, so the next caller tries again.
        second.configure(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());
        verify(delegate, times(2)).configure(any(), any(), any());

        second.close();
        verify(delegate, times(1)).close();
    }

    @Test
    public void testSaslOauthbearerTokenEndpointUrlIsNotAllowed() throws Exception {
        // Should fail if the URL is not allowed
        String file = tempFile("{}").toURI().toString();
        Map<String, ?> configs = getSaslConfigs(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, file);
        assertThrowsWithMessage(ConfigException.class, () -> VerificationKeyResolverFactory.create(configs, OAUTHBEARER_MECHANISM, getJaasConfigEntries()),
                ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }
}
