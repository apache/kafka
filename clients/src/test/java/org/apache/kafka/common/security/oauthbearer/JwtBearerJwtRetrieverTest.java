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

package org.apache.kafka.common.security.oauthbearer;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_ASSERTION_ALGORITHM;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_ALGORITHM;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JwtBearerJwtRetrieverTest extends OAuthBearerTest {

    @AfterEach
    public void tearDown() throws Exception {
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG);
    }

    @Test
    public void testConfigure() throws Exception {
        String tokenEndpointUrl = "https://www.example.com";
        String privateKeyFile = generatePrivateKey().getPath();

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, tokenEndpointUrl);
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, privateKeyFile);

        Map<String, ?> configs = getSaslConfigs(
            Map.of(
                SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl,
                SASL_OAUTHBEARER_ASSERTION_ALGORITHM, DEFAULT_SASL_OAUTHBEARER_ASSERTION_ALGORITHM,
                SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, privateKeyFile
            )
        );

        List<AppConfigurationEntry> jaasConfigEntries = getJaasConfigEntries();

        try (JwtBearerJwtRetriever jwtRetriever = new JwtBearerJwtRetriever()) {
            assertDoesNotThrow(() -> jwtRetriever.configure(configs, OAUTHBEARER_MECHANISM, jaasConfigEntries));
        }
    }

    @Test
    public void testConfigureWithMalformedPrivateKey() throws Exception {
        String tokenEndpointUrl = "https://www.example.com";
        String malformedPrivateKeyFile = tempFile().getPath();

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, tokenEndpointUrl);
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, malformedPrivateKeyFile);

        Map<String, ?> configs = getSaslConfigs(
            Map.of(
                SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl,
                SASL_OAUTHBEARER_ASSERTION_ALGORITHM, DEFAULT_SASL_OAUTHBEARER_ASSERTION_ALGORITHM,
                SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, malformedPrivateKeyFile
            )
        );

        List<AppConfigurationEntry> jaasConfigEntries = getJaasConfigEntries();

        try (JwtBearerJwtRetriever jwtRetriever = new JwtBearerJwtRetriever()) {
            JwtRetrieverException e = assertThrows(JwtRetrieverException.class, () -> jwtRetriever.configure(configs, OAUTHBEARER_MECHANISM, jaasConfigEntries));
            assertNotNull(e.getCause());
            assertInstanceOf(GeneralSecurityException.class, e.getCause());
        }
    }

    @Test
    public void testConfigureWithStaticAssertion() throws Exception {
        String tokenEndpointUrl = "https://www.example.com";
        String assertionFile = tempFile(createJwt("jdoe")).getPath();

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, tokenEndpointUrl);
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, assertionFile);

        Map<String, ?> configs = getSaslConfigs(
            Map.of(
                SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl,
                SASL_OAUTHBEARER_ASSERTION_ALGORITHM, DEFAULT_SASL_OAUTHBEARER_ASSERTION_ALGORITHM,
                SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile
            )
        );

        List<AppConfigurationEntry> jaasConfigEntries = getJaasConfigEntries();

        try (JwtBearerJwtRetriever jwtRetriever = new JwtBearerJwtRetriever()) {
            assertDoesNotThrow(() -> jwtRetriever.configure(configs, OAUTHBEARER_MECHANISM, jaasConfigEntries));
        }
    }

    @Test
    public void testConfigureWithInvalidPassphrase() throws Exception {
        String tokenEndpointUrl = "https://www.example.com";
        String privateKeyFile = generatePrivateKey().getPath();

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, tokenEndpointUrl);
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, privateKeyFile);

        Map<String, ?> configs = getSaslConfigs(
            Map.of(
                SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl,
                SASL_OAUTHBEARER_ASSERTION_ALGORITHM, DEFAULT_SASL_OAUTHBEARER_ASSERTION_ALGORITHM,
                SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, privateKeyFile,
                SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE, "this-passphrase-is-invalid"
            )
        );

        List<AppConfigurationEntry> jaasConfigEntries = getJaasConfigEntries();

        try (JwtBearerJwtRetriever jwtRetriever = new JwtBearerJwtRetriever()) {
            JwtRetrieverException e = assertThrows(JwtRetrieverException.class, () -> jwtRetriever.configure(configs, OAUTHBEARER_MECHANISM, jaasConfigEntries));
            assertNotNull(e.getCause());
            assertInstanceOf(IOException.class, e.getCause());
        }
    }
}
