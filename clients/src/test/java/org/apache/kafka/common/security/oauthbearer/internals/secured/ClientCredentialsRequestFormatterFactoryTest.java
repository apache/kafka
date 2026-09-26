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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.security.KeyPair;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_ALGORITHM;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientCredentialsRequestFormatterFactoryTest extends OAuthBearerTest {

    private String savedAllowedFiles;

    @BeforeEach
    public void saveSystemProperty() {
        savedAllowedFiles = System.getProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG);
    }

    @AfterEach
    public void restoreSystemProperty() {
        if (savedAllowedFiles != null)
            System.setProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, savedAllowedFiles);
        else
            System.clearProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG);
    }

    @Test
    public void testCreateWithLocalClientAssertionWithUnencryptedPKCS8() throws Exception {
        KeyPair keyPair = generateKeyPair();
        File privateKeyFile = generatePrivateKey(keyPair.getPrivate());
        allowFile(privateKeyFile);

        Map<String, Object> configs = getAssertionConfigs(privateKeyFile.getAbsolutePath());
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        JaasOptionsUtils jou = new JaasOptionsUtils(getJaasOptions());

        HttpRequestFormatter formatter = ClientCredentialsRequestFormatterFactory.create(cu, jou);
        assertInstanceOf(ClientAssertionRequestFormatter.class, formatter);
    }

    @Test
    public void testCreateWithLocalClientAssertionWithPassphraseOnUnencryptedKey() throws Exception {
        KeyPair keyPair = generateKeyPair();
        File privateKeyFile = generatePrivateKey(keyPair.getPrivate());
        allowFile(privateKeyFile);

        Map<String, Object> configs = getAssertionConfigs(privateKeyFile.getAbsolutePath());
        configs.put(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE, new Password("my-passphrase"));
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        JaasOptionsUtils jou = new JaasOptionsUtils(getJaasOptions());

        // Providing a passphrase for an unencrypted key triggers an error during key loading
        assertThrows(JwtRetrieverException.class, () -> ClientCredentialsRequestFormatterFactory.create(cu, jou));
    }

    @Test
    public void testCreateWithFileClientAssertion() throws Exception {
        String jwt = createJwt("test-subject");
        File jwtFile = tempFile(jwt);
        allowFile(jwtFile);

        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_ASSERTION_FILE, jwtFile.getAbsolutePath());
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        JaasOptionsUtils jou = new JaasOptionsUtils(getJaasOptions());

        HttpRequestFormatter formatter = ClientCredentialsRequestFormatterFactory.create(cu, jou);
        assertInstanceOf(ClientAssertionRequestFormatter.class, formatter);
    }

    @Test
    public void testCreateWithClientAssertionThrowsExceptionForInvalidKeyPath() {
        Map<String, Object> configs = getAssertionConfigs("nonexistent/path/private.key");
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        JaasOptionsUtils jou = new JaasOptionsUtils(getJaasOptions());

        assertThrows(ConfigException.class, () -> ClientCredentialsRequestFormatterFactory.create(cu, jou));
    }

    @Test
    public void testCreateWithClientSecret() {
        Map<String, Object> configs = new HashMap<>();
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        JaasOptionsUtils jou = new JaasOptionsUtils(getJaasOptions());

        HttpRequestFormatter formatter = ClientCredentialsRequestFormatterFactory.create(cu, jou);
        assertInstanceOf(ClientSecretRequestFormatter.class, formatter);
    }

    @Test
    public void testCreateWithClientSecretFromConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, "config-client-id");
        configs.put(SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, new Password("config-client-secret"));
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        // Empty JAAS options — values come from config
        JaasOptionsUtils jou = new JaasOptionsUtils(Map.of());

        HttpRequestFormatter formatter = ClientCredentialsRequestFormatterFactory.create(cu, jou);
        assertInstanceOf(ClientSecretRequestFormatter.class, formatter);
    }

    @Test
    public void testAssertionPathTriggeredByIssConfig() throws Exception {
        KeyPair keyPair = generateKeyPair();
        File privateKeyFile = generatePrivateKey(keyPair.getPrivate());
        allowFile(privateKeyFile);

        Map<String, Object> configs = getAssertionConfigs(privateKeyFile.getAbsolutePath());
        // Ensure only ISS is configured (no assertion file)
        configs.remove(SASL_OAUTHBEARER_ASSERTION_FILE);
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        JaasOptionsUtils jou = new JaasOptionsUtils(getJaasOptions());

        HttpRequestFormatter formatter = ClientCredentialsRequestFormatterFactory.create(cu, jou);
        assertInstanceOf(ClientAssertionRequestFormatter.class, formatter);
    }

    @Test
    public void testAssertionPathTriggeredByAssertionFileConfig() throws Exception {
        String jwt = createJwt("file-trigger-test");
        File jwtFile = tempFile(jwt);
        allowFile(jwtFile);

        // Only assertion file configured, no ISS
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_ASSERTION_FILE, jwtFile.getAbsolutePath());
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        JaasOptionsUtils jou = new JaasOptionsUtils(getJaasOptions());

        HttpRequestFormatter formatter = ClientCredentialsRequestFormatterFactory.create(cu, jou);
        assertInstanceOf(ClientAssertionRequestFormatter.class, formatter);
    }

    /**
     * This test explicitly documents and verifies the three-tier fallback precedence mechanism:
     * 1. First Preference: File-based assertion (sasl.oauthbearer.assertion.file)
     * 2. Second Preference: Locally-generated assertion (sasl.oauthbearer.assertion.claim.iss + private key)
     * 3. Third Preference: Client secret (client.id + client.secret)
     *
     * When multiple authentication methods are configured simultaneously, the first preference
     * takes precedence and other configurations are silently ignored.
     */
    @Test
    public void testThreeTierFallbackPrecedence() throws Exception {
        String jwt = createJwt("precedence-test");
        File jwtFile = tempFile(jwt);
        allowFile(jwtFile);

        KeyPair keyPair = generateKeyPair();
        File privateKeyFile = generatePrivateKey(keyPair.getPrivate());
        allowFile(privateKeyFile);

        // Configure ALL three authentication methods
        Map<String, Object> configs = new HashMap<>();
        // First preference: file-based assertion
        configs.put(SASL_OAUTHBEARER_ASSERTION_FILE, jwtFile.getAbsolutePath());
        // Second preference: locally-generated assertion
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, "issuer");
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, "audience");
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, "subject");
        configs.put(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, privateKeyFile.getAbsolutePath());
        configs.put(SASL_OAUTHBEARER_ASSERTION_ALGORITHM, "RS256");

        ConfigurationUtils cu = new ConfigurationUtils(configs);
        // Third preference: client secret (from JAAS options)
        JaasOptionsUtils jou = new JaasOptionsUtils(getJaasOptions());

        HttpRequestFormatter formatter = ClientCredentialsRequestFormatterFactory.create(cu, jou);

        // Should use ClientAssertionRequestFormatter (not ClientSecretRequestFormatter)
        // because assertion configs are present
        assertInstanceOf(ClientAssertionRequestFormatter.class, formatter);

        // Verify the assertion comes from the file (first preference)
        // The file contains the JWT we created, so the body should contain that JWT
        String body = formatter.formatBody();
        assertInstanceOf(String.class, body);
        // Verify it contains client_assertion parameter
        assertTrue(body.contains("client_assertion="));
    }

    /**
     * Tests that when only locally-generated assertion configs are present (no file),
     * the second preference (locally-generated) is used, not the third preference (client secret).
     */
    @Test
    public void testSecondPreferenceTakesPrecedenceOverThird() throws Exception {
        KeyPair keyPair = generateKeyPair();
        File privateKeyFile = generatePrivateKey(keyPair.getPrivate());
        allowFile(privateKeyFile);

        // Configure locally-generated assertion (second preference) but not file (first preference)
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, "issuer");
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, "audience");
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, "subject");
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS, 600);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS, 60);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE, false);
        configs.put(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, privateKeyFile.getAbsolutePath());
        configs.put(SASL_OAUTHBEARER_ASSERTION_ALGORITHM, "RS256");

        ConfigurationUtils cu = new ConfigurationUtils(configs);
        // Client secret is available (third preference)
        JaasOptionsUtils jou = new JaasOptionsUtils(getJaasOptions());

        HttpRequestFormatter formatter = ClientCredentialsRequestFormatterFactory.create(cu, jou);

        // Should use ClientAssertionRequestFormatter (second preference)
        // even though client secret is available (third preference)
        assertInstanceOf(ClientAssertionRequestFormatter.class, formatter);
    }

    /**
     * Tests that when no assertion configs are present, the third preference
     * (client secret) is used as the fallback.
     */
    @Test
    public void testThirdPreferenceUsedWhenNoAssertionConfigs() {
        // No assertion configs at all
        Map<String, Object> configs = new HashMap<>();
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        // Only client secret available
        JaasOptionsUtils jou = new JaasOptionsUtils(getJaasOptions());

        HttpRequestFormatter formatter = ClientCredentialsRequestFormatterFactory.create(cu, jou);

        // Should use ClientSecretRequestFormatter (third preference/fallback)
        assertInstanceOf(ClientSecretRequestFormatter.class, formatter);
    }

    @Test
    public void testCreateWithNeitherAssertionNorClientCredentials() {
        ConfigurationUtils cu = new ConfigurationUtils(new HashMap<>());
        JaasOptionsUtils jou = new JaasOptionsUtils(Map.of());
        assertThrows(ConfigException.class, () -> ClientCredentialsRequestFormatterFactory.create(cu, jou));
    }

    @ParameterizedTest
    @MethodSource("urlEncodeHeaderSupplier")
    public void testValidateUrlEncodeHeader(Map<String, Object> configs, boolean expectedValue) {
        ConfigurationUtils cu = new ConfigurationUtils(configs);
        boolean actualValue = ClientCredentialsRequestFormatterFactory.validateUrlEncodeHeader(cu);
        assertEquals(expectedValue, actualValue);
    }

    private static Stream<Arguments> urlEncodeHeaderSupplier() {
        return Stream.of(
            Arguments.of(Collections.emptyMap(), DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE),
            Arguments.of(Collections.singletonMap(SASL_OAUTHBEARER_HEADER_URLENCODE, null), DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE),
            Arguments.of(Collections.singletonMap(SASL_OAUTHBEARER_HEADER_URLENCODE, true), true),
            Arguments.of(Collections.singletonMap(SASL_OAUTHBEARER_HEADER_URLENCODE, false), false)
        );
    }

    private static Map<String, Object> getAssertionConfigs(String privateKeyFilePath) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, "issuer");
        configs.put(SASL_OAUTHBEARER_ASSERTION_FILE, null);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, "audience");
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, "subject");
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS, 600);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS, 60);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE, true);
        configs.put(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, privateKeyFilePath);
        configs.put(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE, null);
        configs.put(SASL_OAUTHBEARER_ASSERTION_ALGORITHM, "RS256");
        return configs;
    }

    private static Map<String, Object> getJaasOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("clientId", "test-client-id");
        options.put("clientSecret", "test-client-secret");
        return options;
    }

    private void allowFile(File... files) {
        String current = System.getProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, "");
        StringBuilder sb = new StringBuilder(current);
        for (File file : files) {
            if (sb.length() > 0)
                sb.append(",");
            sb.append(file.getAbsolutePath());
        }
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, sb.toString());
    }
}
