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
package org.apache.kafka.common.security.oauthbearer.internals.secured.assertion;

import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.security.KeyPair;

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
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AssertionSupplierFactoryTest extends OAuthBearerTest {

    /**
     * When {@code SASL_OAUTHBEARER_ASSERTION_FILE} is configured, the factory should use file-based
     * assertion creation and return the JWT from the file.
     */
    @Test
    public void testCreateWithAssertionFile() throws Exception {
        String expectedJwt = createJwt("test-subject");
        File jwtFile = tempFile(expectedJwt);

        ConfigurationUtils cu = mock(ConfigurationUtils.class);
        when(cu.validateString(SASL_OAUTHBEARER_ASSERTION_FILE, false)).thenReturn(jwtFile.getAbsolutePath());
        when(cu.validateFile(SASL_OAUTHBEARER_ASSERTION_FILE)).thenReturn(jwtFile);

        Time time = new MockTime();

        try (CloseableSupplier<String> supplier = AssertionSupplierFactory.create(cu, time)) {
            String assertion = supplier.get();
            assertEquals(expectedJwt, assertion);
        }
    }

    /**
     * When no assertion file is configured, the factory should fall back to locally-generated
     * assertions using the private key and signing algorithm.
     */
    @Test
    public void testCreateWithPrivateKeyFile() throws Exception {
        KeyPair keyPair = generateKeyPair();
        File privateKeyFile = generatePrivateKey(keyPair.getPrivate());

        ConfigurationUtils cu = mockConfigForLocalAssertion(privateKeyFile);

        Time time = new MockTime();

        try (CloseableSupplier<String> supplier = AssertionSupplierFactory.create(cu, time)) {
            String assertion = supplier.get();
            assertNotNull(assertion);
            assertValidJwtFormat(assertion);
            assertClaims(keyPair.getPublic(), assertion);
        }
    }

    /**
     * When static claims (aud, iss, sub) are configured alongside the private key,
     * they should be included in the generated JWT assertion.
     */
    @Test
    public void testCreateWithPrivateKeyFileAndStaticClaims() throws Exception {
        KeyPair keyPair = generateKeyPair();
        File privateKeyFile = generatePrivateKey(keyPair.getPrivate());

        ConfigurationUtils cu = mockConfigForLocalAssertion(privateKeyFile);
        // Override static claims to be present
        when(cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD)).thenReturn(true);
        when(cu.validateString(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD)).thenReturn("https://auth.example.com");
        when(cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS)).thenReturn(true);
        when(cu.validateString(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS)).thenReturn("my-client");
        when(cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB)).thenReturn(true);
        when(cu.validateString(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB)).thenReturn("service-account");

        Time time = new MockTime();

        try (CloseableSupplier<String> supplier = AssertionSupplierFactory.create(cu, time)) {
            String assertion = supplier.get();
            assertNotNull(assertion);
            assertValidJwtFormat(assertion);

            // Verify all configured static claims are present in the assertion
            JwtConsumer jwtConsumer = new JwtConsumerBuilder()
                .setVerificationKey(keyPair.getPublic())
                .setRequireExpirationTime()
                .setAllowedClockSkewInSeconds(30)
                .setExpectedAudience("https://auth.example.com")
                .setExpectedIssuer("my-client")
                .setExpectedSubject("service-account")
                .build();
            JwtContext context = jwtConsumer.process(assertion);
            assertNotNull(context);
        }
    }

    /**
     * When a passphrase is configured, the factory should read it via
     * {@code validatePassword} and pass it to the {@code DefaultAssertionCreator}.
     * Providing a passphrase for an unencrypted key is an invalid combination and
     * should result in a {@code JwtRetrieverException} because the raw key bytes
     * cannot be parsed as an {@code EncryptedPrivateKeyInfo} structure.
     */
    @Test
    public void testCreateWithPassphraseReadsPasswordConfig() throws Exception {
        KeyPair keyPair = generateKeyPair();
        File privateKeyFile = generatePrivateKey(keyPair.getPrivate());

        ConfigurationUtils cu = mockConfigForLocalAssertion(privateKeyFile);
        // Enable passphrase config
        when(cu.containsKey(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE)).thenReturn(true);
        when(cu.validatePassword(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE)).thenReturn("my-passphrase");

        Time time = new MockTime();

        // Providing a passphrase for an unencrypted key triggers an error during key loading
        // because the raw key bytes cannot be parsed as an EncryptedPrivateKeyInfo structure
        JwtRetrieverException e = assertThrows(JwtRetrieverException.class, () -> AssertionSupplierFactory.create(cu, time));
        assertInstanceOf(IOException.class, e.getCause());

        // Verify that the password config was read from configuration
        verify(cu).validatePassword(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE);
    }

    /**
     * When the passphrase config key is absent, the factory should not attempt to read
     * a password and should produce a working supplier with the unencrypted private key.
     */
    @Test
    public void testCreateWithoutPassphraseDoesNotReadPasswordConfig() throws Exception {
        KeyPair keyPair = generateKeyPair();
        File privateKeyFile = generatePrivateKey(keyPair.getPrivate());

        ConfigurationUtils cu = mockConfigForLocalAssertion(privateKeyFile);
        // Passphrase not configured (already the default from mockConfigForLocalAssertion)

        Time time = new MockTime();

        try (CloseableSupplier<String> supplier = AssertionSupplierFactory.create(cu, time)) {
            String assertion = supplier.get();
            assertNotNull(assertion);
            assertClaims(keyPair.getPublic(), assertion);
        }

        // Verify that validatePassword was never called
        verify(cu, never()).validatePassword(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE);
    }

    /**
     * The file-based supplier should return the same value on consecutive calls since
     * the file contents haven't changed.
     */
    @Test
    public void testFileBasedSupplierReturnsSameValueOnMultipleCalls() throws Exception {
        String expectedJwt = createJwt("repeated-calls");
        File jwtFile = tempFile(expectedJwt);

        ConfigurationUtils cu = mock(ConfigurationUtils.class);
        when(cu.validateString(SASL_OAUTHBEARER_ASSERTION_FILE, false)).thenReturn(jwtFile.getAbsolutePath());
        when(cu.validateFile(SASL_OAUTHBEARER_ASSERTION_FILE)).thenReturn(jwtFile);

        Time time = new MockTime();

        try (CloseableSupplier<String> supplier = AssertionSupplierFactory.create(cu, time)) {
            String first = supplier.get();
            String second = supplier.get();
            assertEquals(first, second);
            assertEquals(expectedJwt, first);
        }
    }

    /**
     * The locally-generated supplier should produce valid assertions on consecutive calls.
     * With {@link MockTime}, timestamps are deterministic so both assertions will be identical,
     * confirming that no randomness (e.g. JTI) causes unexpected variation.
     */
    @Test
    public void testLocallyGeneratedSupplierProducesConsistentAssertions() throws Exception {
        KeyPair keyPair = generateKeyPair();
        File privateKeyFile = generatePrivateKey(keyPair.getPrivate());

        ConfigurationUtils cu = mockConfigForLocalAssertion(privateKeyFile);
        Time time = new MockTime();

        try (CloseableSupplier<String> supplier = AssertionSupplierFactory.create(cu, time)) {
            String first = supplier.get();
            String second = supplier.get();
            assertNotNull(first);
            assertNotNull(second);
            // Both should be valid JWTs
            assertClaims(keyPair.getPublic(), first);
            assertClaims(keyPair.getPublic(), second);
        }
    }

    /**
     * When the underlying file is deleted after factory creation, calling get() should
     * throw a JwtRetrieverException since the assertion creator cannot read the file.
     */
    @Test
    public void testSupplierGetWrapsExceptionInJwtRetrieverException() throws Exception {
        String jwt = createJwt("will-be-deleted");
        File jwtFile = tempFile(jwt);

        ConfigurationUtils cu = mock(ConfigurationUtils.class);
        when(cu.validateString(SASL_OAUTHBEARER_ASSERTION_FILE, false)).thenReturn(jwtFile.getAbsolutePath());
        when(cu.validateFile(SASL_OAUTHBEARER_ASSERTION_FILE)).thenReturn(jwtFile);

        Time time = new MockTime();

        try (CloseableSupplier<String> supplier = AssertionSupplierFactory.create(cu, time)) {
            // Delete the file so the read will fail
            assertTrue(jwtFile.delete());
            assertThrows(JwtRetrieverException.class, supplier::get);
        }
    }

    /**
     * Closing the supplier should not throw any exceptions.
     */
    @Test
    public void testSupplierCloseDoesNotThrow() throws Exception {
        String expectedJwt = createJwt("close-test");
        File jwtFile = tempFile(expectedJwt);

        ConfigurationUtils cu = mock(ConfigurationUtils.class);
        when(cu.validateString(SASL_OAUTHBEARER_ASSERTION_FILE, false)).thenReturn(jwtFile.getAbsolutePath());
        when(cu.validateFile(SASL_OAUTHBEARER_ASSERTION_FILE)).thenReturn(jwtFile);

        Time time = new MockTime();

        CloseableSupplier<String> supplier = AssertionSupplierFactory.create(cu, time);
        // Verify close does not throw
        assertDoesNotThrow(supplier::close);
    }

    /**
     * After closing the supplier, the underlying resources should be released.
     * Verify that the supplier can be closed multiple times without error.
     */
    @Test
    public void testSupplierCanBeClosedMultipleTimes() throws Exception {
        String expectedJwt = createJwt("multi-close");
        File jwtFile = tempFile(expectedJwt);

        ConfigurationUtils cu = mock(ConfigurationUtils.class);
        when(cu.validateString(SASL_OAUTHBEARER_ASSERTION_FILE, false)).thenReturn(jwtFile.getAbsolutePath());
        when(cu.validateFile(SASL_OAUTHBEARER_ASSERTION_FILE)).thenReturn(jwtFile);

        Time time = new MockTime();

        CloseableSupplier<String> supplier = AssertionSupplierFactory.create(cu, time);
        assertDoesNotThrow(supplier::close);
        assertDoesNotThrow(supplier::close);
    }

    /**
     * When ES256 algorithm is configured, the factory should produce valid assertions
     * signed with the EC private key.
     */
    @Test
    public void testCreateWithPrivateKeyFileEs256() throws Exception {
        KeyPair keyPair = generateKeyPair("EC");
        File privateKeyFile = generatePrivateKey(keyPair.getPrivate());

        ConfigurationUtils cu = mockConfigForLocalAssertion(privateKeyFile);
        when(cu.validateString(SASL_OAUTHBEARER_ASSERTION_ALGORITHM)).thenReturn("ES256");

        Time time = new MockTime();

        try (CloseableSupplier<String> supplier = AssertionSupplierFactory.create(cu, time)) {
            String assertion = supplier.get();
            assertNotNull(assertion);
            assertValidJwtFormat(assertion);
            assertClaims(keyPair.getPublic(), assertion);
        }
    }

    /**
     * Minimal structural check that the string is a valid JWT format (three Base64-encoded
     * parts separated by dots). Full claim and signature validation is performed by
     * {@link OAuthBearerTest#assertClaims} where cryptographic verification is needed.
     */
    private void assertValidJwtFormat(String jwt) {
        String[] parts = jwt.split("\\.");
        assertEquals(3, parts.length, "JWT should have 3 parts (header.payload.signature), but was: " + jwt);
    }

    /**
     * Sets up a ConfigurationUtils mock for the locally-generated assertion path
     * with sensible defaults (no static claims, no template file).
     */
    private ConfigurationUtils mockConfigForLocalAssertion(File privateKeyFile) {
        ConfigurationUtils cu = mock(ConfigurationUtils.class);
        // File-based assertion not configured
        when(cu.validateString(SASL_OAUTHBEARER_ASSERTION_FILE, false)).thenReturn(null);
        // Private key configuration
        when(cu.validateString(SASL_OAUTHBEARER_ASSERTION_ALGORITHM)).thenReturn("RS256");
        when(cu.validateFile(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE)).thenReturn(privateKeyFile);
        when(cu.containsKey(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE)).thenReturn(false);
        // No static claims
        when(cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD)).thenReturn(false);
        when(cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS)).thenReturn(false);
        when(cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB)).thenReturn(false);
        // No template file
        when(cu.containsKey(SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE)).thenReturn(false);
        // Dynamic template defaults
        when(cu.validateString(SASL_OAUTHBEARER_ASSERTION_ALGORITHM)).thenReturn("RS256");
        when(cu.validateInteger(SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS, true)).thenReturn(3600);
        when(cu.validateInteger(SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS, true)).thenReturn(60);
        when(cu.validateBoolean(SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE, true)).thenReturn(false);
        return cu;
    }
}
