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

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ClientCredentialsRequestFormatterFactory;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpJwtRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpRequestFormatter;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientRepresentation;
import org.keycloak.representations.idm.RealmRepresentation;
import org.testcontainers.DockerClientFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import dasniko.testcontainers.keycloak.KeycloakContainer;

import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
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
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for OAuth client assertion authentication using Keycloak testcontainer.
 *
 * <p>These tests validate the complete OAuth client assertion flow as specified by KIP-1258,
 * including RFC 7521 (Assertion Framework) and RFC 7523 (JWT Profile for Client Authentication):
 * <ul>
 *   <li>Client creates and signs JWT assertion using private key</li>
 *   <li>Client sends assertion to Keycloak token endpoint</li>
 *   <li>Keycloak validates signature and returns access token</li>
 *   <li>Access token can be used for authentication</li>
 * </ul>
 *
 * <p>Tests use a {@link KeycloakContainer} (from
 * <a href="https://github.com/dasniko/testcontainers-keycloak">testcontainers-keycloak</a>)
 * configured with:
 * <ul>
 *   <li>Test realm: kafka-authz</li>
 *   <li>Test client: kafka-producer (client assertion with JWT, RS256 + ES256)</li>
 *   <li>Test client: kafka-secret-client (client secret fallback)</li>
 * </ul>
 *
 * <p>Note: These tests require Docker to be available and will be skipped if Docker is not running.
 * They are tagged with {@code @Tag("integration")} for optional exclusion in quick builds.
 */
@Tag("integration")
public class ClientAssertionKeycloakIntegrationTest extends OAuthBearerTest {

    private static final String REALM_NAME = "kafka-authz";
    private static final String SHORT_LIVED_REALM_NAME = "kafka-authz-short";
    private static final int SHORT_TOKEN_LIFESPAN_SECONDS = 5;
    private static final String CLIENT_ID = "kafka-producer";
    private static final String SECRET_CLIENT_ID = "kafka-secret-client";
    private static final String CLIENT_SECRET = "test-client-secret-value";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static KeycloakContainer keycloak;

    private static String tokenEndpointUrl;
    private static String shortLivedTokenEndpointUrl;
    private static KeyPair rsaKeyPair;
    private static KeyPair ecKeyPair;
    private static File rsaPrivateKeyFile;
    private static File ecPrivateKeyFile;

    @BeforeAll
    public static void setUpKeycloak() throws Exception {
        // Skip the entire test class if Docker is not available
        org.junit.jupiter.api.Assumptions.assumeTrue(
            DockerClientFactory.instance().isDockerAvailable(),
            "Docker is not available - skipping integration tests"
        );

        keycloak = new KeycloakContainer();
        keycloak.start();

        String authServerUrl = keycloak.getAuthServerUrl();
        tokenEndpointUrl = authServerUrl + "/realms/" + REALM_NAME + "/protocol/openid-connect/token";
        shortLivedTokenEndpointUrl = authServerUrl + "/realms/" + SHORT_LIVED_REALM_NAME + "/protocol/openid-connect/token";

        // Generate key pairs for testing
        rsaKeyPair = generateRsaKeyPair();
        ecKeyPair = generateEcKeyPair();

        // Save private keys to temp files
        rsaPrivateKeyFile = writePrivateKeyToFile(rsaKeyPair);
        ecPrivateKeyFile = writePrivateKeyToFile(ecKeyPair);

        // Configure Keycloak using the admin client provided by the container
        try (Keycloak adminClient = keycloak.getKeycloakAdminClient()) {
            createRealm(adminClient, REALM_NAME, 300);
            createRealm(adminClient, SHORT_LIVED_REALM_NAME, SHORT_TOKEN_LIFESPAN_SECONDS);
            createAssertionClient(adminClient, REALM_NAME);
            createAssertionClient(adminClient, SHORT_LIVED_REALM_NAME);
            createSecretClient(adminClient);
        }

        // Allow the token endpoints and key files in the security allowlists
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG,
            tokenEndpointUrl + "," + shortLivedTokenEndpointUrl);
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG,
            rsaPrivateKeyFile.getAbsolutePath() + "," + ecPrivateKeyFile.getAbsolutePath());
    }

    @AfterAll
    public static void tearDown() {
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG);
        if (rsaPrivateKeyFile != null) rsaPrivateKeyFile.delete();
        if (ecPrivateKeyFile != null) ecPrivateKeyFile.delete();
        if (keycloak != null) {
            keycloak.stop();
        }
    }

    // ==================== Locally-Generated Assertion Tests ====================

    /**
     * Test client assertion authentication with locally-generated JWT using RS256 algorithm.
     *
     * <p>Verifies the complete end-to-end flow:
     * <ol>
     *   <li>Kafka client generates JWT assertion with configured claims</li>
     *   <li>JWT is signed with RSA private key using RS256</li>
     *   <li>Client sends assertion to Keycloak token endpoint</li>
     *   <li>Keycloak validates signature against registered public key and returns access token</li>
     *   <li>Access token is a valid JWT with expected structure and claims</li>
     * </ol>
     */
    @Test
    public void testClientAssertionWithLocallyGeneratedJWT_RS256() throws Exception {
        Map<String, Object> configs = createAssertionConfigs(
            rsaPrivateKeyFile, "RS256", CLIENT_ID, tokenEndpointUrl);

        try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
            String accessToken = retriever.retrieve();

            assertValidJwt(accessToken);
            assertTokenClaims(accessToken, CLIENT_ID);
        }
    }

    /**
     * Test client assertion authentication with locally-generated JWT using ES256 algorithm.
     *
     * <p>Validates that EC (Elliptic Curve) key pairs on the P-256 curve work correctly with
     * the SHA256withECDSA signature algorithm for client assertion authentication.
     */
    @Test
    public void testClientAssertionWithLocallyGeneratedJWT_ES256() throws Exception {
        Map<String, Object> configs = createAssertionConfigs(
            ecPrivateKeyFile, "ES256", CLIENT_ID, tokenEndpointUrl);

        try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
            String accessToken = retriever.retrieve();

            assertValidJwt(accessToken);
            assertTokenClaims(accessToken, CLIENT_ID);
        }
    }

    /**
     * Test that multiple successive token retrievals produce different access tokens,
     * verifying that the assertion is regenerated each time with fresh timestamps and jti.
     */
    @Test
    public void testMultipleTokenRetrievalsProduceDifferentTokens() throws Exception {
        Map<String, Object> configs = createAssertionConfigs(
            rsaPrivateKeyFile, "RS256", CLIENT_ID, tokenEndpointUrl);

        try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
            String token1 = retriever.retrieve();
            Thread.sleep(1000); // Ensure different iat/jti values
            String token2 = retriever.retrieve();

            assertValidJwt(token1);
            assertValidJwt(token2);
            assertNotEquals(token1, token2, "Successive tokens should be different");
        }
    }

    // ==================== File-Based Assertion Tests ====================

    /**
     * Test client assertion with file-based pre-generated JWT.
     *
     * <p>Verifies the end-to-end flow when using a pre-generated assertion file:
     * <ol>
     *   <li>A valid JWT assertion is generated and written to a file</li>
     *   <li>The client reads the assertion from the file</li>
     *   <li>The assertion is sent to Keycloak and validated</li>
     *   <li>An access token is returned</li>
     * </ol>
     */
    @Test
    public void testClientAssertionWithFileBasedJWT() throws Exception {
        String assertion = generateSignedAssertion(rsaKeyPair, "RS256", CLIENT_ID, tokenEndpointUrl);
        File assertionFile = writeStringToTempFile(assertion, "assertion-", ".jwt");

        try {
            addToFileAllowlist(assertionFile);

            Map<String, Object> configs = new HashMap<>(getSaslConfigs());
            configs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
            configs.put(SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());

            try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
                String accessToken = retriever.retrieve();

                assertValidJwt(accessToken);
                assertTokenClaims(accessToken, CLIENT_ID);
            }
        } finally {
            assertionFile.delete();
        }
    }

    // ==================== Token Refresh Tests ====================

    /**
     * Test token refresh with client assertion using short-lived tokens.
     *
     * <p>Uses a Keycloak realm configured with a very short {@code accessTokenLifespan}
     * (5 seconds) and client-side {@code sasl.login.refresh.*} configs tuned for aggressive
     * refresh. This simulates the real-world flow where {@code connections.max.reauth.ms}
     * on the broker triggers periodic re-authentication, causing the client to call
     * {@link ClientCredentialsJwtRetriever#retrieve()} to obtain a fresh token with a
     * newly-generated assertion.
     *
     * <p>Verifies:
     * <ol>
     *   <li>Initial token has an {@code exp} claim reflecting the short lifespan</li>
     *   <li>After expiration, a new retrieve() returns a fresh, valid token</li>
     *   <li>The refreshed token has an updated {@code exp} in the future</li>
     * </ol>
     */
    @Test
    public void testTokenRefreshWithClientAssertion() throws Exception {
        // Configure assertion against the short-lived realm
        Map<String, Object> configs = createAssertionConfigs(
            rsaPrivateKeyFile, "RS256", CLIENT_ID, shortLivedTokenEndpointUrl);

        // Set sasl.login.refresh.* configs for aggressive token refresh
        configs.put(SASL_LOGIN_REFRESH_WINDOW_FACTOR, 0.5);    // refresh at 50% of token lifetime
        configs.put(SASL_LOGIN_REFRESH_WINDOW_JITTER, 0.0);    // no jitter for deterministic testing
        configs.put(SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, (short) 1);  // allow refresh after 1 second
        configs.put(SASL_LOGIN_REFRESH_BUFFER_SECONDS, (short) 1);      // 1 second buffer before expiry

        try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
            // Retrieve initial token
            String initialToken = retriever.retrieve();
            assertValidJwt(initialToken);

            // Verify the token exp reflects the short lifespan
            JsonNode initialPayload = decodeJwtPayload(initialToken);
            long initialExp = initialPayload.get("exp").asLong();
            long initialIat = initialPayload.get("iat").asLong();
            assertEquals(SHORT_TOKEN_LIFESPAN_SECONDS, initialExp - initialIat,
                "Token lifespan should match the realm's accessTokenLifespan ("
                    + SHORT_TOKEN_LIFESPAN_SECONDS + "s)");

            // Wait for the token to expire
            Thread.sleep((SHORT_TOKEN_LIFESPAN_SECONDS + 1) * 1000L);

            // Retrieve a refreshed token (generates a new assertion, gets a new access token)
            String refreshedToken = retriever.retrieve();
            assertValidJwt(refreshedToken);

            assertNotEquals(initialToken, refreshedToken,
                "Refreshed token should differ from initial token");

            // Verify the refreshed token's exp is in the future
            JsonNode refreshedPayload = decodeJwtPayload(refreshedToken);
            long refreshedExp = refreshedPayload.get("exp").asLong();
            long nowSeconds = System.currentTimeMillis() / 1000;
            assertTrue(refreshedExp > nowSeconds,
                "Refreshed token's exp should be in the future");
        }
    }

    // ==================== HTTP Request Format Compliance (RFC 7523) ====================

    /**
     * Test HTTP request format compliance with RFC 7523.
     *
     * <p>Verifies that the HTTP request built by the formatter matches RFC 7523 specifications:
     * <ul>
     *   <li>{@code Content-Type: application/x-www-form-urlencoded}</li>
     *   <li>{@code grant_type=client_credentials}</li>
     *   <li>{@code client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer}</li>
     *   <li>{@code client_assertion=<JWT>} (three-part Base64URL-encoded JWT)</li>
     * </ul>
     */
    @Test
    public void testHttpRequestFormatCompliance() throws Exception {
        Map<String, Object> configs = createAssertionConfigs(
            rsaPrivateKeyFile, "RS256", CLIENT_ID, tokenEndpointUrl);

        ConfigurationUtils cu = new ConfigurationUtils(configs);
        JaasOptionsUtils jou = new JaasOptionsUtils(Collections.emptyMap());
        HttpRequestFormatter formatter = ClientCredentialsRequestFormatterFactory.create(cu, jou);

        // Verify headers
        Map<String, String> headers = formatter.formatHeaders();
        assertEquals("application/x-www-form-urlencoded", headers.get("Content-Type"),
            "Content-Type must be application/x-www-form-urlencoded per RFC 7523");
        assertEquals("application/json", headers.get("Accept"),
            "Accept header should request JSON response");
        assertEquals("no-cache", headers.get("Cache-Control"),
            "Cache-Control should be no-cache");

        // Verify body parameters
        String body = formatter.formatBody();
        assertNotNull(body, "Request body should not be null");

        assertTrue(body.contains("grant_type=client_credentials"),
            "Body must contain grant_type=client_credentials");
        assertTrue(body.contains(
            "client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"),
            "Body must contain URL-encoded client_assertion_type per RFC 7521");
        assertTrue(body.contains("client_assertion="),
            "Body must contain client_assertion parameter");

        // Verify the client_assertion value is a valid 3-part JWT
        String assertionParam = extractBodyParam(body, "client_assertion");
        assertNotNull(assertionParam, "client_assertion parameter should have a value");
        String[] jwtParts = assertionParam.split("\\.");
        assertEquals(3, jwtParts.length,
            "client_assertion should be a JWT with 3 parts (header.payload.signature)");
    }

    // ==================== Fallback to Client Secret ====================

    /**
     * Test fallback to client secret when no assertion configs are present.
     *
     * <p>Verifies that when only client_id and client_secret are configured (without any
     * assertion-related configs), the system falls back to traditional client secret
     * authentication and successfully obtains an access token.
     */
    @Test
    public void testFallbackToClientSecret() throws Exception {
        Map<String, Object> configs = new HashMap<>(getSaslConfigs());
        configs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
        configs.put(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, SECRET_CLIENT_ID);
        configs.put(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, new Password(CLIENT_SECRET));

        try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
            String accessToken = retriever.retrieve();

            assertValidJwt(accessToken);
            assertTokenClaims(accessToken, SECRET_CLIENT_ID);
        }
    }

    /**
     * Test that client secret formatter produces Basic auth header instead of assertion body.
     */
    @Test
    public void testClientSecretFormatterUsesBasicAuth() {
        Map<String, Object> configs = new HashMap<>(getSaslConfigs());
        configs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
        configs.put(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, SECRET_CLIENT_ID);
        configs.put(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, new Password(CLIENT_SECRET));

        ConfigurationUtils cu = new ConfigurationUtils(configs);
        JaasOptionsUtils jou = new JaasOptionsUtils(Collections.emptyMap());
        HttpRequestFormatter formatter = ClientCredentialsRequestFormatterFactory.create(cu, jou);

        Map<String, String> headers = formatter.formatHeaders();
        assertTrue(headers.containsKey("Authorization"),
            "Client secret formatter should use Authorization header");
        assertTrue(headers.get("Authorization").startsWith("Basic "),
            "Authorization header should use Basic scheme");

        String body = formatter.formatBody();
        assertTrue(body.contains("grant_type=client_credentials"),
            "Body should contain grant_type");
        assertFalse(body.contains("client_assertion"),
            "Client secret body should not contain client_assertion");
    }

    // ==================== Precedence Tests ====================

    /**
     * Test that file-based assertion takes precedence over locally-generated.
     *
     * <p>When both {@code sasl.oauthbearer.assertion.file} and locally-generated assertion
     * configs (e.g. {@code sasl.oauthbearer.assertion.claim.iss}) are configured, the
     * file-based assertion should be used (first preference) and locally-generated
     * configs should be ignored (with a WARN log).
     */
    @Test
    public void testFileAssertionTakesPrecedence() throws Exception {
        String assertion = generateSignedAssertion(rsaKeyPair, "RS256", CLIENT_ID, tokenEndpointUrl);
        File assertionFile = writeStringToTempFile(assertion, "assertion-", ".jwt");

        try {
            addToFileAllowlist(assertionFile);

            // Configure BOTH file-based and locally-generated assertion configs
            Map<String, Object> configs = new HashMap<>(getSaslConfigs());
            configs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
            // File-based assertion (first preference)
            configs.put(SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());
            // Locally-generated assertion configs (should be ignored)
            configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, CLIENT_ID);
            configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, CLIENT_ID);
            configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, tokenEndpointUrl);
            configs.put(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, rsaPrivateKeyFile.getAbsolutePath());
            configs.put(SASL_OAUTHBEARER_ASSERTION_ALGORITHM, "RS256");

            try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
                String accessToken = retriever.retrieve();
                assertValidJwt(accessToken);
            }
        } finally {
            assertionFile.delete();
        }
    }

    // ==================== Migration Scenario Tests ====================

    /**
     * Test that assertion configs take precedence over client secret when both are present.
     *
     * <p>Simulates a migration scenario where a user adds assertion configs alongside
     * existing client secret configs. The {@link ClientCredentialsRequestFormatterFactory} should
     * select the assertion path over client secret (fallback). The client_id is set
     * to the assertion client, and a deliberately wrong secret is provided. If the
     * factory incorrectly chose the secret path, the wrong secret would cause a
     * failure. The test succeeding proves the assertion path was selected.
     */
    @Test
    public void testAssertionConfigOverridesClientSecret() throws Exception {
        Map<String, Object> configs = createAssertionConfigs(
            rsaPrivateKeyFile, "RS256", CLIENT_ID, tokenEndpointUrl);

        // Also add client secret configs with a deliberately wrong secret.
        // If the factory incorrectly falls back to the secret path, authentication
        // will fail because the secret is invalid for this client.
        configs.put(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, CLIENT_ID);
        configs.put(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, new Password("wrong-secret"));

        try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
            String accessToken = retriever.retrieve();

            assertValidJwt(accessToken);
            assertTokenClaims(accessToken, CLIENT_ID);
        }
    }

    /**
     * Test migration from client assertion back to client secret.
     *
     * <p>Simulates the scenario where a user first authenticates with client assertion,
     * then reconfigures to use client secret only (e.g. by removing assertion configs).
     * Both sequential configurations should produce valid tokens from the respective
     * authentication methods.
     */
    @Test
    public void testReconfigureFromAssertionToClientSecret() throws Exception {
        // Step 1: Authenticate with client assertion
        Map<String, Object> assertionConfigs = createAssertionConfigs(
            rsaPrivateKeyFile, "RS256", CLIENT_ID, tokenEndpointUrl);

        try (ClientCredentialsJwtRetriever assertionRetriever = createRetriever(assertionConfigs)) {
            String assertionToken = assertionRetriever.retrieve();
            assertValidJwt(assertionToken);
            assertTokenClaims(assertionToken, CLIENT_ID);
        }

        // Step 2: Reconfigure with only client secret (assertion configs removed)
        Map<String, Object> secretConfigs = new HashMap<>(getSaslConfigs());
        secretConfigs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
        secretConfigs.put(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, SECRET_CLIENT_ID);
        secretConfigs.put(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, new Password(CLIENT_SECRET));

        try (ClientCredentialsJwtRetriever secretRetriever = createRetriever(secretConfigs)) {
            String secretToken = secretRetriever.retrieve();
            assertValidJwt(secretToken);
            assertTokenClaims(secretToken, SECRET_CLIENT_ID);
        }
    }

    /**
     * Test switching from locally-generated assertion to file-based assertion.
     *
     * <p>Simulates a migration where a user switches from dynamically generating
     * assertions (using private key + claims configs) to using a pre-generated
     * assertion file. Both configurations should produce valid tokens.
     */
    @Test
    public void testSwitchFromLocallyGeneratedToFileBased() throws Exception {
        // Step 1: Authenticate with locally-generated assertion
        Map<String, Object> localConfigs = createAssertionConfigs(
            rsaPrivateKeyFile, "RS256", CLIENT_ID, tokenEndpointUrl);

        try (ClientCredentialsJwtRetriever localRetriever = createRetriever(localConfigs)) {
            String localToken = localRetriever.retrieve();
            assertValidJwt(localToken);
            assertTokenClaims(localToken, CLIENT_ID);
        }

        // Step 2: Reconfigure with file-based assertion (locally-generated configs removed)
        String assertion = generateSignedAssertion(rsaKeyPair, "RS256", CLIENT_ID, tokenEndpointUrl);
        File assertionFile = writeStringToTempFile(assertion, "migration-assertion-", ".jwt");

        try {
            addToFileAllowlist(assertionFile);

            Map<String, Object> fileConfigs = new HashMap<>(getSaslConfigs());
            fileConfigs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
            fileConfigs.put(SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());

            try (ClientCredentialsJwtRetriever fileRetriever = createRetriever(fileConfigs)) {
                String fileToken = fileRetriever.retrieve();
                assertValidJwt(fileToken);
                assertTokenClaims(fileToken, CLIENT_ID);
            }
        } finally {
            assertionFile.delete();
        }
    }

    /**
     * Test switching from file-based assertion to locally-generated assertion.
     *
     * <p>Simulates the reverse migration where a user switches from a pre-generated
     * assertion file to dynamically generating assertions. Both configurations should
     * produce valid tokens.
     */
    @Test
    public void testSwitchFromFileBasedToLocallyGenerated() throws Exception {
        // Step 1: Authenticate with file-based assertion
        String assertion = generateSignedAssertion(rsaKeyPair, "RS256", CLIENT_ID, tokenEndpointUrl);
        File assertionFile = writeStringToTempFile(assertion, "migration-assertion-", ".jwt");

        try {
            addToFileAllowlist(assertionFile);

            Map<String, Object> fileConfigs = new HashMap<>(getSaslConfigs());
            fileConfigs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
            fileConfigs.put(SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());

            try (ClientCredentialsJwtRetriever fileRetriever = createRetriever(fileConfigs)) {
                String fileToken = fileRetriever.retrieve();
                assertValidJwt(fileToken);
                assertTokenClaims(fileToken, CLIENT_ID);
            }
        } finally {
            assertionFile.delete();
        }

        // Step 2: Reconfigure with locally-generated assertion (file config removed)
        Map<String, Object> localConfigs = createAssertionConfigs(
            rsaPrivateKeyFile, "RS256", CLIENT_ID, tokenEndpointUrl);

        try (ClientCredentialsJwtRetriever localRetriever = createRetriever(localConfigs)) {
            String localToken = localRetriever.retrieve();
            assertValidJwt(localToken);
            assertTokenClaims(localToken, CLIENT_ID);
        }
    }

    // ==================== Error Handling Tests ====================

    /**
     * Test error handling for invalid signature (key not registered with Keycloak).
     *
     * <p>When a client signs the assertion with a private key whose corresponding
     * public key is NOT registered in Keycloak, authentication should fail with
     * an unretryable error rather than entering a retry loop.
     */
    @Test
    public void testClientAssertionWithInvalidSignature() throws Exception {
        // Generate a new key pair NOT registered with Keycloak
        KeyPair unregisteredKeyPair = generateRsaKeyPair();
        File unregisteredKeyFile = writePrivateKeyToFile(unregisteredKeyPair);

        try {
            addToFileAllowlist(unregisteredKeyFile);

            Map<String, Object> configs = createAssertionConfigs(
                unregisteredKeyFile, "RS256", CLIENT_ID, tokenEndpointUrl);

            try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
                Exception exception = assertThrows(Exception.class, retriever::retrieve,
                    "Authentication with unregistered key should fail");
                assertNotNull(exception.getMessage(), "Error message should not be null");
            }
        } finally {
            unregisteredKeyFile.delete();
        }
    }

    /**
     * Test error handling for expired file-based assertion.
     *
     * <p>When a file-based assertion with exp in the past is used,
     * Keycloak should reject it.
     */
    @Test
    public void testClientAssertionWithExpiredAssertion() throws Exception {
        String expiredAssertion = generateExpiredAssertion(rsaKeyPair, "RS256", CLIENT_ID, tokenEndpointUrl);
        File assertionFile = writeStringToTempFile(expiredAssertion, "expired-assertion-", ".jwt");

        try {
            addToFileAllowlist(assertionFile);

            Map<String, Object> configs = new HashMap<>(getSaslConfigs());
            configs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
            configs.put(SASL_OAUTHBEARER_ASSERTION_FILE, assertionFile.getAbsolutePath());

            try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
                assertThrows(Exception.class, retriever::retrieve,
                    "Expired assertion should be rejected by Keycloak");
            }
        } finally {
            assertionFile.delete();
        }
    }

    /**
     * Test error handling for invalid audience claim.
     *
     * <p>When the aud claim doesn't match the Keycloak token endpoint URL,
     * Keycloak should reject the assertion.
     */
    @Test
    public void testClientAssertionWithInvalidAudience() throws Exception {
        Map<String, Object> configs = new HashMap<>(getSaslConfigs());
        configs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
        configs.put(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, rsaPrivateKeyFile.getAbsolutePath());
        configs.put(SASL_OAUTHBEARER_ASSERTION_ALGORITHM, "RS256");
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, CLIENT_ID);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, CLIENT_ID);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, "https://wrong-audience.example.com/token");
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS, 300);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS, 0);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE, true);

        try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
            assertThrows(Exception.class, retriever::retrieve,
                "Assertion with invalid audience should be rejected by Keycloak");
        }
    }

    /**
     * Test error handling for a malformed (non-JWT) file-based assertion.
     *
     * <p>When the assertion file contains garbage content that is not a valid JWT,
     * the token endpoint should reject it with an error.
     */
    @Test
    public void testClientAssertionWithMalformedAssertionFile() throws Exception {
        File malformedFile = writeStringToTempFile(
            "this-is-not-a-valid-jwt-assertion", "malformed-assertion-", ".jwt");

        try {
            addToFileAllowlist(malformedFile);

            Map<String, Object> configs = new HashMap<>(getSaslConfigs());
            configs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
            configs.put(SASL_OAUTHBEARER_ASSERTION_FILE, malformedFile.getAbsolutePath());

            // CachedFile eagerly validates the JWT structure during configure(),
            // so the error may surface in createRetriever rather than retrieve().
            assertThrows(Exception.class, () -> {
                try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
                    retriever.retrieve();
                }
            }, "Malformed assertion file content should be rejected");
        } finally {
            malformedFile.delete();
        }
    }

    // ==================== Missing Configuration Tests ====================

    /**
     * Test that a clear error is thrown when the token endpoint URL is missing.
     *
     * <p>The {@code sasl.oauthbearer.token.endpoint.url} config is required for all
     * OAuth authentication methods. When it is absent, configure() should fail with
     * a descriptive error.
     */
    @Test
    public void testMissingTokenEndpointUrl() {
        Map<String, Object> configs = new HashMap<>(getSaslConfigs());
        // Deliberately omit SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL
        configs.put(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, rsaPrivateKeyFile.getAbsolutePath());
        configs.put(SASL_OAUTHBEARER_ASSERTION_ALGORITHM, "RS256");
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, CLIENT_ID);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, CLIENT_ID);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, tokenEndpointUrl);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS, 300);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS, 0);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE, true);

        assertThrows(Exception.class, () -> {
            try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
                retriever.retrieve();
            }
        }, "Missing token endpoint URL should produce an error");
    }

    /**
     * Test that a clear error is thrown when the private key file is missing for
     * locally-generated assertion mode.
     *
     * <p>When {@code sasl.oauthbearer.assertion.claim.iss} is set (enabling local
     * generation mode) but {@code sasl.oauthbearer.assertion.private.key.file} is
     * absent, an error should be raised during configure() or retrieve().
     */
    @Test
    public void testMissingPrivateKeyFile() {
        Map<String, Object> configs = new HashMap<>(getSaslConfigs());
        configs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
        // Deliberately omit SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE
        configs.put(SASL_OAUTHBEARER_ASSERTION_ALGORITHM, "RS256");
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, CLIENT_ID);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, CLIENT_ID);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, tokenEndpointUrl);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS, 300);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS, 0);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE, true);

        assertThrows(Exception.class, () -> {
            try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
                retriever.retrieve();
            }
        }, "Missing private key file should produce an error");
    }

    /**
     * Test that a clear error is thrown when the private key file path does not exist.
     *
     * <p>When {@code sasl.oauthbearer.assertion.private.key.file} points to a file
     * that does not exist on disk, an error should be raised during configure() or
     * retrieve().
     */
    @Test
    public void testNonexistentPrivateKeyFile() {
        Map<String, Object> configs = new HashMap<>(getSaslConfigs());
        configs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl);
        configs.put(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, "/tmp/nonexistent-key-file.pem");
        configs.put(SASL_OAUTHBEARER_ASSERTION_ALGORITHM, "RS256");
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, CLIENT_ID);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, CLIENT_ID);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, tokenEndpointUrl);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS, 300);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS, 0);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE, true);

        assertThrows(Exception.class, () -> {
            try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
                retriever.retrieve();
            }
        }, "Nonexistent private key file should produce an error");
    }

    // ==================== Network Failure Tests ====================

    /**
     * Test error handling when the token endpoint is unreachable.
     *
     * <p>When the token endpoint URL points to a host/port that is not listening,
     * the retriever should attempt retries with exponential backoff (governed by
     * {@code sasl.login.retry.backoff.ms} / {@code sasl.login.retry.backoff.max.ms})
     * and eventually fail with a descriptive error rather than hanging indefinitely.
     *
     * <p>This test uses very short timeouts and retry intervals to keep execution fast.
     */
    @Test
    public void testNetworkFailureUnreachableTokenEndpoint() throws Exception {
        // Use a localhost URL on a port that is almost certainly not listening.
        // Port 1 requires root and is conventionally unused.
        String unreachableUrl = "http://localhost:1/realms/nonexistent/protocol/openid-connect/token";

        // Allow this URL in the security allowlist
        String existing = System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, "");
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, existing + "," + unreachableUrl);

        try {
            Map<String, Object> configs = createAssertionConfigs(
                rsaPrivateKeyFile, "RS256", CLIENT_ID, unreachableUrl);

            // Use very short timeouts and retries so the test completes quickly
            configs.put(SASL_LOGIN_CONNECT_TIMEOUT_MS, 1000);
            configs.put(SASL_LOGIN_READ_TIMEOUT_MS, 1000);
            configs.put(SASL_LOGIN_RETRY_BACKOFF_MS, 100L);
            configs.put(SASL_LOGIN_RETRY_BACKOFF_MAX_MS, 1000L);

            try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
                long start = System.currentTimeMillis();
                assertThrows(Exception.class, retriever::retrieve,
                    "Retrieval from unreachable endpoint should fail with an exception");
                long elapsed = System.currentTimeMillis() - start;

                // Verify the request did not hang forever; with 1s max backoff it should
                // complete well within 30 seconds even with retries.
                assertTrue(elapsed < 30_000,
                    "Request to unreachable endpoint should not hang; took " + elapsed + " ms");
            }
        } finally {
            // Restore the original allowlist
            System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, existing);
        }
    }

    /**
     * Test that unretryable HTTP errors (4xx) are not retried.
     *
     * <p>When a client authenticates with a key that Keycloak does not recognize,
     * the token endpoint returns 400 Bad Request. The {@link HttpJwtRetriever}
     * classifies 4xx codes as unretryable and should fail immediately rather than
     * entering the retry loop. This test verifies the fast-fail behaviour by
     * checking the elapsed time is well below the maximum retry backoff window.
     */
    @Test
    public void testUnretryableHttpErrorFailsFast() throws Exception {
        // Generate a key pair NOT registered with Keycloak
        KeyPair unregisteredKeyPair = generateRsaKeyPair();
        File unregisteredKeyFile = writePrivateKeyToFile(unregisteredKeyPair);

        try {
            addToFileAllowlist(unregisteredKeyFile);

            Map<String, Object> configs = createAssertionConfigs(
                unregisteredKeyFile, "RS256", CLIENT_ID, tokenEndpointUrl);

            // Set a generous retry window so we can verify it was NOT used
            configs.put(SASL_LOGIN_RETRY_BACKOFF_MS, 500L);
            configs.put(SASL_LOGIN_RETRY_BACKOFF_MAX_MS, 10_000L);

            try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
                long start = System.currentTimeMillis();
                assertThrows(Exception.class, retriever::retrieve,
                    "Unregistered key should fail authentication");
                long elapsed = System.currentTimeMillis() - start;

                // 400 Bad Request is in UNRETRYABLE_HTTP_CODES, so the retry loop should
                // break immediately. If retries were attempted we'd see ~10s+ elapsed.
                assertTrue(elapsed < 5_000,
                    "Unretryable 4xx error should fail fast without retries; took " + elapsed + " ms");
            }
        } finally {
            unregisteredKeyFile.delete();
        }
    }

    // ==================== Full Retriever Lifecycle ====================

    /**
     * Test that the full {@link ClientCredentialsJwtRetriever} lifecycle works end-to-end:
     * configure, retrieve, and close.
     */
    @Test
    public void testClientCredentialsJwtRetrieverLifecycle() throws Exception {
        Map<String, Object> configs = createAssertionConfigs(
            rsaPrivateKeyFile, "RS256", CLIENT_ID, tokenEndpointUrl);

        ClientCredentialsJwtRetriever retriever = new ClientCredentialsJwtRetriever();

        assertDoesNotThrow(() -> retriever.configure(
            configs, "OAUTHBEARER", getJaasConfigEntries()));

        String token = retriever.retrieve();
        assertValidJwt(token);

        assertDoesNotThrow(retriever::close);
    }

    // ==================== Encrypted Private Key Tests ====================

    /**
     * Test client assertion with an encrypted RSA private key (PKCS#8 + PBE).
     *
     * <p>Verifies that:
     * <ol>
     *   <li>An encrypted PKCS#8 PEM private key file can be loaded</li>
     *   <li>The passphrase is used to decrypt the private key</li>
     *   <li>The decrypted key correctly signs the JWT assertion (RS256)</li>
     *   <li>Keycloak validates the signature and returns an access token</li>
     * </ol>
     */
    @Test
    public void testEncryptedRsaPrivateKeyWithPassphrase() throws Exception {
        String passphrase = "test-rsa-passphrase";
        File encryptedKeyFile = writeEncryptedPrivateKeyToFile(rsaKeyPair, passphrase);

        try {
            addToFileAllowlist(encryptedKeyFile);

            Map<String, Object> configs = createAssertionConfigs(
                encryptedKeyFile, "RS256", CLIENT_ID, tokenEndpointUrl);
            configs.put(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE,
                new org.apache.kafka.common.config.types.Password(passphrase));

            try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
                String accessToken = retriever.retrieve();

                assertValidJwt(accessToken);
                assertTokenClaims(accessToken, CLIENT_ID);
            }
        } finally {
            encryptedKeyFile.delete();
        }
    }

    /**
     * Test client assertion with an encrypted EC private key (PKCS#8 + PBE).
     *
     * <p>Verifies the encrypted key path works for EC keys with the ES256 algorithm.
     */
    @Test
    public void testEncryptedEcPrivateKeyWithPassphrase() throws Exception {
        String passphrase = "test-ec-passphrase";
        File encryptedKeyFile = writeEncryptedPrivateKeyToFile(ecKeyPair, passphrase);

        try {
            addToFileAllowlist(encryptedKeyFile);

            Map<String, Object> configs = createAssertionConfigs(
                encryptedKeyFile, "ES256", CLIENT_ID, tokenEndpointUrl);
            configs.put(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE,
                new org.apache.kafka.common.config.types.Password(passphrase));

            try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
                String accessToken = retriever.retrieve();

                assertValidJwt(accessToken);
                assertTokenClaims(accessToken, CLIENT_ID);
            }
        } finally {
            encryptedKeyFile.delete();
        }
    }

    /**
     * Test that using the wrong passphrase for an encrypted private key fails.
     *
     * <p>Verifies that an incorrect passphrase produces a meaningful error
     * rather than silently producing bad signatures.
     */
    @Test
    public void testEncryptedPrivateKeyWithWrongPassphrase() throws Exception {
        String correctPassphrase = "correct-passphrase";
        String wrongPassphrase = "wrong-passphrase";
        File encryptedKeyFile = writeEncryptedPrivateKeyToFile(rsaKeyPair, correctPassphrase);

        try {
            addToFileAllowlist(encryptedKeyFile);

            Map<String, Object> configs = createAssertionConfigs(
                encryptedKeyFile, "RS256", CLIENT_ID, tokenEndpointUrl);
            configs.put(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE,
                new org.apache.kafka.common.config.types.Password(wrongPassphrase));

            // The wrong passphrase error can surface during configure() (CachedFile eagerly
            // loads the key) or during retrieve(), so wrap the entire flow in assertThrows.
            assertThrows(Exception.class, () -> {
                try (ClientCredentialsJwtRetriever retriever = createRetriever(configs)) {
                    retriever.retrieve();
                }
            }, "Decrypting with wrong passphrase should fail");
        } finally {
            encryptedKeyFile.delete();
        }
    }

    // ==================== Helper: Retriever Factory ====================

    private ClientCredentialsJwtRetriever createRetriever(Map<String, Object> configs) {
        ClientCredentialsJwtRetriever retriever = new ClientCredentialsJwtRetriever();
        retriever.configure(configs, "OAUTHBEARER", getJaasConfigEntries());
        return retriever;
    }

    // ==================== Helper: Config Builders ====================

    private Map<String, Object> createAssertionConfigs(File privateKeyFile,
                                                       String algorithm,
                                                       String clientId,
                                                       String audience) {
        Map<String, Object> configs = new HashMap<>(getSaslConfigs());
        configs.put(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, audience);
        configs.put(SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE, privateKeyFile.getAbsolutePath());
        configs.put(SASL_OAUTHBEARER_ASSERTION_ALGORITHM, algorithm);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, clientId);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, clientId);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, audience);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS, 300);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS, 0);
        configs.put(SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE, true);
        return configs;
    }

    // ==================== Helper: JWT Assertions ====================

    private void assertValidJwt(String jwt) {
        assertNotNull(jwt, "JWT should not be null");
        assertFalse(jwt.isEmpty(), "JWT should not be empty");
        String[] parts = jwt.split("\\.");
        assertEquals(3, parts.length,
            "JWT should have 3 parts (header.payload.signature), got: " + parts.length);

        Base64.Decoder decoder = Base64.getUrlDecoder();
        for (int i = 0; i < 3; i++) {
            final int idx = i;
            assertDoesNotThrow(() -> decoder.decode(parts[idx]),
                "JWT part " + idx + " should be valid Base64URL");
        }
    }

    private void assertTokenClaims(String jwt, String expectedClientId) throws Exception {
        assertTokenClaims(jwt, expectedClientId, REALM_NAME);
    }

    private void assertTokenClaims(String jwt, String expectedClientId, String realmName) throws Exception {
        JsonNode payload = decodeJwtPayload(jwt);

        assertTrue(payload.has("exp"), "Token should have exp claim");
        assertTrue(payload.has("iat"), "Token should have iat claim");
        assertTrue(payload.has("iss"), "Token should have iss claim");

        String expectedIssuer = keycloak.getAuthServerUrl() + "/realms/" + realmName;
        assertEquals(expectedIssuer, payload.get("iss").asText(),
            "Token issuer should match Keycloak realm");

        if (payload.has("azp")) {
            assertEquals(expectedClientId, payload.get("azp").asText(),
                "Token authorized party (azp) should match client ID");
        }
    }

    private static JsonNode decodeJwtPayload(String jwt) throws Exception {
        String[] parts = jwt.split("\\.");
        String payloadJson = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
        return OBJECT_MAPPER.readTree(payloadJson);
    }

    private static String extractBodyParam(String body, String paramName) {
        for (String param : body.split("&")) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2 && kv[0].equals(paramName)) {
                return kv[1];
            }
        }
        return null;
    }

    // ==================== Helper: Key Generation ====================

    private static KeyPair generateRsaKeyPair() throws Exception {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        return keyGen.generateKeyPair();
    }

    private static KeyPair generateEcKeyPair() throws Exception {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("EC");
        keyGen.initialize(new ECGenParameterSpec("secp256r1"));
        return keyGen.generateKeyPair();
    }

    private static File writePrivateKeyToFile(KeyPair keyPair) throws IOException {
        File file = File.createTempFile("private-", ".key");
        file.deleteOnExit();
        byte[] encoded = Base64.getEncoder().encode(keyPair.getPrivate().getEncoded());
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(new String(encoded, StandardCharsets.UTF_8));
        }
        return file;
    }

    /**
     * Encrypts a private key using PKCS#8 PBE (Password-Based Encryption) and writes
     * it to a PEM-formatted temporary file with {@code -----BEGIN ENCRYPTED PRIVATE KEY-----} headers.
     */
    private static File writeEncryptedPrivateKeyToFile(KeyPair keyPair, String passphrase) throws Exception {
        byte[] pkcs8Bytes = keyPair.getPrivate().getEncoded();

        // Use PBEWithSHA1AndDESede which is universally available across JDK implementations
        String pbeAlgorithm = "PBEWithSHA1AndDESede";
        byte[] salt = new byte[8];
        new java.security.SecureRandom().nextBytes(salt);
        int iterations = 10000;

        javax.crypto.spec.PBEParameterSpec pbeParamSpec =
            new javax.crypto.spec.PBEParameterSpec(salt, iterations);
        javax.crypto.spec.PBEKeySpec pbeKeySpec =
            new javax.crypto.spec.PBEKeySpec(passphrase.toCharArray());
        javax.crypto.SecretKeyFactory skf =
            javax.crypto.SecretKeyFactory.getInstance(pbeAlgorithm);
        javax.crypto.SecretKey pbeKey = skf.generateSecret(pbeKeySpec);

        javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance(pbeAlgorithm);
        cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, pbeKey, pbeParamSpec);
        byte[] encryptedBytes = cipher.doFinal(pkcs8Bytes);

        // Build EncryptedPrivateKeyInfo (PKCS#8 encrypted format)
        java.security.AlgorithmParameters algParams = cipher.getParameters();
        javax.crypto.EncryptedPrivateKeyInfo encryptedInfo =
            new javax.crypto.EncryptedPrivateKeyInfo(algParams, encryptedBytes);
        byte[] derEncoded = encryptedInfo.getEncoded();

        // Write as PEM
        File file = File.createTempFile("encrypted-private-", ".key");
        file.deleteOnExit();
        String base64 = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8))
            .encodeToString(derEncoded);
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("-----BEGIN ENCRYPTED PRIVATE KEY-----\n");
            writer.write(base64);
            writer.write("\n-----END ENCRYPTED PRIVATE KEY-----\n");
        }
        return file;
    }

    // ==================== Helper: Signed Assertion Generation ====================

    private static String generateSignedAssertion(KeyPair keyPair, String algorithm,
                                                  String clientId, String audience) throws Exception {
        long nowSeconds = System.currentTimeMillis() / 1000;

        Map<String, Object> header = Map.of("alg", algorithm, "typ", "JWT");
        Map<String, Object> payload = new HashMap<>();
        payload.put("iss", clientId);
        payload.put("sub", clientId);
        payload.put("aud", audience);
        payload.put("iat", nowSeconds);
        payload.put("exp", nowSeconds + 300);
        payload.put("nbf", nowSeconds);
        payload.put("jti", java.util.UUID.randomUUID().toString());

        return signJwt(header, payload, keyPair, algorithm);
    }

    private static String generateExpiredAssertion(KeyPair keyPair, String algorithm,
                                                   String clientId, String audience) throws Exception {
        long nowSeconds = System.currentTimeMillis() / 1000;

        Map<String, Object> header = Map.of("alg", algorithm, "typ", "JWT");
        Map<String, Object> payload = new HashMap<>();
        payload.put("iss", clientId);
        payload.put("sub", clientId);
        payload.put("aud", audience);
        payload.put("iat", nowSeconds - 600);
        payload.put("exp", nowSeconds - 300); // Expired 5 minutes ago
        payload.put("jti", java.util.UUID.randomUUID().toString());

        return signJwt(header, payload, keyPair, algorithm);
    }

    private static String signJwt(Map<String, Object> header, Map<String, Object> payload,
                                  KeyPair keyPair, String algorithm) throws Exception {
        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        String headerB64 = encoder.encodeToString(
            OBJECT_MAPPER.writeValueAsBytes(header));
        String payloadB64 = encoder.encodeToString(
            OBJECT_MAPPER.writeValueAsBytes(payload));
        String content = headerB64 + "." + payloadB64;

        java.security.Signature sig = "ES256".equals(algorithm)
            ? java.security.Signature.getInstance("SHA256withECDSAinP1363Format")
            : java.security.Signature.getInstance("SHA256withRSA");
        sig.initSign(keyPair.getPrivate());
        sig.update(content.getBytes(StandardCharsets.UTF_8));
        String signature = encoder.encodeToString(sig.sign());

        return content + "." + signature;
    }

    // ==================== Helper: File Utilities ====================

    private static File writeStringToTempFile(String content, String prefix, String suffix) throws IOException {
        File file = File.createTempFile(prefix, suffix);
        file.deleteOnExit();
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
        }
        return file;
    }

    private static void addToFileAllowlist(File file) {
        String existing = System.getProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG, "");
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG,
            existing + "," + file.getAbsolutePath());
    }

    // ==================== Helper: Keycloak Admin Setup ====================

    /**
     * Creates a realm in Keycloak with the given name and access token lifespan.
     *
     * @param adminClient          the Keycloak admin client
     * @param realmName            the realm name
     * @param accessTokenLifespan  the access token lifespan in seconds
     */
    private static void createRealm(Keycloak adminClient, String realmName, int accessTokenLifespan) {
        RealmRepresentation realm = new RealmRepresentation();
        realm.setRealm(realmName);
        realm.setEnabled(true);
        realm.setSslRequired("none");
        realm.setAccessTokenLifespan(accessTokenLifespan);

        try {
            adminClient.realms().create(realm);
        } catch (Exception e) {
            // 409 Conflict: realm already exists - ignore
            if (!e.getMessage().contains("409")) {
                throw e;
            }
        }
    }

    /**
     * Creates a client configured for client assertion authentication (JWT) and
     * registers both RSA and EC public keys as inline JWKS.
     */
    private static void createAssertionClient(Keycloak adminClient, String realmName) {
        RealmResource realmResource = adminClient.realm(realmName);

        // Build JWKS containing both RSA and EC public keys
        String rsaJwk = convertRsaToJwk((RSAPublicKey) rsaKeyPair.getPublic(), "rsa-key-1");
        String ecJwk = convertEcToJwk((ECPublicKey) ecKeyPair.getPublic(), "ec-key-1");
        String jwks = String.format("{\"keys\":[%s,%s]}", rsaJwk, ecJwk);

        ClientRepresentation client = new ClientRepresentation();
        client.setClientId(CLIENT_ID);
        client.setName("Kafka Producer (Assertion Auth)");
        client.setEnabled(true);
        client.setClientAuthenticatorType("client-jwt");
        client.setPublicClient(false);
        client.setServiceAccountsEnabled(true);
        client.setStandardFlowEnabled(false);
        client.setDirectAccessGrantsEnabled(false);
        client.setProtocol("openid-connect");

        Map<String, String> attributes = new HashMap<>();
        attributes.put("use.jwks.url", "false");
        // Enable inline JWKS string for public key verification (Keycloak 26.0+)
        attributes.put("use.jwks.string", "true");
        // Register public keys as inline JWKS (attribute name: jwks.string)
        attributes.put("jwks.string", jwks);
        client.setAttributes(attributes);

        realmResource.clients().create(client);
    }

    /**
     * Creates a client configured for client secret authentication (for fallback tests).
     */
    private static void createSecretClient(Keycloak adminClient) {
        RealmResource realmResource = adminClient.realm(REALM_NAME);

        ClientRepresentation client = new ClientRepresentation();
        client.setClientId(SECRET_CLIENT_ID);
        client.setName("Kafka Client (Secret Auth)");
        client.setEnabled(true);
        client.setClientAuthenticatorType("client-secret");
        client.setSecret(CLIENT_SECRET);
        client.setPublicClient(false);
        client.setServiceAccountsEnabled(true);
        client.setStandardFlowEnabled(false);
        client.setDirectAccessGrantsEnabled(false);
        client.setProtocol("openid-connect");

        realmResource.clients().create(client);
    }

    // ==================== Helper: JWK Conversion (RFC 7517) ====================

    private static String convertRsaToJwk(RSAPublicKey rsaKey, String keyId) {
        byte[] nBytes = toUnsignedBytes(rsaKey.getModulus().toByteArray());
        byte[] eBytes = rsaKey.getPublicExponent().toByteArray();

        String n = Base64.getUrlEncoder().withoutPadding().encodeToString(nBytes);
        String e = Base64.getUrlEncoder().withoutPadding().encodeToString(eBytes);

        return String.format(
            "{\"kty\":\"RSA\",\"kid\":\"%s\",\"use\":\"sig\",\"alg\":\"RS256\",\"n\":\"%s\",\"e\":\"%s\"}",
            keyId, n, e);
    }

    private static String convertEcToJwk(ECPublicKey ecKey, String keyId) {
        byte[] xBytes = toFixedLengthUnsigned(ecKey.getW().getAffineX().toByteArray(), 32);
        byte[] yBytes = toFixedLengthUnsigned(ecKey.getW().getAffineY().toByteArray(), 32);

        String x = Base64.getUrlEncoder().withoutPadding().encodeToString(xBytes);
        String y = Base64.getUrlEncoder().withoutPadding().encodeToString(yBytes);

        return String.format(
            "{\"kty\":\"EC\",\"kid\":\"%s\",\"use\":\"sig\",\"alg\":\"ES256\",\"crv\":\"P-256\",\"x\":\"%s\",\"y\":\"%s\"}",
            keyId, x, y);
    }

    /** Removes leading zero byte from BigInteger two's-complement representation. */
    private static byte[] toUnsignedBytes(byte[] bytes) {
        if (bytes.length > 1 && bytes[0] == 0) {
            byte[] result = new byte[bytes.length - 1];
            System.arraycopy(bytes, 1, result, 0, result.length);
            return result;
        }
        return bytes;
    }

    /** Converts a BigInteger byte array to a fixed-length unsigned byte array (for EC coordinates). */
    private static byte[] toFixedLengthUnsigned(byte[] bytes, int length) {
        if (bytes.length == length) {
            return bytes;
        } else if (bytes.length == length + 1 && bytes[0] == 0) {
            byte[] result = new byte[length];
            System.arraycopy(bytes, 1, result, 0, length);
            return result;
        } else if (bytes.length < length) {
            byte[] result = new byte[length];
            System.arraycopy(bytes, 0, result, length - bytes.length, bytes.length);
            return result;
        } else {
            throw new IllegalArgumentException(
                "Byte array of length " + bytes.length + " cannot be converted to " + length + " bytes");
        }
    }
}
