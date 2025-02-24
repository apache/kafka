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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;
import org.jose4j.jwx.JsonWebStructure;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.apache.kafka.common.security.oauthbearer.internals.secured.JwtBearerRequestFormatter.GRANT_TYPE;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.JwtBearerRequestFormatter.TOKEN_SIGNING_ALGORITHM_RS256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JwtBearerRequestFormatterTest extends RequestFormatterTest {

    @Test
    public void testRequestBodyParameters() throws Exception {
        Builder builder = new Builder()
            .setPrivateKeySecret(generatePrivateKeySecret());
        JwtBearerRequestFormatter requestFormatter = builder.build();
        String assertion = requestFormatter.createAssertion();
        String requestBody = requestFormatter.formatBody();
        String expected = "grant_type=" + URLEncoder.encode(GRANT_TYPE, StandardCharsets.UTF_8) + "&assertion=" + assertion;
        assertEquals(
            expected,
            requestBody
        );
    }

    @Test
    public void testPrivateKeyId() throws Exception {
        KeyPair keyPair = generateKeyPair();
        Builder builder = new Builder()
            .setPrivateKeySecret(generatePrivateKeySecret(keyPair.getPrivate()))
            .setPrivateKeyId("test-id");
        JwtBearerRequestFormatter requestFormatter = builder.build();
        String assertion = requestFormatter.createAssertion();
        JwtContext context = assertContext(builder, keyPair.getPublic(), assertion);
        List<JsonWebStructure> joseObjects = context.getJoseObjects();
        assertNotNull(joseObjects);
        assertEquals(1, joseObjects.size());
        JsonWebStructure jsonWebStructure = joseObjects.get(0);
        assertEquals("test-id", jsonWebStructure.getKeyIdHeaderValue());
    }

    @Test
    public void testPrivateKeySecret() throws Exception {
        KeyPair keyPair = generateKeyPair();
        Builder builder = new Builder()
            .setPrivateKeySecret(generatePrivateKeySecret(keyPair.getPrivate()));
        JwtBearerRequestFormatter requestFormatter = builder.build();
        String assertion = requestFormatter.createAssertion();
        assertClaims(builder, keyPair.getPublic(), assertion);
    }

    @Test
    public void testInvalidPrivateKeySecret() throws Exception {
        // Intentionally "mangle" the private key secret by stripping off the first character.
        String privateKeySecret = generatePrivateKeySecret().substring(1);

        JwtBearerRequestFormatter requestFormatter = new Builder()
            .setPrivateKeySecret(privateKeySecret)
            .build();
        assertThrows(KafkaException.class, requestFormatter::formatBody);
    }

    @ParameterizedTest
    @CsvSource("RS256,ES256")
    public void testTokenSigningAlgo(String tokenSigningAlgo) throws Exception {
        KeyPair keyPair = generateKeyPair();
        Builder builder = new Builder()
            .setPrivateKeySecret(generatePrivateKeySecret(keyPair.getPrivate()))
            .setTokenSigningAlgo(tokenSigningAlgo);
        JwtBearerRequestFormatter requestFormatter = builder.build();
        String assertion = requestFormatter.createAssertion();
        assertClaims(builder, keyPair.getPublic(), assertion);

        JwtContext context = assertContext(builder, keyPair.getPublic(), assertion);
        List<JsonWebStructure> joseObjects = context.getJoseObjects();
        assertNotNull(joseObjects);
        assertEquals(1, joseObjects.size());
        JsonWebStructure jsonWebStructure = joseObjects.get(0);
        assertEquals(tokenSigningAlgo, jsonWebStructure.getAlgorithmHeaderValue());
    }

    @Test
    public void testInvalidTokenSigningAlgo() {
        PrivateKey privateKey = generateKeyPair().getPrivate();
        Builder builder = new Builder()
            .setPrivateKeySecret(generatePrivateKeySecret(privateKey))
            .setTokenSigningAlgo("thisisnotvalid");
        JwtBearerRequestFormatter requestFormatter = builder.build();
        assertThrows(NoSuchAlgorithmException.class, requestFormatter::getSignature);
        assertThrows(
            NoSuchAlgorithmException.class,
            () -> requestFormatter.sign(privateKey, "dummy content"));
    }

    @Test
    public void testContentTypeHeader() {
        JwtBearerRequestFormatter requestFormatter = new Builder().build();
        assertHeadersEqual(requestFormatter, Collections.singletonMap("Content-Type", "application/x-www-form-urlencoded"));
    }

    private JwtClaims assertClaims(Builder builder, PublicKey publicKey, String assertion) throws InvalidJwtException {
        JwtConsumer jwtConsumer = jwtConsumer(builder, publicKey);
        return jwtConsumer.processToClaims(assertion);
    }

    private JwtContext assertContext(Builder builder, PublicKey publicKey, String assertion) throws InvalidJwtException {
        JwtConsumer jwtConsumer = jwtConsumer(builder, publicKey);
        return jwtConsumer.process(assertion);
    }

    private JwtConsumer jwtConsumer(Builder builder, PublicKey publicKey) {
        return new JwtConsumerBuilder()
            .setVerificationKey(publicKey)
            .setRequireExpirationTime()
            .setAllowedClockSkewInSeconds(30)               // Sure, let's give it some slack
            .setExpectedSubject(builder.tokenSubject)
            .setExpectedIssuer(builder.tokenIssuer)
            .setExpectedAudience(builder.tokenAudience)
            .build();
    }

    private String generatePrivateKeySecret(PrivateKey privateKey) {
        return Base64.getEncoder().encodeToString(privateKey.getEncoded());
    }

    private String generatePrivateKeySecret() {
        return generatePrivateKeySecret(generateKeyPair().getPrivate());
    }

    private KeyPair generateKeyPair() {
        try {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(2048);
            return keyGen.generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Received unexpected error during private key generation", e);
        }
    }

    private static class Builder {

        private final Time time = new MockTime();
        private String privateKeyId = "testTokenSubject";
        private String privateKeySecret = "testTokenSubject";
        private String tokenSigningAlgo = TOKEN_SIGNING_ALGORITHM_RS256;
        private String tokenSubject = "testTokenSubject";
        private String tokenIssuer = "testTokenIssuer";
        private String tokenAudience = "testTokenAudience";
        private String tokenTargetAudience = "testTokenTargetAudience";

        public Builder setPrivateKeyId(String privateKeyId) {
            this.privateKeyId = privateKeyId;
            return this;
        }

        public Builder setPrivateKeySecret(String privateKeySecret) {
            this.privateKeySecret = privateKeySecret;
            return this;
        }

        public Builder setTokenSigningAlgo(String tokenSigningAlgo) {
            this.tokenSigningAlgo = tokenSigningAlgo;
            return this;
        }

        public Builder setTokenSubject(String tokenSubject) {
            this.tokenSubject = tokenSubject;
            return this;
        }

        public Builder setTokenIssuer(String tokenIssuer) {
            this.tokenIssuer = tokenIssuer;
            return this;
        }

        public Builder setTokenAudience(String tokenAudience) {
            this.tokenAudience = tokenAudience;
            return this;
        }

        public Builder setTokenTargetAudience(String tokenTargetAudience) {
            this.tokenTargetAudience = tokenTargetAudience;
            return this;
        }

        private JwtBearerRequestFormatter build() {
            return new JwtBearerRequestFormatter(
                time,
                privateKeyId,
                privateKeySecret,
                tokenSigningAlgo,
                tokenSubject,
                tokenIssuer,
                tokenAudience,
                tokenTargetAudience
            );
        }
    }

}
