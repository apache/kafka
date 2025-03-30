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

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;
import org.jose4j.jwx.JsonWebStructure;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.security.oauthbearer.DefaultAssertionCreator.TOKEN_SIGNING_ALGORITHM_RS256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultAssertionCreatorTest {

    @Test
    public void testPrivateKeyId() throws Exception {
        TestAssertionJwtTemplate jwtTemplate = new TestAssertionJwtTemplate();
        jwtTemplate.setHeaders(Map.of("kid", "test-id"));

        KeyPair keyPair = generateKeyPair();
        Builder builder = new Builder()
            .setPrivateKeyFile(generatePrivateKeySecret(keyPair.getPrivate()));
        AssertionCreator assertionCreator = builder.build();
        String assertion = assertionCreator.create(jwtTemplate);
        JwtContext context = assertContext(builder, keyPair.getPublic(), assertion);
        List<JsonWebStructure> joseObjects = context.getJoseObjects();
        assertNotNull(joseObjects);
        assertEquals(1, joseObjects.size());
        JsonWebStructure jsonWebStructure = joseObjects.get(0);
        assertEquals("test-id", jsonWebStructure.getKeyIdHeaderValue());
    }

    @Test
    public void testPrivateKeySecret() throws Exception {
        TestAssertionJwtTemplate jwtTemplate = new TestAssertionJwtTemplate();
        KeyPair keyPair = generateKeyPair();
        Builder builder = new Builder()
            .setPrivateKeyFile(generatePrivateKeySecret(keyPair.getPrivate()));
        AssertionCreator assertionCreator = builder.build();
        String assertion = assertionCreator.create(jwtTemplate);
        assertClaims(builder, keyPair.getPublic(), assertion);
    }

    @Test
    public void testInvalidPrivateKeySecret() throws Exception {
        File privateKeyFile = generatePrivateKeySecret();
        long originalFileLength = privateKeyFile.length();
        int bytesToTruncate = 10;       // A single byte isn't enough

        // Intentionally "mangle" the private key secret by truncating the file.
        try (FileChannel channel = FileChannel.open(privateKeyFile.toPath(), StandardOpenOption.WRITE)) {
            long size = channel.size();
            assertEquals(originalFileLength, size);
            assertTrue(size > bytesToTruncate);
            channel.truncate(size - bytesToTruncate);
        }

        assertEquals(originalFileLength - bytesToTruncate, privateKeyFile.length());

        TestAssertionJwtTemplate jwtTemplate = new TestAssertionJwtTemplate();
        AssertionCreator assertionCreator = new Builder()
            .setPrivateKeyFile(privateKeyFile)
            .build();
        assertThrows(GeneralSecurityException.class, () -> assertionCreator.create(jwtTemplate));
    }

    @ParameterizedTest
    @CsvSource("RS256,ES256")
    public void testAlgorithm(String algorithm) throws Exception {
        KeyPair keyPair = generateKeyPair();
        Builder builder = new Builder()
            .setPrivateKeyFile(generatePrivateKeySecret(keyPair.getPrivate()))
            .setAlgorithm(algorithm);
        AssertionCreator assertionCreator = builder.build();

        TestAssertionJwtTemplate jwtTemplate = new TestAssertionJwtTemplate();
        String assertion = assertionCreator.create(jwtTemplate);

        assertClaims(builder, keyPair.getPublic(), assertion);

        JwtContext context = assertContext(builder, keyPair.getPublic(), assertion);
        List<JsonWebStructure> joseObjects = context.getJoseObjects();
        assertNotNull(joseObjects);
        assertEquals(1, joseObjects.size());
        JsonWebStructure jsonWebStructure = joseObjects.get(0);
        assertEquals(algorithm, jsonWebStructure.getAlgorithmHeaderValue());
    }

    @Test
    public void testInvalidAlgorithm() throws IOException {
        PrivateKey privateKey = generateKeyPair().getPrivate();
        Builder builder = new Builder()
            .setPrivateKeyFile(generatePrivateKeySecret(privateKey))
            .setAlgorithm("thisisnotvalid");
        DefaultAssertionCreator assertionCreator = builder.build();
        assertThrows(NoSuchAlgorithmException.class, assertionCreator::getSignature);
        assertThrows(
            NoSuchAlgorithmException.class,
            () -> assertionCreator.sign(privateKey, "dummy content"));
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
            .build();
    }

    private File generatePrivateKeySecret(PrivateKey privateKey) throws IOException {
        File file = File.createTempFile("private-", ".key");
        byte[] bytes = Base64.getEncoder().encode(privateKey.getEncoded());

        try (FileChannel channel = FileChannel.open(file.toPath(), EnumSet.of(StandardOpenOption.WRITE))) {
            Utils.writeFully(channel, ByteBuffer.wrap(bytes));
        }

        return file;
    }

    private File generatePrivateKeySecret() throws IOException {
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
        private String algorithm = TOKEN_SIGNING_ALGORITHM_RS256;
        private File privateKeyFile;

        public Builder setAlgorithm(String algorithm) {
            this.algorithm = algorithm;
            return this;
        }

        public Builder setPrivateKeyFile(File privateKeyFile) {
            this.privateKeyFile = privateKeyFile;
            return this;
        }

        private DefaultAssertionCreator build() {
            return new DefaultAssertionCreator(time, algorithm, privateKeyFile);
        }
    }

    private static class TestAssertionJwtTemplate implements AssertionJwtTemplate {

        private Map<String, Object> headers = new HashMap<>();
        private Map<String, Object> payload = new HashMap<>();

        public TestAssertionJwtTemplate setHeaders(Map<String, Object> headers) {
            this.headers = headers;
            return this;
        }

        public TestAssertionJwtTemplate setPayload(Map<String, Object> payload) {
            this.payload = payload;
            return this;
        }

        @Override
        public Map<String, Object> header() {
            return headers;
        }

        @Override
        public Map<String, Object> payload() {
            return payload;
        }
    }
}
