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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;
import org.jose4j.jwx.JsonWebStructure;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionUtils.TOKEN_SIGNING_ALGORITHM_RS256;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionUtils.getSignature;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionUtils.privateKey;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionUtils.sign;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultAssertionCreatorTest {

    @Test
    public void testPrivateKey() throws Exception {
        KeyPair keyPair = generateKeyPair();
        Builder builder = new Builder()
            .setPrivateKeyFile(generatePrivateKey(keyPair.getPrivate()));
        AssertionJwtTemplate jwtTemplate = new LayeredAssertionJwtTemplate(
            new StaticAssertionJwtTemplate(Map.of("kid", "test-id"), Map.of()),
            new DynamicAssertionJwtTemplate(
                new MockTime(),
                builder.algorithm,
                3600,
                60,
                false
            )
        );

        try (AssertionCreator assertionCreator = builder.build()) {
            String assertion = assertionCreator.create(jwtTemplate);
            assertClaims(keyPair.getPublic(), assertion);
        }
    }

    @Test
    public void testPrivateKeyWithPassphrase() throws Exception {
        KeyPair keyPair = loadKeyPair(
            "test_id_rsa_x509.pub",
            "test_id_rsa.pk8",
            Optional.of("thisisatest")
        );

        Builder builder = new Builder()
            .setPrivateKeyFile(generatePrivateKey(keyPair.getPrivate()));

        AssertionJwtTemplate jwtTemplate = new LayeredAssertionJwtTemplate(
            new StaticAssertionJwtTemplate(Map.of("kid", "test-id"), Map.of()),
            new DynamicAssertionJwtTemplate(
                new MockTime(),
                builder.algorithm,
                3600,
                60,
                false
            )
        );

        try (AssertionCreator assertionCreator = builder.build()) {
            String assertion = assertionCreator.create(jwtTemplate);
            JwtContext context = assertContext(keyPair.getPublic(), assertion);
            List<JsonWebStructure> joseObjects = context.getJoseObjects();
            assertNotNull(joseObjects);
            assertEquals(1, joseObjects.size());
            JsonWebStructure jsonWebStructure = joseObjects.get(0);
            assertEquals("test-id", jsonWebStructure.getKeyIdHeaderValue());
        }
    }

    @Test
    public void testPrivateKeyId() throws Exception {
        KeyPair keyPair = generateKeyPair();
        Builder builder = new Builder()
            .setPrivateKeyFile(generatePrivateKey(keyPair.getPrivate()));

        AssertionJwtTemplate jwtTemplate = new LayeredAssertionJwtTemplate(
            new StaticAssertionJwtTemplate(Map.of("kid", "test-id"), Map.of()),
            new DynamicAssertionJwtTemplate(
                new MockTime(),
                builder.algorithm,
                3600,
                60,
                false
            )
        );

        try (AssertionCreator assertionCreator = builder.build()) {
            String assertion = assertionCreator.create(jwtTemplate);
            JwtContext context = assertContext(keyPair.getPublic(), assertion);
            List<JsonWebStructure> joseObjects = context.getJoseObjects();
            assertNotNull(joseObjects);
            assertEquals(1, joseObjects.size());
            JsonWebStructure jsonWebStructure = joseObjects.get(0);
            assertEquals("test-id", jsonWebStructure.getKeyIdHeaderValue());
        }
    }

    @Test
    public void testInvalidPrivateKey() throws Exception {
        File privateKeyFile = generatePrivateKey();
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

        KafkaException e = assertThrows(KafkaException.class, () -> new Builder().setPrivateKeyFile(privateKeyFile).build());
        assertNotNull(e.getCause());
        assertInstanceOf(GeneralSecurityException.class, e.getCause());
    }

    @ParameterizedTest
    @CsvSource("RS256,ES256")
    public void testAlgorithm(String algorithm) throws Exception {
        KeyPair keyPair = generateKeyPair();
        Builder builder = new Builder()
            .setPrivateKeyFile(generatePrivateKey(keyPair.getPrivate()))
            .setAlgorithm(algorithm);

        String assertion;

        try (AssertionCreator assertionCreator = builder.build()) {
            AssertionJwtTemplate jwtTemplate = new DynamicAssertionJwtTemplate(
                new MockTime(),
                algorithm,
                3600,
                60,
                false
            );
            assertion = assertionCreator.create(jwtTemplate);
        }

        assertClaims(keyPair.getPublic(), assertion);

        JwtContext context = assertContext(keyPair.getPublic(), assertion);
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
            .setPrivateKeyFile(generatePrivateKey(privateKey))
            .setAlgorithm("thisisnotvalid");
        assertThrows(NoSuchAlgorithmException.class, () -> getSignature(builder.algorithm));
        assertThrows(
            NoSuchAlgorithmException.class,
            () -> sign(builder.algorithm, privateKey, "dummy content"));
    }

    private void assertClaims(PublicKey publicKey, String assertion) throws InvalidJwtException {
        JwtConsumer jwtConsumer = jwtConsumer(publicKey);
        jwtConsumer.processToClaims(assertion);
    }

    private JwtContext assertContext(PublicKey publicKey, String assertion) throws InvalidJwtException {
        JwtConsumer jwtConsumer = jwtConsumer(publicKey);
        return jwtConsumer.process(assertion);
    }

    private JwtConsumer jwtConsumer(PublicKey publicKey) {
        return new JwtConsumerBuilder()
            .setVerificationKey(publicKey)
            .setRequireExpirationTime()
            .setAllowedClockSkewInSeconds(30)               // Sure, let's give it some slack
            .build();
    }

    private File generatePrivateKey(PrivateKey privateKey) throws IOException {
        File file = File.createTempFile("private-", ".key");
        byte[] bytes = Base64.getEncoder().encode(privateKey.getEncoded());

        try (FileChannel channel = FileChannel.open(file.toPath(), EnumSet.of(StandardOpenOption.WRITE))) {
            Utils.writeFully(channel, ByteBuffer.wrap(bytes));
        }

        return file;
    }

    private File generatePrivateKey() throws IOException {
        return generatePrivateKey(generateKeyPair().getPrivate());
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

    private KeyPair loadKeyPair(String publicKeyResourceName, String privateKeyResourceName, Optional<String> passphrase) throws IOException, GeneralSecurityException {
        byte[] publicKeyBytes = loadResource(publicKeyResourceName);

        String publicKeyPem = new String(publicKeyBytes, StandardCharsets.UTF_8)
            .replaceAll("-----BEGIN PUBLIC KEY-----", "")
            .replaceAll("-----END PUBLIC KEY-----", "")
            .replaceAll("\\s+", "");
        byte[] decodedPublicKeyPem = Base64.getDecoder().decode(publicKeyPem);
        X509EncodedKeySpec spec = new X509EncodedKeySpec(decodedPublicKeyPem);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        PublicKey publicKey = kf.generatePublic(spec);

        byte[] privateKeyBytes = loadResource(privateKeyResourceName);
        PrivateKey privateKey = privateKey(privateKeyBytes, passphrase);

        return new KeyPair(publicKey, privateKey);
    }

    private byte[] loadResource(String resourceName) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4096);
        int numBytesRead = 0;
        String fullyQualifiedResourceName = "common/security/oauthbearer/" + resourceName;

        try (InputStream in = getClass().getClassLoader().getResourceAsStream(fullyQualifiedResourceName)) {
            if (in == null)
                throw new FileNotFoundException("Classpath resource " + fullyQualifiedResourceName + " was not found");

            numBytesRead = Utils.readFully(in, buf);
        }

        byte[] bytes = new byte[numBytesRead];
        buf.flip();
        buf.get(bytes);
        return bytes;
    }

    private static class Builder {

        private final Time time = new MockTime();
        private String algorithm = TOKEN_SIGNING_ALGORITHM_RS256;
        private File privateKeyFile;
        private Optional<String> passphrase = Optional.empty();

        public Builder setAlgorithm(String algorithm) {
            this.algorithm = algorithm;
            return this;
        }

        public Builder setPrivateKeyFile(File privateKeyFile) {
            this.privateKeyFile = privateKeyFile;
            return this;
        }

        public Builder setPassphrase(String passphrase) {
            this.passphrase = Optional.of(passphrase);
            return this;
        }

        private DefaultAssertionCreator build() {
            return new DefaultAssertionCreator(algorithm, privateKeyFile, passphrase);
        }
    }
}
