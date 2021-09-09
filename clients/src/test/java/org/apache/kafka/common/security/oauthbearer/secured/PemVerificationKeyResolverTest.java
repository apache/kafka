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

package org.apache.kafka.common.security.oauthbearer.secured;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.security.Key;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Collections;
import org.jose4j.jwa.AlgorithmConstraints.ConstraintType;
import org.jose4j.jwk.EcJwkGenerator;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jwk.RsaJwkGenerator;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.EllipticCurves;
import org.jose4j.keys.RsaKeyUtil;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.jose4j.lang.UnresolvableKeyException;
import org.junit.jupiter.api.Test;

public class PemVerificationKeyResolverTest extends OAuthBearerTest {

    @Test
    public void testEcdsa() throws Exception {
        PublicJsonWebKey jwk = EcJwkGenerator.generateJwk(EllipticCurves.P256);
        String pemEncoded = RsaKeyUtil.pemEncode((PublicKey) jwk.getKey());
        testResolution(pemEncoded, AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256, jwk);
    }

    @Test
    public void testRsa() throws Exception {
        RsaJsonWebKey jwk = RsaJwkGenerator.generateJwk(2048);
        String pemEncoded = RsaKeyUtil.pemEncode((PublicKey) jwk.getKey());
        testResolution(pemEncoded, AlgorithmIdentifiers.RSA_USING_SHA256, jwk);
    }

    @Test
    public void testIgnoreNonPemFiles() throws Exception {
        // First create some non-PEM file in the same directory, then go about the normal
        // test and make sure it is ignored...
        File tmpPemDir = createTempPemDir();
        createTempFile(tmpPemDir, "README-", ".md", "some text file");

        RsaJsonWebKey jwk = RsaJwkGenerator.generateJwk(2048);
        String pemEncoded = RsaKeyUtil.pemEncode((PublicKey) jwk.getKey());
        testResolution(pemEncoded,
            tmpPemDir,
            AlgorithmIdentifiers.RSA_USING_SHA256,
            jwk,
            jwk.getRsaPrivateKey());
    }

    @Test
    public void testNoPemFiles() throws Exception {
        File tmpPemDir = createTempPemDir();
        RsaJsonWebKey jwk = RsaJwkGenerator.generateJwk(2048);

        assertThrowsWithMessage(UnresolvableKeyException.class,
            () ->
                testResolution(null,
                    tmpPemDir,
                    AlgorithmIdentifiers.RSA_USING_SHA256,
                    jwk,
                    jwk.getRsaPrivateKey()),
            "Unable to find a suitable verification key for JWS");
    }

    @Test
    public void testMalformedPem() throws Exception {
        File tmpPemDir = createTempPemDir();
        RsaJsonWebKey jwk = RsaJwkGenerator.generateJwk(2048);

        assertThrowsWithMessage(IOException.class,
            () ->
                testResolution("This is not a valid PEM file",
                    tmpPemDir,
                    AlgorithmIdentifiers.RSA_USING_SHA256,
                    jwk,
                    jwk.getRsaPrivateKey()),
            "malformed contents");
    }

    private void testResolution(String pemEncoded,
        File tmpPemDir,
        String alg,
        JsonWebKey jwk,
        PrivateKey privateKey)
        throws Exception {
        String kid = null;

        if (pemEncoded != null) {
            File pemFile = createTempFile(tmpPemDir, "key-", ".pem", pemEncoded);
            kid = PemVerificationKeyResolver.toKid(pemFile);
        }

        if (kid != null) {
            jwk.setKeyId(kid);
            jwk.setAlgorithm(alg);
        }

        JsonWebSignature jws = new JsonWebSignature();
        jws.setKey(privateKey);

        if (kid != null) {
            jws.setKeyIdHeaderValue(kid);
            jws.setAlgorithmHeaderValue(alg);
        }

        jws.setPayload("{}");

        PemVerificationKeyResolver factory = new PemVerificationKeyResolver(tmpPemDir.toPath());
        VerificationKeyResolver vkr = factory.createDelegate();
        Key key = vkr.resolveKey(jws, Collections.emptyList());
        assertNotNull(key);

        JwtConsumerBuilder jwtConsumerBuilder = new JwtConsumerBuilder();

        JwtConsumer jwtConsumer = jwtConsumerBuilder
            .setJwsAlgorithmConstraints(ConstraintType.PERMIT, alg)
            .setVerificationKeyResolver(vkr)
            .build();

        jwtConsumer.process(jws.getCompactSerialization());
    }

    private void testResolution(String pemEncoded, String alg, JsonWebKey jwk) throws Exception {
        File tmpPemDir = createTempPemDir();

        try (PemVerificationKeyResolver vkr = new PemVerificationKeyResolver(tmpPemDir.getAbsoluteFile().toPath())) {
            vkr.init();

            File pemFile = createTempFile(tmpPemDir, "key-", ".pem", pemEncoded);
            String kid = PemVerificationKeyResolver.toKid(pemFile);

            jwk.setKeyId(kid);
            jwk.setAlgorithm(alg);

            JsonWebSignature jws = new JsonWebSignature();
            jws.setKey(jwk.getKey());
            jws.setKeyIdHeaderValue(kid);
            jws.setAlgorithmHeaderValue(alg);
            jws.setPayload("{}");

            try {
                log.debug("resolveKey invocation started");

                Key key = vkr.resolveKey(jws, Collections.emptyList());
                assertNotNull(key);
            } finally {
                log.debug("resolveKey invocation completed");
            }
        }
    }

}
