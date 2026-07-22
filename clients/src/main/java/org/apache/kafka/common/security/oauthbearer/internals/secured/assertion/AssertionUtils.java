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

import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.utils.Time;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_ALGORITHM;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE;

/**
 * Set of utilities for the OAuth JWT assertion logic.
 */
public class AssertionUtils {

    public static final String TOKEN_SIGNING_ALGORITHM_RS256 = "RS256";
    public static final String TOKEN_SIGNING_ALGORITHM_ES256 = "ES256";

    /**
     * Strips PEM headers/footers and whitespace from a PEM-encoded private key string,
     * returning the raw Base64-encoded key content as bytes.
     */
    public static byte[] stripPemEncoding(String pemContents) {
        String stripped = pemContents
            .replace("-----BEGIN PRIVATE KEY-----", "")
            .replace("-----END PRIVATE KEY-----", "")
            .replace("-----BEGIN ENCRYPTED PRIVATE KEY-----", "")
            .replace("-----END ENCRYPTED PRIVATE KEY-----", "")
            .replace("\n", "")
            .replace("\r", "");
        return stripped.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Inspired by {@code org.apache.kafka.common.security.ssl.DefaultSslEngineFactory.PemStore}, which is not
     * visible to reuse directly.
     */
    public static PrivateKey privateKey(byte[] privateKeyContents,
                                        String signingAlgorithm,
                                        Optional<String> passphrase) throws GeneralSecurityException, IOException {
        byte[] decodedKeyBytes = Base64.getDecoder().decode(privateKeyContents);
        PKCS8EncodedKeySpec keySpec;
        if (passphrase.isPresent()) {
            EncryptedPrivateKeyInfo keyInfo = new EncryptedPrivateKeyInfo(decodedKeyBytes);
            String encryptionAlgorithm = keyInfo.getAlgName();
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(encryptionAlgorithm);
            SecretKey pbeKey = secretKeyFactory.generateSecret(new PBEKeySpec(passphrase.get().toCharArray()));
            Cipher cipher = Cipher.getInstance(encryptionAlgorithm);
            cipher.init(Cipher.DECRYPT_MODE, pbeKey, keyInfo.getAlgParameters());
            keySpec = keyInfo.getKeySpec(cipher);
        } else {
            keySpec = new PKCS8EncodedKeySpec(decodedKeyBytes);
        }

        KeyFactory keyFactory = KeyFactory.getInstance(keyAlgorithm(signingAlgorithm));
        return keyFactory.generatePrivate(keySpec);
    }

    /**
     * Maps a JWT signing algorithm (e.g. RS256, ES256) to the corresponding Java key algorithm (RSA, EC).
     */
    static String keyAlgorithm(String signingAlgorithm) {
        if (signingAlgorithm.equalsIgnoreCase(TOKEN_SIGNING_ALGORITHM_RS256)) return "RSA";
        if (signingAlgorithm.equalsIgnoreCase(TOKEN_SIGNING_ALGORITHM_ES256)) return "EC";
        throw new IllegalArgumentException("Unsupported signing algorithm: " + signingAlgorithm);
    }

    public static Signature getSignature(String algorithm) throws GeneralSecurityException {
        if (algorithm.equalsIgnoreCase(TOKEN_SIGNING_ALGORITHM_RS256)) {
            return Signature.getInstance("SHA256withRSA");
        } else if (algorithm.equalsIgnoreCase(TOKEN_SIGNING_ALGORITHM_ES256)) {
            // Use P1363 format which produces raw R||S concatenation as required by JWS (RFC 7515).
            // Java's default SHA256withECDSA uses DER encoding which is not compatible with JWT signatures.
            return Signature.getInstance("SHA256withECDSAinP1363Format");
        } else {
            throw new NoSuchAlgorithmException(String.format("Unsupported signing algorithm: %s", algorithm));
        }
    }

    public static String sign(String algorithm, PrivateKey privateKey, String contentToSign) throws GeneralSecurityException {
        Signature signature = getSignature(algorithm);
        signature.initSign(privateKey);
        signature.update(contentToSign.getBytes(StandardCharsets.UTF_8));
        byte[] signedContent = signature.sign();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(signedContent);
    }

    public static Optional<StaticAssertionJwtTemplate> staticAssertionJwtTemplate(ConfigurationUtils cu) {
        if (cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD) ||
            cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS) ||
            cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB)) {
            Map<String, Object> staticClaimsPayload = new HashMap<>();

            if (cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD))
                staticClaimsPayload.put("aud", cu.validateString(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD));

            if (cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS))
                staticClaimsPayload.put("iss", cu.validateString(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS));

            if (cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB))
                staticClaimsPayload.put("sub", cu.validateString(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB));

            Map<String, Object> header = Map.of();
            return Optional.of(new StaticAssertionJwtTemplate(header, staticClaimsPayload));
        } else {
            return Optional.empty();
        }
    }

    public static Optional<FileAssertionJwtTemplate> fileAssertionJwtTemplate(ConfigurationUtils cu) {
        if (cu.containsKey(SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE)) {
            File assertionTemplateFile = cu.validateFile(SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE);
            return Optional.of(new FileAssertionJwtTemplate(assertionTemplateFile));
        } else {
            return Optional.empty();
        }
    }

    public static DynamicAssertionJwtTemplate dynamicAssertionJwtTemplate(ConfigurationUtils cu, Time time) {
        String algorithm = cu.validateString(SASL_OAUTHBEARER_ASSERTION_ALGORITHM);
        int expSeconds = cu.validateInteger(SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS, true);
        int nbfSeconds = cu.validateInteger(SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS, true);
        boolean includeJti = cu.validateBoolean(SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE, true);
        return new DynamicAssertionJwtTemplate(time, algorithm, expSeconds, nbfSeconds, includeJti);
    }

    public static LayeredAssertionJwtTemplate layeredAssertionJwtTemplate(ConfigurationUtils cu, Time time) {
        List<AssertionJwtTemplate> templates = new ArrayList<>();
        staticAssertionJwtTemplate(cu).ifPresent(templates::add);
        fileAssertionJwtTemplate(cu).ifPresent(templates::add);
        templates.add(dynamicAssertionJwtTemplate(cu, time));
        return new LayeredAssertionJwtTemplate(templates);
    }
}
