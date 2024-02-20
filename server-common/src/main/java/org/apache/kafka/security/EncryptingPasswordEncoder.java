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
package org.apache.kafka.security;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.server.util.Csv;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Password encoder and decoder implementation. Encoded passwords are persisted as a CSV map
 * containing the encoded password in base64 and along with the properties used for encryption.
 */
public class EncryptingPasswordEncoder implements PasswordEncoder {

    private final SecureRandom secureRandom = new SecureRandom();

    private final Password secret;
    private final String keyFactoryAlgorithm;
    private final String cipherAlgorithm;
    private final int keyLength;
    private final int iterations;
    private final CipherParamsEncoder cipherParamsEncoder;


    /**
     * @param secret The secret used for encoding and decoding
     * @param keyFactoryAlgorithm  Key factory algorithm if configured. By default, PBKDF2WithHmacSHA512 is
     *                             used if available, PBKDF2WithHmacSHA1 otherwise.
     * @param cipherAlgorithm Cipher algorithm used for encoding.
     * @param keyLength Key length used for encoding. This should be valid for the specified algorithms.
     * @param iterations Iteration count used for encoding.
     * The provided `keyFactoryAlgorithm`, `cipherAlgorithm`, `keyLength` and `iterations` are used for encoding passwords.
     * The values used for encoding are stored along with the encoded password and the stored values are used for decoding.
     */
    public EncryptingPasswordEncoder(
            Password secret,
            String keyFactoryAlgorithm,
            String cipherAlgorithm,
            int keyLength,
            int iterations) {
        this.secret = secret;
        this.keyFactoryAlgorithm = keyFactoryAlgorithm;
        this.cipherAlgorithm = cipherAlgorithm;
        this.keyLength = keyLength;
        this.iterations = iterations;
        this.cipherParamsEncoder = cipherParamsInstance(cipherAlgorithm);
    }

    @Override
    public String encode(Password password) throws GeneralSecurityException {
        byte[] salt = new byte[256];
        secureRandom.nextBytes(salt);
        Cipher cipher = Cipher.getInstance(cipherAlgorithm);
        SecretKeyFactory keyFactory = secretKeyFactory(keyFactoryAlgorithm);
        SecretKeySpec keySpec = secretKeySpec(keyFactory, cipherAlgorithm, keyLength, salt, iterations);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec);
        byte[] encryptedPassword = cipher.doFinal(password.value().getBytes(StandardCharsets.UTF_8));
        Map<String, String> encryptedMap = new HashMap<>();
        encryptedMap.put(PasswordEncoder.KEY_FACTORY_ALGORITHM, keyFactory.getAlgorithm());
        encryptedMap.put(PasswordEncoder.CIPHER_ALGORITHM, cipherAlgorithm);
        encryptedMap.put(PasswordEncoder.KEY_LENGTH, String.valueOf(keyLength));
        encryptedMap.put(PasswordEncoder.SALT, PasswordEncoder.base64Encode(salt));
        encryptedMap.put(PasswordEncoder.ITERATIONS, String.valueOf(iterations));
        encryptedMap.put(PasswordEncoder.ENCRYPTED_PASSWORD, PasswordEncoder.base64Encode(encryptedPassword));
        encryptedMap.put(PasswordEncoder.PASSWORD_LENGTH, String.valueOf(password.value().length()));
        encryptedMap.putAll(cipherParamsEncoder.toMap(cipher.getParameters()));

        return encryptedMap.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .collect(Collectors.joining(","));
    }

    @Override
    public Password decode(String encodedPassword) throws GeneralSecurityException {
        Map<String, String> params = Csv.parseCsvMap(encodedPassword);
        String keyFactoryAlg = params.get(PasswordEncoder.KEY_FACTORY_ALGORITHM);
        String cipherAlg = params.get(PasswordEncoder.CIPHER_ALGORITHM);
        int keyLength = Integer.parseInt(params.get(PasswordEncoder.KEY_LENGTH));
        byte[] salt = PasswordEncoder.base64Decode(params.get(PasswordEncoder.SALT));
        int iterations = Integer.parseInt(params.get(PasswordEncoder.ITERATIONS));
        byte[] encryptedPassword = PasswordEncoder.base64Decode(params.get(PasswordEncoder.ENCRYPTED_PASSWORD));
        int passwordLengthProp = Integer.parseInt(params.get(PasswordEncoder.PASSWORD_LENGTH));
        Cipher cipher = Cipher.getInstance(cipherAlg);
        SecretKeyFactory keyFactory = secretKeyFactory(keyFactoryAlg);
        SecretKeySpec keySpec = secretKeySpec(keyFactory, cipherAlg, keyLength, salt, iterations);
        cipher.init(Cipher.DECRYPT_MODE, keySpec, cipherParamsEncoder.toParameterSpec(params));
        try {
            byte[] decrypted = cipher.doFinal(encryptedPassword);
            String password = new String(decrypted, StandardCharsets.UTF_8);
            if (password.length() != passwordLengthProp) // Sanity check
                throw new ConfigException("Password could not be decoded, sanity check of length failed");
            return new Password(password);
        } catch (Exception e) {
            throw new ConfigException("Password could not be decoded", e);
        }
    }

    private SecretKeyFactory secretKeyFactory(String keyFactoryAlg) throws NoSuchAlgorithmException {
        if (keyFactoryAlg != null) {
            return SecretKeyFactory.getInstance(keyFactoryAlg);
        } else {
            try {
                return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
            } catch (NoSuchAlgorithmException nsae) {
                return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            }
        }
    }

    private SecretKeySpec secretKeySpec(SecretKeyFactory keyFactory,
                                        String cipherAlg,
                                        int keyLength,
                                        byte[] salt,
                                        int iterations) throws InvalidKeySpecException {
        PBEKeySpec keySpec = new PBEKeySpec(secret.value().toCharArray(), salt, iterations, keyLength);
        String algorithm = (cipherAlg.indexOf('/') > 0) ? cipherAlg.substring(0, cipherAlg.indexOf('/')) : cipherAlg;
        return new SecretKeySpec(keyFactory.generateSecret(keySpec).getEncoded(), algorithm);
    }

    private CipherParamsEncoder cipherParamsInstance(String cipherAlgorithm) {
        if (cipherAlgorithm.startsWith("AES/GCM/")) {
            return new GcmParamsEncoder();
        } else {
            return new IvParamsEncoder();
        }
    }
}
