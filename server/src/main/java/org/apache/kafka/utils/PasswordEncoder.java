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
package org.apache.kafka.utils;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.AlgorithmParameters;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public interface PasswordEncoder {
    String KEY_FACTORY_ALGORITHM_PROP = "keyFactoryAlgorithm";
    String CIPHER_ALGORITHM_PROP = "cipherAlgorithm";
    String INITIALIZATION_VECTOR_PROP = "initializationVector";
    String KEY_LENGTH_PROP = "keyLength";
    String SALT_PROP = "salt";
    String ITERATIONS_PROP = "iterations";
    String ENCRYPTED_PASSWORD_PROP = "encryptedPassword";
    String PASSWORD_LENGTH_PROP = "passwordLength";

    static EncryptingPasswordEncoder encrypting(
            Password secret,
            String keyFactoryAlgorithm,
            String cipherAlgorithm,
            int keyLength,
            int iterations) {
        return new EncryptingPasswordEncoder(secret, keyFactoryAlgorithm, cipherAlgorithm, keyLength, iterations);
    }

    String encode(Password password) throws Exception;

    Password decode(String encodedPassword) throws Exception;

    static byte[] base64Decode(String encoded) {
        return Base64.getDecoder().decode(encoded);
    }

    /**
     * A password encoder that does not modify the given password. This is used in KRaft mode only.
     */
    PasswordEncoder NO_OP_PASSWORD_ENCODER = new PasswordEncoder() {
        @Override public String encode(Password password) throws Exception {
            return password.value();
        }

        @Override
        public Password decode(String encodedPassword) throws Exception {
            return new Password(encodedPassword);
        }
    };
    class EncryptingPasswordEncoder implements PasswordEncoder {
        private final Password secret;
        private final String keyFactoryAlgorithm;
        private final String cipherAlgorithm;
        private final int keyLength;
        private final int iterations;

        private final SecureRandom secureRandom = new SecureRandom();
        private final CipherParamsEncoder cipherParamsEncoder;

        private static final Logger log = LoggerFactory.getLogger(EncryptingPasswordEncoder.class);

        /**
         * Password encoder and decoder implementation. Encoded passwords are persisted as a CSV map
         * containing the encoded password in base64 and along with the properties used for encryption.
         *
         * @param secret The secret used for encoding and decoding
         * @param keyFactoryAlgorithm  Key factory algorithm if configured. By default, PBKDF2WithHmacSHA512 is
         *                             used if available, PBKDF2WithHmacSHA1 otherwise.
         * @param cipherAlgorithm Cipher algorithm used for encoding.
         * @param keyLength Key length used for encoding. This should be valid for the specified algorithms.
         * @param iterations Iteration count used for encoding.
         *
         * The provided `keyFactoryAlgorithm`, `cipherAlgorithm`, `keyLength` and `iterations` are used for encoding passwords.
         * The values used for encoding are stored along with the encoded password and the stored values are used for decoding.
         *
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
        public String encode(Password password) throws Exception {
            byte[] salt = new byte[256];
            secureRandom.nextBytes(salt);

            Cipher cipher = Cipher.getInstance(cipherAlgorithm);
            SecretKeyFactory keyFactory = secretKeyFactory(keyFactoryAlgorithm);
            SecretKeySpec keySpec = secretKeySpec(keyFactory, cipherAlgorithm, keyLength, salt, iterations);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec);

            byte[] encryptedPassword = cipher.doFinal(password.value().getBytes(StandardCharsets.UTF_8));
            Map<String, String> encryptedMap = new HashMap<String, String>() {{
                    put(KEY_FACTORY_ALGORITHM_PROP, keyFactory.getAlgorithm());
                    put(CIPHER_ALGORITHM_PROP, cipherAlgorithm);
                    put(KEY_LENGTH_PROP, Integer.toString(keyLength));
                    put(SALT_PROP, base64Encode(salt));
                    put(ITERATIONS_PROP, Integer.toString(iterations));
                    put(ENCRYPTED_PASSWORD_PROP, base64Encode(encryptedPassword));
                    put(PASSWORD_LENGTH_PROP, Integer.toString(password.value().length()));
                }};

            encryptedMap.putAll(cipherParamsEncoder.toMap(cipher.getParameters()));

            return encryptedMap.entrySet()
                    .stream()
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(","));
        }

        @Override
        public Password decode(String encodedPassword) throws Exception {
            Map<String, String> params = CoreUtils.parseCsvMap(encodedPassword);
            String keyFactoryAlg = params.get(PasswordEncoder.KEY_FACTORY_ALGORITHM_PROP);
            String cipherAlg = params.get(PasswordEncoder.CIPHER_ALGORITHM_PROP);
            int keyLength = Integer.parseInt(params.get(PasswordEncoder.KEY_LENGTH_PROP));
            byte[] salt = base64Decode(params.get(PasswordEncoder.SALT_PROP));
            int iterations = Integer.parseInt(params.get(PasswordEncoder.ITERATIONS_PROP));
            byte[] encryptedPassword = base64Decode(params.get(PasswordEncoder.ENCRYPTED_PASSWORD_PROP));
            int passwordLengthProp = Integer.parseInt(params.get(PasswordEncoder.PASSWORD_LENGTH_PROP));

            Cipher cipher = Cipher.getInstance(cipherAlg);
            SecretKeyFactory keyFactory = secretKeyFactory(keyFactoryAlg);
            SecretKeySpec keySpec = secretKeySpec(keyFactory, cipherAlg, keyLength, salt, iterations);
            cipher.init(Cipher.DECRYPT_MODE, keySpec, cipherParamsEncoder.toParameterSpec(params));
            String password;
            try {
                byte[] decrypted = cipher.doFinal(encryptedPassword);
                password = new String(decrypted, StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new ConfigException("Password could not be decoded", e);
            }
            if (password.length() != passwordLengthProp) {
                throw new ConfigException("Password could not be decoded, sanity check of length failed");
            }

            return new Password(password);
        }

        private SecretKeyFactory secretKeyFactory(String keyFactoryAlg) throws NoSuchAlgorithmException {
            if (keyFactoryAlg == null) {
                try {
                    return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
                } catch (NoSuchAlgorithmException e) {
                    return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
                }
            }
            return SecretKeyFactory.getInstance(keyFactoryAlg);
        }

        private SecretKeySpec secretKeySpec(
                SecretKeyFactory keyFactory,
                String cipherAlg,
                int keyLength,
                byte[] salt,
                int iterations) throws InvalidKeySpecException {
            PBEKeySpec keySpec = new PBEKeySpec(secret.value().toCharArray(), salt, iterations, keyLength);
            String algorithm = cipherAlg.indexOf('/') > 0 ? cipherAlg.substring(0, cipherAlg.indexOf('/')) : cipherAlg;
            return new SecretKeySpec(keyFactory.generateSecret(keySpec).getEncoded(), algorithm);
        }

        static String base64Encode(byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }

        private CipherParamsEncoder cipherParamsInstance(String cipherAlgorithm) {
            Pattern aesPattern = Pattern.compile("AES/(.*)/.*");
            Matcher matcher = aesPattern.matcher(cipherAlgorithm);

            if (matcher.matches()) {
                String mode = matcher.group(1);
                if ("GCM".equals(mode)) {
                    return new GcmParamsEncoder();
                }
            }

            return new IvParamsEncoder();
        }
        interface CipherParamsEncoder {
            Map<String, String> toMap(AlgorithmParameters cipherParams) throws InvalidParameterSpecException;
            AlgorithmParameterSpec toParameterSpec(Map<String, String> paramMap);
        }

        static class IvParamsEncoder implements CipherParamsEncoder {
            @Override
            public Map<String, String> toMap(AlgorithmParameters cipherParams) throws InvalidParameterSpecException {
                if (cipherParams != null) {
                    IvParameterSpec ivSpec = cipherParams.getParameterSpec(IvParameterSpec.class);
                    return new HashMap<String, String>() {{
                            put(PasswordEncoder.INITIALIZATION_VECTOR_PROP, base64Encode(ivSpec.getIV()));
                        }};
                } else {
                    throw new IllegalStateException("Could not determine initialization vector for cipher");
                }
            }

            @Override
            public AlgorithmParameterSpec toParameterSpec(Map<String, String> paramMap) {
                return new IvParameterSpec(base64Decode(paramMap.get(PasswordEncoder.INITIALIZATION_VECTOR_PROP)));
            }
        }

        static class GcmParamsEncoder implements CipherParamsEncoder {
            @Override
            public Map<String, String> toMap(AlgorithmParameters cipherParams) throws InvalidParameterSpecException {
                if (cipherParams != null) {
                    GCMParameterSpec spec = cipherParams.getParameterSpec(GCMParameterSpec.class);
                    return new HashMap<String, String>() {{
                            put(PasswordEncoder.INITIALIZATION_VECTOR_PROP, base64Encode(spec.getIV()));
                            put("authenticationTagLength", Integer.toString(spec.getTLen()));
                        }};
                } else {
                    throw new IllegalStateException("Could not determine initialization vector for cipher");
                }
            }

            @Override
            public AlgorithmParameterSpec toParameterSpec(Map<String, String> paramMap) {
                return new GCMParameterSpec(
                        Integer.parseInt(paramMap.get("authenticationTagLength")),
                        base64Decode(paramMap.get(PasswordEncoder.INITIALIZATION_VECTOR_PROP))
                );
            }
        }
    }
}