/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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


import javax.crypto.SecretKeyFactory;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.utils.CoreUtils;
import org.apache.kafka.utils.PasswordEncoder;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PasswordEncoderTest {

    @Test
    public void testEncodeDecode() throws Exception {
        PasswordEncoder.EncryptingPasswordEncoder encoder = PasswordEncoder.encrypting(new Password("password-encoder-secret"),
                null,
                Defaults.PASSWORD_ENCODER_CIPHER_ALGORITHM,
                Defaults.PASSWORD_ENCODER_KEY_LENGTH,
                Defaults.PASSWORD_ENCODER_ITERATIONS);
        String password = "test-password";
        String encoded = encoder.encode(new Password(password));
        Map<String, String> encodedMap = CoreUtils.parseCsvMap(encoded);
        assertEquals("4096", encodedMap.get(PasswordEncoder.ITERATIONS_PROP));
        assertEquals("128", encodedMap.get(PasswordEncoder.KEY_LENGTH_PROP));
        String defaultKeyFactoryAlgorithm;
        try {
            SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
            defaultKeyFactoryAlgorithm = "PBKDF2WithHmacSHA512";
        } catch(Exception e) {
            defaultKeyFactoryAlgorithm = "PBKDF2WithHmacSHA1";
        }

        assertEquals(defaultKeyFactoryAlgorithm, encodedMap.get(PasswordEncoder.KEY_FACTORY_ALGORITHM_PROP));
        assertEquals("AES/CBC/PKCS5Padding", encodedMap.get(PasswordEncoder.CIPHER_ALGORITHM_PROP));

        verifyEncodedPassword(encoder, password, encoded);
    }

    @Test
    public void testEncoderConfigChange() throws Exception {
        PasswordEncoder.EncryptingPasswordEncoder encoder = PasswordEncoder.encrypting(new Password("password-encoder-secret"),
                "PBKDF2WithHmacSHA1",
                "DES/CBC/PKCS5Padding",
                64,
                1024);
        String password = "test-password";
        String encoded = encoder.encode(new Password(password));
        Map<String, String> encodedMap = CoreUtils.parseCsvMap(encoded)
        assertEquals("1024", encodedMap.get(PasswordEncoder.ITERATIONS_PROP));
        assertEquals("64", encodedMap.get(PasswordEncoder.KEY_LENGTH_PROP));
        assertEquals("PBKDF2WithHmacSHA1", encodedMap.get(PasswordEncoder.KEY_FACTORY_ALGORITHM_PROP));
        assertEquals("DES/CBC/PKCS5Padding", encodedMap.get(PasswordEncoder.CIPHER_ALGORITHM_PROP));

        // Test that decoding works even if PasswordEncoder algorithm, iterations etc. are altered
        PasswordEncoder.EncryptingPasswordEncoder decoder = PasswordEncoder.encrypting(new Password("password-encoder-secret"),
                "PBKDF2WithHmacSHA1",
                "AES/CBC/PKCS5Padding",
                128,
                2048);
        assertEquals(password, decoder.decode(encoded).value());

        // Test that decoding fails if secret is altered
        PasswordEncoder.EncryptingPasswordEncoder decoder2 = PasswordEncoder.encrypting(new Password("secret-2"),
                "PBKDF2WithHmacSHA1",
                "AES/CBC/PKCS5Padding",
                128,
                1024);
        try {
            decoder2.decode(encoded);
        } catch(ConfigException e) {
            // expected exception
        }
    }

    private void verifyEncodeDecode(String keyFactoryAlg, String cipherAlg, int keyLength) throws Exception {
        PasswordEncoder.EncryptingPasswordEncoder encoder = PasswordEncoder.encrypting(new Password("password-encoder-secret"),
                keyFactoryAlg,
                cipherAlg,
                keyLength,
                Defaults.PASSWORD_ENCODER_ITERATIONS);
        String password = "test-password";
        String encoded = encoder.encode(new Password(password));
        verifyEncodedPassword(encoder, password, encoded);
    }

    @Test
    public void testEncodeDecodeAlgorithms() throws Exception {
        verifyEncodeDecode(null, "DES/CBC/PKCS5Padding", 64);
        verifyEncodeDecode(null, "DESede/CBC/PKCS5Padding", 192);
        verifyEncodeDecode(null, "AES/CBC/PKCS5Padding", 128);
        verifyEncodeDecode(null, "AES/CFB/PKCS5Padding", 128);
        verifyEncodeDecode(null, "AES/OFB/PKCS5Padding", 128);
        verifyEncodeDecode("PBKDF2WithHmacSHA1", Defaults.PASSWORD_ENCODER_CIPHER_ALGORITHM, 128);
        verifyEncodeDecode(null, "AES/GCM/NoPadding", 128);
        verifyEncodeDecode("PBKDF2WithHmacSHA256", Defaults.PASSWORD_ENCODER_CIPHER_ALGORITHM, 128);
        verifyEncodeDecode("PBKDF2WithHmacSHA512", Defaults.PASSWORD_ENCODER_CIPHER_ALGORITHM, 128);
    }

    private void verifyEncodedPassword(PasswordEncoder encoder, String password, String encoded) throws Exception {
        Map<String, String> encodedMap = CoreUtils.parseCsvMap(encoded);
        assertEquals(Integer.toString(password.length()), encodedMap.get(PasswordEncoder.PASSWORD_LENGTH_PROP));
        assertNotNull(PasswordEncoder.base64Decode(encodedMap.get("salt")), "Invalid salt");
        assertNotNull(PasswordEncoder.base64Decode(encodedMap.get(PasswordEncoder.INITIALIZATION_VECTOR_PROP)), "Invalid encoding parameters");
        assertNotNull(PasswordEncoder.base64Decode(encodedMap.get(PasswordEncoder.ENCRYPTED_PASSWORD_PROP)), "Invalid encoded password");
        assertEquals(password, encoder.decode(encoded).value());
    }
}