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

import javax.crypto.SecretKeyFactory;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.server.util.Csv;
import org.junit.jupiter.api.Test;

import java.security.GeneralSecurityException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PasswordEncoderTest {

    @Test
    public void testEncodeDecode() throws GeneralSecurityException {
        PasswordEncoder encoder = PasswordEncoder.encrypting(new Password("password-encoder-secret"),
                null,
                PasswordEncoderConfigs.DEFAULT_CIPHER_ALGORITHM,
                PasswordEncoderConfigs.DEFAULT_KEY_LENGTH,
                PasswordEncoderConfigs.DEFAULT_ITERATIONS);
        String password = "test-password";
        String encoded = encoder.encode(new Password(password));
        Map<String, String> encodedMap = Csv.parseCsvMap(encoded);
        assertEquals("4096", encodedMap.get(PasswordEncoder.ITERATIONS));
        assertEquals("128", encodedMap.get(PasswordEncoder.KEY_LENGTH));
        String defaultKeyFactoryAlgorithm;
        try {
            SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
            defaultKeyFactoryAlgorithm = "PBKDF2WithHmacSHA512";

        } catch (Exception e) {
            defaultKeyFactoryAlgorithm = "PBKDF2WithHmacSHA1";
        }
        assertEquals(defaultKeyFactoryAlgorithm, encodedMap.get(PasswordEncoder.KEY_FACTORY_ALGORITHM));
        assertEquals("AES/CBC/PKCS5Padding", encodedMap.get(PasswordEncoder.CIPHER_ALGORITHM));
        verifyEncodedPassword(encoder, password, encoded);
    }

    @Test
    public void testEncoderConfigChange() throws GeneralSecurityException {
        PasswordEncoder encoder = PasswordEncoder.encrypting(new Password("password-encoder-secret"),
                "PBKDF2WithHmacSHA1",
                "DES/CBC/PKCS5Padding",
                64,
                1024);
        String password = "test-password";
        String encoded = encoder.encode(new Password(password));
        Map<String, String> encodedMap = Csv.parseCsvMap(encoded);
        assertEquals("1024", encodedMap.get(PasswordEncoder.ITERATIONS));
        assertEquals("64", encodedMap.get(PasswordEncoder.KEY_LENGTH));
        assertEquals("PBKDF2WithHmacSHA1", encodedMap.get(PasswordEncoder.KEY_FACTORY_ALGORITHM));
        assertEquals("DES/CBC/PKCS5Padding", encodedMap.get(PasswordEncoder.CIPHER_ALGORITHM));

        // Test that decoding works even if PasswordEncoder algorithm, iterations etc. are altered
        PasswordEncoder decoder = PasswordEncoder.encrypting(new Password("password-encoder-secret"),
                "PBKDF2WithHmacSHA1",
                "AES/CBC/PKCS5Padding",
                128,
                2048);
        assertEquals(password, decoder.decode(encoded).value());

        // Test that decoding fails if secret is altered
        PasswordEncoder decoder2 = PasswordEncoder.encrypting(new Password("secret-2"),
                "PBKDF2WithHmacSHA1",
                "AES/CBC/PKCS5Padding",
                128,
                1024);
        assertThrows(ConfigException.class, () -> decoder2.decode(encoded));
    }

    @Test
    public void  testEncodeDecodeAlgorithms() throws GeneralSecurityException {
        verifyEncodeDecode(null, "DES/CBC/PKCS5Padding", 64);
        verifyEncodeDecode(null, "DESede/CBC/PKCS5Padding", 192);
        verifyEncodeDecode(null, "AES/CBC/PKCS5Padding", 128);
        verifyEncodeDecode(null, "AES/CFB/PKCS5Padding", 128);
        verifyEncodeDecode(null, "AES/OFB/PKCS5Padding", 128);
        verifyEncodeDecode("PBKDF2WithHmacSHA1", PasswordEncoderConfigs.DEFAULT_CIPHER_ALGORITHM, 128);
        verifyEncodeDecode(null, "AES/GCM/NoPadding", 128);
        verifyEncodeDecode("PBKDF2WithHmacSHA256", PasswordEncoderConfigs.DEFAULT_CIPHER_ALGORITHM, 128);
        verifyEncodeDecode("PBKDF2WithHmacSHA512", PasswordEncoderConfigs.DEFAULT_CIPHER_ALGORITHM, 128);
    }

    private void verifyEncodeDecode(String keyFactoryAlg, String cipherAlg, int keyLength) throws GeneralSecurityException {
        PasswordEncoder encoder = PasswordEncoder.encrypting(new Password("password-encoder-secret"),
                keyFactoryAlg,
                cipherAlg,
                keyLength,
                PasswordEncoderConfigs.DEFAULT_ITERATIONS);
        String password = "test-password";
        String encoded = encoder.encode(new Password(password));
        verifyEncodedPassword(encoder, password, encoded);
    }

    private void verifyEncodedPassword(PasswordEncoder encoder, String password, String encoded) throws GeneralSecurityException {
        Map<String, String> encodedMap = Csv.parseCsvMap(encoded);
        assertEquals(String.valueOf(password.length()), encodedMap.get(PasswordEncoder.PASSWORD_LENGTH));
        assertNotNull(PasswordEncoder.base64Decode(encodedMap.get("salt")), "Invalid salt");
        assertNotNull(PasswordEncoder.base64Decode(encodedMap.get(PasswordEncoder.INITIALIZATION_VECTOR)), "Invalid encoding parameters");
        assertNotNull(PasswordEncoder.base64Decode(encodedMap.get(PasswordEncoder.ENCRYPTED_PASSWORD)), "Invalid encoded password");
        assertEquals(password, encoder.decode(encoded).value());
    }
}
