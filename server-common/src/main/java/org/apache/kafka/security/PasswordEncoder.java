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

import org.apache.kafka.common.config.types.Password;

import java.security.GeneralSecurityException;
import java.util.Base64;

public interface PasswordEncoder {

    String KEY_FACTORY_ALGORITHM = "keyFactoryAlgorithm";
    String CIPHER_ALGORITHM = "cipherAlgorithm";
    String INITIALIZATION_VECTOR = "initializationVector";
    String KEY_LENGTH = "keyLength";
    String SALT = "salt";
    String ITERATIONS = "iterations";
    String ENCRYPTED_PASSWORD = "encryptedPassword";
    String PASSWORD_LENGTH = "passwordLength";

    /**
     * A password encoder that does not modify the given password. This is used in KRaft mode only.
     */
    PasswordEncoder NOOP = new PasswordEncoder() {

        @Override
        public String encode(Password password) {
            return password.value();
        }

        @Override
        public Password decode(String encodedPassword) {
            return new Password(encodedPassword);
        }
    };

    static byte[] base64Decode(String encoded) {
        return Base64.getDecoder().decode(encoded);
    }

    static String base64Encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    static EncryptingPasswordEncoder encrypting(Password secret,
                   String keyFactoryAlgorithm,
                   String cipherAlgorithm,
                   int keyLength,
                   int iterations) {
        return new EncryptingPasswordEncoder(secret, keyFactoryAlgorithm, cipherAlgorithm, keyLength, iterations);
    }

    String encode(Password password) throws GeneralSecurityException;
    Password decode(String encodedPassword) throws GeneralSecurityException;
}
