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

public class PasswordEncoderConfigs {
    
    public static final String PASSWORD_ENCODER_SECRET_CONFIG = "password.encoder.secret";
    public static final String PASSWORD_ENCODER_SECRET_DOC = "The secret used for encoding dynamically configured passwords for this broker.";

    public static final String PASSWORD_ENCODER_OLD_SECRET_CONFIG = "password.encoder.old.secret";
    public static final String PASSWORD_ENCODER_OLD_SECRET_DOC = "The old secret that was used for encoding dynamically configured passwords. " +
            "This is required only when the secret is updated. If specified, all dynamically encoded passwords are " +
            "decoded using this old secret and re-encoded using " + PASSWORD_ENCODER_SECRET_CONFIG + " when broker starts up.";

    public static final String PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_CONFIG = "password.encoder.keyfactory.algorithm";
    public static final String PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_DOC = "The SecretKeyFactory algorithm used for encoding dynamically configured passwords. " +
            "Default is PBKDF2WithHmacSHA512 if available and PBKDF2WithHmacSHA1 otherwise.";

    public static final String PASSWORD_ENCODER_CIPHER_ALGORITHM_CONFIG = "password.encoder.cipher.algorithm";
    public static final String PASSWORD_ENCODER_CIPHER_ALGORITHM_DOC = "The Cipher algorithm used for encoding dynamically configured passwords.";
    public static final String PASSWORD_ENCODER_CIPHER_ALGORITHM_DEFAULT = "AES/CBC/PKCS5Padding";

    public static final String PASSWORD_ENCODER_KEY_LENGTH_CONFIG =  "password.encoder.key.length";
    public static final String PASSWORD_ENCODER_KEY_LENGTH_DOC =  "The key length used for encoding dynamically configured passwords.";
    public static final int PASSWORD_ENCODER_KEY_LENGTH_DEFAULT = 128;

    public static final String PASSWORD_ENCODER_ITERATIONS_CONFIG =  "password.encoder.iterations";
    public static final String PASSWORD_ENCODER_ITERATIONS_DOC =  "The iteration count used for encoding dynamically configured passwords.";
    public static final int PASSWORD_ENCODER_ITERATIONS_DEFAULT = 4096;
}
