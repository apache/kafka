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
    
    public static final String SECRET = "password.encoder.secret";
    public static final String OLD_SECRET = "password.encoder.old.secret";
    public static final String KEYFACTORY_ALGORITHM = "password.encoder.keyfactory.algorithm";
    public static final String CIPHER_ALGORITHM = "password.encoder.cipher.algorithm";
    public static final String DEFAULT_CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding";
    public static final String KEY_LENGTH =  "password.encoder.key.length";
    public static final int DEFAULT_KEY_LENGTH = 128;
    public static final String ITERATIONS =  "password.encoder.iterations";
    public static final int DEFAULT_ITERATIONS = 4096;
}
