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
package org.apache.kafka.connect.runtime.distributed;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import java.security.NoSuchAlgorithmException;

/**
 * An interface to allow the dependency injection of {@link Mac} and {@link KeyGenerator} instances for testing.
 *
 * Implementations of this class should be thread-safe.
 */
public interface CryptoLibrary {

    CryptoLibrary SYSTEM = new SystemLibrary();

    Mac getMacInstance(String algorithm) throws NoSuchAlgorithmException;

    KeyGenerator getKeyGeneratorInstance(String algorithm) throws NoSuchAlgorithmException;

    class SystemLibrary implements CryptoLibrary {
        @Override
        public Mac getMacInstance(String algorithm) throws NoSuchAlgorithmException {
            return Mac.getInstance(algorithm);
        }

        @Override
        public KeyGenerator getKeyGeneratorInstance(String algorithm) throws NoSuchAlgorithmException {
            return KeyGenerator.getInstance(algorithm);
        }
    };

}
