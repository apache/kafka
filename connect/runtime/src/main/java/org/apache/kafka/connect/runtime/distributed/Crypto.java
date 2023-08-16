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
 * Implementations of this class should be thread-safe.
 */
public interface Crypto {

    /**
     * Use the system implementation for cryptography calls.
     */
    Crypto SYSTEM = new SystemCrypto();

    /**
     * Returns a {@code Mac} object that implements the
     * specified MAC algorithm. See {@link Mac#getInstance(String)}.
     *
     * @param algorithm the standard name of the requested MAC algorithm.
     * @return the new {@code Mac} object
     * @throws NoSuchAlgorithmException if no {@code Provider} supports a
     *         {@code MacSpi} implementation for the specified algorithm
     */
    Mac mac(String algorithm) throws NoSuchAlgorithmException;

    /**
     * Returns a {@code KeyGenerator} object that generates secret keys
     * for the specified algorithm. See {@link KeyGenerator#getInstance(String)}.
     *
     * @param algorithm the standard name of the requested key algorithm.
     * @return the new {@code KeyGenerator} object
     * @throws NoSuchAlgorithmException if no {@code Provider} supports a
     *         {@code KeyGeneratorSpi} implementation for the
     *         specified algorithm
     */
    KeyGenerator keyGenerator(String algorithm) throws NoSuchAlgorithmException;

    class SystemCrypto implements Crypto {
        @Override
        public Mac mac(String algorithm) throws NoSuchAlgorithmException {
            return Mac.getInstance(algorithm);
        }

        @Override
        public KeyGenerator keyGenerator(String algorithm) throws NoSuchAlgorithmException {
            return KeyGenerator.getInstance(algorithm);
        }
    }

}
