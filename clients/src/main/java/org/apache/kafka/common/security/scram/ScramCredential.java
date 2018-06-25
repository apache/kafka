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
package org.apache.kafka.common.security.scram;

/**
 * SCRAM credential class that encapsulates the credential data persisted for each user that is
 * accessible to the server. See <a href="https://tools.ietf.org/html/rfc5802#section-5">RFC rfc5802</a>
 * for details.
 */
public class ScramCredential {

    private final byte[] salt;
    private final byte[] serverKey;
    private final byte[] storedKey;
    private final int iterations;

    /**
     * Constructs a new credential.
     */
    public ScramCredential(byte[] salt, byte[] storedKey, byte[] serverKey, int iterations) {
        this.salt = salt;
        this.serverKey = serverKey;
        this.storedKey = storedKey;
        this.iterations = iterations;
    }

    /**
     * Returns the salt used to process this credential using the SCRAM algorithm.
     */
    public byte[] salt() {
        return salt;
    }

    /**
     * Server key computed from the client password using the SCRAM algorithm.
     */
    public byte[] serverKey() {
        return serverKey;
    }

    /**
     * Stored key computed from the client password using the SCRAM algorithm.
     */
    public byte[] storedKey() {
        return storedKey;
    }

    /**
     * Number of iterations used to process this credential using the SCRAM algorithm.
     */
    public int iterations() {
        return iterations;
    }
}