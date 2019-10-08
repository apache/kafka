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
package org.apache.kafka.connect.runtime;

import javax.crypto.SecretKey;
import java.util.Objects;

/**
 * A session key, which can be used to validate internal REST requests between workers.
 */
public class SessionKey {

    private final SecretKey key;
    private final long creationTimestamp;

    /**
     * Create a new session key with the given key value and creation timestamp
     * @param key the actual cryptographic key to use for request validation; may not be null
     * @param creationTimestamp the time at which the key was generated
     */
    public SessionKey(SecretKey key, long creationTimestamp) {
        this.key = Objects.requireNonNull(key, "Key may not be null");
        this.creationTimestamp = creationTimestamp;
    }

    /**
     * Get the cryptographic key to use for request validation.
     *
     * @return the cryptographic key; may not be null
     */
    public SecretKey key() {
        return key;
    }

    /**
     * Get the time at which the key was generated.
     *
     * @return the time at which the key was generated
     */
    public long creationTimestamp() {
        return creationTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SessionKey that = (SessionKey) o;
        return creationTimestamp == that.creationTimestamp
            && key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, creationTimestamp);
    }
}
