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

import org.apache.kafka.connect.runtime.SessionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

/**
 * Owns the state of the inter-worker session key (the key itself and when it expires)
 * and decides when it needs to be rotated. The herder only coordinates: it generates
 * the key, writes it to the config topic and asks this class for the current key.
 */
class SessionKeyManager {

    private static final Logger log = LoggerFactory.getLogger(SessionKeyManager.class);

    private final long rotationIntervalMs;
    private SecretKey key;
    private long expiration = Long.MAX_VALUE;

    SessionKeyManager(long rotationIntervalMs) {
        this.rotationIntervalMs = rotationIntervalMs;
    }

    synchronized void reset() {
        key = null;
        expiration = Long.MAX_VALUE;
    }

    synchronized SecretKey key() {
        return key;
    }

    synchronized long expiration() {
        return expiration;
    }

    /** Called when a new session key is read from the config topic. */
    synchronized void onKeyUpdate(SessionKey sessionKey) {
        key = sessionKey.key();
        // Followers don't rotate keys, but may become leader later and need to know the expiration.
        if (rotationIntervalMs > 0) {
            expiration = sessionKey.creationTimestamp() + rotationIntervalMs;
        }
    }

    /** Called right before a new key is written, so the same key isn't rotated twice. */
    synchronized void markRotationInProgress() {
        expiration = Long.MAX_VALUE;
    }

    /**
     * @param snapshotKey the key present in the config snapshot, or null; on startup the snapshot
     *                    has the key but no listener callback has fired for it yet
     */
    boolean needsRotation(long now, SessionKey snapshotKey, boolean leader,
                          boolean validationEnabled, KeyGenerator keyGenerator) {
        SecretKey currentKey;
        long currentExpiration;
        synchronized (this) {
            if (key == null && snapshotKey != null) {
                key = snapshotKey.key();
                expiration = snapshotKey.creationTimestamp() + rotationIntervalMs;
            }
            currentKey = key;
            currentExpiration = expiration;
        }

        if (validationEnabled && leader) {
            if (currentKey == null) {
                log.debug("Internal request signing is enabled but no session key has been distributed yet. "
                    + "Distributing new key now.");
                return true;
            } else if (currentExpiration <= now) {
                log.debug("Existing key has expired. Distributing new key now.");
                return true;
            } else if (!currentKey.getAlgorithm().equals(keyGenerator.getAlgorithm())
                    || currentKey.getEncoded().length != keyGenerator.generateKey().getEncoded().length) {
                log.debug("Previously-distributed key uses different algorithm/key size "
                    + "than required by current worker configuration. Distributing new key now.");
                return true;
            }
        }
        return false;
    }
}
