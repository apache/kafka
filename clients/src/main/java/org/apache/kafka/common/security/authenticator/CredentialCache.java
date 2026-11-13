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
package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class CredentialCache {

    private final ConcurrentHashMap<String, Cache> cacheMap = new ConcurrentHashMap<>();

    /**
     * Creates a cache for each SCRAM mechanism in {@code mechanisms}. Other mechanisms are ignored.
     */
    public CredentialCache(Collection<String> mechanisms) {
        for (String mechanism : ScramMechanism.mechanismNames()) {
            if (mechanisms.contains(mechanism))
                cacheMap.put(mechanism, new Cache());
        }
    }

    public Cache cache(String mechanism) {
        return cacheMap.get(mechanism);
    }

    public static class Cache {
        private final ConcurrentHashMap<String, ScramCredential> credentials = new ConcurrentHashMap<>();

        public ScramCredential get(String username) {
            return credentials.get(username);
        }

        public ScramCredential put(String username, ScramCredential credential) {
            return credentials.put(username, credential);
        }

        public ScramCredential remove(String username) {
            return credentials.remove(username);
        }
    }
}
