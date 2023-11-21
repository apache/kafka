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

import org.apache.kafka.common.security.scram.ScramCache;

import java.util.concurrent.ConcurrentHashMap;

public class CredentialCache {

    private final ConcurrentHashMap<String, Cache> cacheMap = new ConcurrentHashMap<>();

    public Cache createCache(String mechanism) {
        Cache credentials = new Cache();
        Cache oldCredentials = cacheMap.putIfAbsent(mechanism, credentials);
        return oldCredentials == null ? credentials : oldCredentials;
    }

    public ScramCache createScramCache(String mechanism) {
        if (scramCache(mechanism) != null)
            return scramCache(mechanism);
        ScramCache credentials = new ScramCache();
        cacheMap.putIfAbsent(mechanism, credentials);
        return credentials;
    }

    public Cache cache(String mechanism) {
        return cacheMap.get(mechanism);
    }

    public ScramCache scramCache(String mechanism) {
        Cache cache = cacheMap.get(mechanism);
        if (cache == null)
            return null;
        if (cache instanceof ScramCache)
            return (ScramCache) cache;
        else
            throw new IllegalArgumentException("Expected cache of ScramCredential, found " + cache.getClass());
    }

    public static class Cache {
        private final ConcurrentHashMap<String, Object> credentials;

        public Cache() {
            this.credentials = new ConcurrentHashMap<>();
        }

        public Object get(String username) {
            return credentials.get(username);
        }

        public Object put(String username, Object credential) {
            return credentials.put(username, credential);
        }

        public Object remove(String username) {
            return credentials.remove(username);
        }
    }
}
