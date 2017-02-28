/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.authenticator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CredentialCache {

    private final Map<String, Cache<? extends Object>> cacheMap = new HashMap<>();

    public <C> Cache<C> createCache(String mechanism, Class<C> credentialClass) {
        Cache<C> cache = new Cache<C>(credentialClass);
        cacheMap.put(mechanism, cache);
        return cache;
    }

    @SuppressWarnings("unchecked")
    public <C> Cache<C> cache(String mechanism, Class<C> credentialClass) {
        Cache<?> cache = cacheMap.get(mechanism);
        if (cache != null) {
            if (cache.credentialClass() != credentialClass)
                throw new IllegalArgumentException("Invalid credential class " + credentialClass + ", expected " + cache.credentialClass());
            return (Cache<C>) cache;
        } else
            return null;
    }

    public static class Cache<C> {
        private final Class<C> credentialClass;
        private final ConcurrentHashMap<String, C> credentials;

        public Cache(Class<C> credentialClass) {
            this.credentialClass = credentialClass;
            this.credentials = new ConcurrentHashMap<>();
        }

        public C get(String username) {
            return credentials.get(username);
        }

        public C put(String username, C credential) {
            return credentials.put(username, credential);
        }

        public C remove(String username) {
            return credentials.remove(username);
        }

        public Class<C> credentialClass() {
            return credentialClass;
        }
    }
}