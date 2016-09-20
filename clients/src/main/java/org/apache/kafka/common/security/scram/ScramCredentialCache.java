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

package org.apache.kafka.common.security.scram;

import java.util.Collection;
import java.util.EnumMap;
import java.util.concurrent.ConcurrentHashMap;

public class ScramCredentialCache {

    private final EnumMap<ScramMechanism, Cache> cacheMap = new EnumMap<>(ScramMechanism.class);

    public ScramCredentialCache(Collection<String> enabledMechanisms) {
        for (String mechanism : enabledMechanisms) {
            ScramMechanism scramMechanism = ScramMechanism.forMechanismName(mechanism);
            if (scramMechanism != null)
                cacheMap.put(scramMechanism, new Cache());
        }
    }

    public Cache cache(ScramMechanism mechanism) {
        return cacheMap.get(mechanism);
    }

    public class Cache {
        private final ConcurrentHashMap<String, ScramCredential> credentials;

        public Cache() {
            this.credentials = new ConcurrentHashMap<>();
        }

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