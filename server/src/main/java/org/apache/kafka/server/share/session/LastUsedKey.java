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

package org.apache.kafka.server.share.session;

import java.util.Objects;

public class LastUsedKey implements Comparable<LastUsedKey> {
    private final ShareSessionKey key;
    private final long lastUsedMs;

    public LastUsedKey(ShareSessionKey key, long lastUsedMs) {
        this.key = key;
        this.lastUsedMs = lastUsedMs;
    }

    public ShareSessionKey key() {
        return key;
    }

    public long lastUsedMs() {
        return lastUsedMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, lastUsedMs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LastUsedKey other = (LastUsedKey) obj;
        return lastUsedMs == other.lastUsedMs && Objects.equals(key, other.key);
    }

    @Override
    public int compareTo(LastUsedKey other) {
        int res = Long.compare(lastUsedMs, other.lastUsedMs);
        if (res != 0)
            return res;
        return Integer.compare(key.hashCode(), other.key.hashCode());
    }
}
