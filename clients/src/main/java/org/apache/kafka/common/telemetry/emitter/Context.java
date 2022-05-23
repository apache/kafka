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
package org.apache.kafka.common.telemetry.emitter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.kafka.common.utils.Utils;

public class Context {

    public static final String APPLICATION_ID = "application_id";

    public static final String CLIENT_RACK = "client_rack";

    public static final String GROUP_ID = "group_id";

    public static final String GROUP_INSTANCE_ID = "group_instance_id";

    public static final String GROUP_MEMBER_ID = "group_member_id";

    public static final String TRANSACTIONAL_ID = "transactional_id";

    private final Map<String, String> tags;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public Context() {
        this.tags = new HashMap<>();
    }

    public Context(Map<String, String> tags) {
        this.tags = new HashMap<>(tags);
    }

    public String put(String key, String value) {
        try {
            lock.writeLock().lock();

            if (Utils.isBlank(value)) {
                return remove(key);
            } else {
                return tags.put(key, value);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String remove(String key) {
        try {
            lock.writeLock().lock();
            return tags.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String get(String key) {
        try {
            lock.readLock().lock();
            return tags.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Map<String, String> tags() {
        try {
            lock.readLock().lock();
            return Collections.unmodifiableMap(tags);
        } finally {
            lock.readLock().unlock();
        }
    }

}
