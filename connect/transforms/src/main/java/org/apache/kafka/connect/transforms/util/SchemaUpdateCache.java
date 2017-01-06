/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.connect.transforms.util;

import org.apache.kafka.connect.data.Schema;

/**
 * Single-entry thread-local cache. It uses reference equality to avoid expensive comparisons.
 */
public class SchemaUpdateCache {

    private static final class Entry {
        final Schema base;
        final Schema updated;

        private Entry(Schema base, Schema updated) {
            this.base = base;
            this.updated = updated;
        }
    }

    private ThreadLocal<Entry> cache;

    public void init() {
        cache = new ThreadLocal<>();
    }

    public Schema get(Schema base) {
        final Entry entry = cache.get();
        return entry != null && entry.base == base ? entry.updated : null;
    }

    public void put(Schema base, Schema updated) {
        cache.set(new Entry(base, updated));
    }

    public void close() {
        cache = null;
    }

}
