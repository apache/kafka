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
package org.apache.kafka.common.config.internals;

import org.apache.kafka.common.config.AbstractConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Marks keys retrieved via `get` as used. This is needed because `Configurable.configure` takes a `Map` instead
 * of an `AbstractConfig` and we can't change that without breaking public API like `Partitioner`.
 */
public class RecordingMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = 8756361579758562482L;

    private final String prefix;
    private final boolean withIgnoreFallback;
    private final transient AbstractConfig config;

    public RecordingMap(AbstractConfig config) {
        this(config, "", false);
    }

    public RecordingMap(AbstractConfig config, String prefix, boolean withIgnoreFallback) {
        this.config = config;
        this.prefix = Objects.requireNonNull(prefix);
        this.withIgnoreFallback = withIgnoreFallback;
    }

    public RecordingMap(AbstractConfig config, Map<String, ? extends V> m) {
        this(config, m, "", false);
    }

    public RecordingMap(AbstractConfig config, Map<String, ? extends V> m, String prefix, boolean withIgnoreFallback) {
        super(m);
        this.config = config;
        this.prefix = Objects.requireNonNull(prefix);
        this.withIgnoreFallback = withIgnoreFallback;
    }

    public static <V> Map<String, V> copyAndPreserve(Map<String, V> map) {
        if (map instanceof RecordingMap) {
            RecordingMap<V> recordingMap = (RecordingMap<V>) map;
            return new RecordingMap<>(recordingMap.config, recordingMap, recordingMap.prefix, recordingMap.withIgnoreFallback);
        } else {
            return new HashMap<>(map);
        }
    }

    @Override
    public V get(Object key) {
        if (key instanceof String) {
            String stringKey = (String) key;
            String keyWithPrefix;
            if (prefix.isEmpty()) {
                keyWithPrefix = stringKey;
            } else {
                keyWithPrefix = prefix + stringKey;
            }
            record(keyWithPrefix);
            if (withIgnoreFallback)
                record(stringKey);
        }
        return super.get(key);
    }

    private void record(String key) {
        if (config != null)
            config.ignore(key);
    }

}
