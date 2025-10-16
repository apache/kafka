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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

public enum ShareAcquireMode {
    BATCH_OPTIMIZED("batch_optimized", (byte) 0),
    RECORD_LIMIT("record_limit", (byte) 1);
    
    public final String name;

    public final byte id;
    
    ShareAcquireMode(final String name, byte id) {
        this.name = name;
        this.id = id;
    }

    /**
     * Case-insensitive acquire mode lookup by string name.
     */
    public static ShareAcquireMode of(final String name) {
        return ShareAcquireMode.valueOf(name.toLowerCase(Locale.ROOT));
    }

    public byte id() {
        return id;
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ROOT);
    }

    public static class Validator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            String acquireMode = (String) value;
            try {
                of(acquireMode);
            } catch (Exception e) {
                throw new ConfigException(name, value, "Invalid value `" + acquireMode + "` for configuration " +
                    name + ". The value must either be 'batch_optimized' or 'record_limit'.");
            }
        }

        @Override
        public String toString() {
            String values = Arrays.stream(ShareAcquireMode.values())
                .map(ShareAcquireMode::toString).collect(Collectors.joining(", "));
            return "[" + values + "]";
        }
    }
}
