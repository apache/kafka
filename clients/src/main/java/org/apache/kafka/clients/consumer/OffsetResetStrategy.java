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

import java.util.Locale;
import java.util.Objects;

/**
 * @see ConsumerConfig#AUTO_OFFSET_RESET_CONFIG
 * @see ConsumerConfig#AUTO_OFFSET_RESET_DOC
 */
public enum OffsetResetStrategy {
    LATEST, EARLIEST, NONE;

    // Enums are singletons, this means that this conversion happens once the
    // first time a variant is actually used, and never again.
    private final String id = name().toLowerCase(Locale.ROOT);

    /**
     * Get the {@link OffsetResetStrategy} for the given {@code name}.
     *
     * @throws IllegalArgumentException if the given {@code name} does not match any {@link OffsetResetStrategy}.
     * @throws NullPointerException     if the given {@code name} is null.
     */
    public static OffsetResetStrategy forName(final String name) {
        Objects.requireNonNull(name, "name must not be null");

        for (final OffsetResetStrategy value : values()) {
            if (value.name().equalsIgnoreCase(name)) {
                return value;
            }
        }

        throw new IllegalArgumentException("Unknown offset reset strategy: " + name);
    }

    @Override
    public String toString() {
        return id;
    }
}
