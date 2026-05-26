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
package org.apache.kafka.connect.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.Objects;
import java.util.Set;


/**
 * Security note: While it seems it possible to build a deserialization gadget to obtain RCE via
 * FileOffsetBackingStore, it requires having write permissions on the filesystem of the Connect worker.
 * For that reason the Apache Kafka project does not consider this a security issue.
 */
public class SafeObjectInputStream extends ObjectInputStream {

    /**
     * The exact class descriptors produced when deserializing {@code HashMap<byte[], byte[]>},
     * the format written by {@link org.apache.kafka.connect.storage.FileOffsetBackingStore}.
     * Verified by inspection: only {@code java.util.HashMap} and {@code [B} (byte[]) appear.
     * Allowing any additional type would widen the attack surface without justification.
     */
    public static final Set<String> BASE_TYPES = Set.of(
            "java.util.HashMap",
            "[B"   // JVM descriptor for byte[]
    );

    private final Set<String> allowedClasses;

    /**
     * Uses {@link #BASE_TYPES} as the allowlist. Suitable for {@code FileOffsetBackingStore}.
     */
    public SafeObjectInputStream(InputStream in) throws IOException {
        this(in, BASE_TYPES);
    }

    /**
     * Permits only the classes in {@code allowedClasses}. Use when the stream contains
     * types beyond {@link #BASE_TYPES}; the caller must enumerate every expected type.
     */
    public SafeObjectInputStream(InputStream in, Set<String> allowedClasses) throws IOException {
        super(in);
        this.allowedClasses = Objects.requireNonNull(allowedClasses, "allowedClasses");
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        String name = desc.getName();
        if (!allowedClasses.contains(name)) {
            throw new InvalidClassException(name,
                    "Rejected by deserialization allowlist. If this class is legitimately " +
                    "required, pass an explicit allowedClasses set to " +
                    "SafeObjectInputStream(InputStream, Set).");
        }
        return super.resolveClass(desc);
    }
}
