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
package org.apache.kafka.connect.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * The type of {@link Converter} and {@link HeaderConverter}.
 */
public enum ConverterType {
    KEY,
    VALUE,
    HEADER;

    private static final Map<String, ConverterType> NAME_TO_TYPE;

    static {
        ConverterType[] types = ConverterType.values();
        Map<String, ConverterType> nameToType = new HashMap<>(types.length);
        for (ConverterType type : types) {
            nameToType.put(type.name, type);
        }
        NAME_TO_TYPE = Collections.unmodifiableMap(nameToType);
    }

    /**
     * Find the ConverterType with the given name, using a case-insensitive match.
     * @param name the name of the converter type; may be null
     * @return the matching converter type, or null if the supplied name is null or does not match the name of the known types
     */
    public static ConverterType withName(String name) {
        if (name == null) {
            return null;
        }
        return NAME_TO_TYPE.get(name.toLowerCase(Locale.getDefault()));
    }

    private final String name;

    ConverterType() {
        this.name = this.name().toLowerCase(Locale.ROOT);
    }

    public String getName() {
        return name;
    }
}
