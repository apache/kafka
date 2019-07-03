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
package org.apache.kafka.common.security.auth;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple immutable value object class holding customizable SASL extensions
 */
public class SaslExtensions {
    /**
     * An "empty" instance indicating no SASL extensions
     */
    public static final SaslExtensions NO_SASL_EXTENSIONS = new SaslExtensions(Collections.emptyMap());
    private final Map<String, String> extensionsMap;

    public SaslExtensions(Map<String, String> extensionsMap) {
        this.extensionsMap = Collections.unmodifiableMap(new HashMap<>(extensionsMap));
    }

    /**
     * Returns an <strong>immutable</strong> map of the extension names and their values
     */
    public Map<String, String> map() {
        return extensionsMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return extensionsMap.equals(((SaslExtensions) o).extensionsMap);
    }

    @Override
    public String toString() {
        return extensionsMap.toString();
    }

    @Override
    public int hashCode() {
        return extensionsMap.hashCode();
    }

}
