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

import org.apache.kafka.common.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A simple value object class holding customizable SASL extensions
 */
public class SaslExtensions {
    protected Map<String, String> extensionMap;
    protected String separator;

    protected SaslExtensions() {
        separator = ",";
        extensionMap = new HashMap<>();
    }

    public SaslExtensions(String extensions, String separator) {
        this(Utils.parseMap(extensions, "=", separator), separator);
    }

    public SaslExtensions(Map<String, String> extensionMap, String separator) {
        this.extensionMap = extensionMap;
        this.separator = separator;
    }

    public String extensionValue(String name) {
        return extensionMap.get(name);
    }

    public Set<String> extensionNames() {
        return extensionMap.keySet();
    }

    public boolean isEmpty() {
        return extensionMap.isEmpty();
    }

    @Override
    public String toString() {
        return mapToString(extensionMap);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return extensionMap.equals(((SaslExtensions) o).extensionMap);
    }

    @Override
    public int hashCode() {
        return extensionMap.hashCode();
    }

    protected Map<String, String> stringToMap(String extensions) {
        return Utils.parseMap(extensions, "=", ",");
    }

    protected String mapToString(Map<String, String> extensionMap) {
        return Utils.mkString(extensionMap, "", "", "=", separator);
    }
}
