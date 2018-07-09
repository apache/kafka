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
package org.apache.kafka.common.security.scram.internals;

import org.apache.kafka.common.security.scram.ScramLoginModule;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ScramExtensions {
    private final Map<String, String> extensionMap;

    public ScramExtensions() {
        this(Collections.<String, String>emptyMap());
    }

    public ScramExtensions(String extensions) {
        this(stringToMap(extensions));
    }

    public ScramExtensions(Map<String, String> extensionMap) {
        this.extensionMap = extensionMap;
    }

    public String extensionValue(String name) {
        return extensionMap.get(name);
    }

    public Set<String> extensionNames() {
        return extensionMap.keySet();
    }

    public boolean tokenAuthenticated() {
        return Boolean.parseBoolean(extensionMap.get(ScramLoginModule.TOKEN_AUTH_CONFIG));
    }

    @Override
    public String toString() {
        return mapToString(extensionMap);
    }

    private static Map<String, String> stringToMap(String extensions) {
        Map<String, String> extensionMap = new HashMap<>();

        if (!extensions.isEmpty()) {
            String[] attrvals = extensions.split(",");
            for (String attrval : attrvals) {
                String[] array = attrval.split("=", 2);
                extensionMap.put(array[0], array[1]);
            }
        }
        return extensionMap;
    }

    private static String mapToString(Map<String, String> extensionMap) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : extensionMap.entrySet()) {
            builder.append(entry.getKey());
            builder.append('=');
            builder.append(entry.getValue());
        }
        return builder.toString();
    }
}