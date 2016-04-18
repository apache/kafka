/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.security.auth;


import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public enum ResourceType {
    CLUSTER((byte) 0, "Cluster"),
    TOPIC((byte) 1, "Topic"),
    GROUP((byte) 2, "Group");

    private static ResourceType[] idToType;
    private static Map<String, ResourceType> nameToResourceType;
    public static final int MAX_ID;

    static {
        int maxId = -1;
        for (ResourceType key : ResourceType.values()) {
            maxId = Math.max(maxId, key.id);
        }
        idToType = new ResourceType[maxId + 1];
        nameToResourceType = new HashMap<String, ResourceType>();
        for (ResourceType key : ResourceType.values()) {
            idToType[key.id] = key;
            nameToResourceType.put(key.name.toUpperCase(Locale.ROOT), key);
        }
        MAX_ID = maxId;
    }

    public final byte id;
    public final String name;

    private ResourceType(byte id, String name) {
        this.id = id;
        this.name = name;
    }

    /** Case insensitive lookup by name */
    public static ResourceType fromString(String name) {
        ResourceType resourceType = nameToResourceType.get(name.toUpperCase(Locale.ROOT));
        if (resourceType == null) {
            throw new IllegalArgumentException(String.format("No enum constant with name %s", name));
        }
        return resourceType;
    }

    public static ResourceType forId(byte id) {
        return idToType[id];
    }
}

