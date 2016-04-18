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

public enum PermissionType {
    ALLOW((byte) 0, "Allow"),
    DENY((byte) 1, "Deny");

    private static PermissionType[] idToType;
    private static Map<String, PermissionType> nameToPermissionType;
    public static final int MAX_ID;

    static {
        int maxId = -1;
        for (PermissionType key : PermissionType.values()) {
            maxId = Math.max(maxId, key.id);
        }
        idToType = new PermissionType[maxId + 1];
        nameToPermissionType = new HashMap<String, PermissionType>();
        for (PermissionType key : PermissionType.values()) {
            idToType[key.id] = key;
            nameToPermissionType.put(key.name.toUpperCase(Locale.ROOT), key);
        }
        MAX_ID = maxId;
    }

    public final byte id;
    public final String name;

    private PermissionType(byte id, String name) {
        this.id = id;
        this.name = name;
    }

    /** Case insensitive lookup by name */
    public static PermissionType fromString(String name) {
        PermissionType permissionType = nameToPermissionType.get(name.toUpperCase(Locale.ROOT));
        if (permissionType == null) {
            throw new IllegalArgumentException(String.format("No enum constant with name %s", name));
        }
        return permissionType;
    }

    public static PermissionType forId(byte id) {
        return idToType[id];
    }
}
