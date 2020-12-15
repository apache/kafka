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

package org.apache.kafka.common.acl;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.Locale;

/**
 * Represents whether an ACL grants or denies permissions.
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public enum AclPermissionType {
    /**
     * Represents any AclPermissionType which this client cannot understand,
     * perhaps because this client is too old.
     */
    UNKNOWN((byte) 0),

    /**
     * In a filter, matches any AclPermissionType.
     */
    ANY((byte) 1),

    /**
     * Disallows access.
     */
    DENY((byte) 2),

    /**
     * Grants access.
     */
    ALLOW((byte) 3);

    private final static HashMap<Byte, AclPermissionType> CODE_TO_VALUE = new HashMap<>();

    static {
        for (AclPermissionType permissionType : AclPermissionType.values()) {
            CODE_TO_VALUE.put(permissionType.code, permissionType);
        }
    }

    /**
    * Parse the given string as an ACL permission.
    *
    * @param str    The string to parse.
    *
    * @return       The AclPermissionType, or UNKNOWN if the string could not be matched.
    */
    public static AclPermissionType fromString(String str) {
        try {
            return AclPermissionType.valueOf(str.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    /**
     * Return the AclPermissionType with the provided code or `AclPermissionType.UNKNOWN` if one cannot be found.
     */
    public static AclPermissionType fromCode(byte code) {
        AclPermissionType permissionType = CODE_TO_VALUE.get(code);
        if (permissionType == null) {
            return UNKNOWN;
        }
        return permissionType;
    }

    private final byte code;

    AclPermissionType(byte code) {
        this.code = code;
    }

    /**
     * Return the code of this permission type.
     */
    public byte code() {
        return code;
    }

    /**
     * Return true if this permission type is UNKNOWN.
     */
    public boolean isUnknown() {
        return this == UNKNOWN;
    }
}
