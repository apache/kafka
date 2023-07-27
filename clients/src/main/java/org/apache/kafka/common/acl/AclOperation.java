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
 * Represents an operation which an ACL grants or denies permission to perform.
 *
 * Some operations imply other operations:
 * <ul>
 * <li><code>ALLOW ALL</code> implies <code>ALLOW</code> everything
 * <li><code>DENY ALL</code> implies <code>DENY</code> everything
 *
 * <li><code>ALLOW READ</code> implies <code>ALLOW DESCRIBE</code>
 * <li><code>ALLOW WRITE</code> implies <code>ALLOW DESCRIBE</code>
 * <li><code>ALLOW DELETE</code> implies <code>ALLOW DESCRIBE</code>
 *
 * <li><code>ALLOW ALTER</code> implies <code>ALLOW DESCRIBE</code>
 *
 * <li><code>ALLOW ALTER_CONFIGS</code> implies <code>ALLOW DESCRIBE_CONFIGS</code>
 * </ul>
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public enum AclOperation {
    /**
     * Represents any AclOperation which this client cannot understand, perhaps because this
     * client is too old.
     */
    UNKNOWN((byte) 0),

    /**
     * In a filter, matches any AclOperation.
     */
    ANY((byte) 1),

    /**
     * ALL operation.
     */
    ALL((byte) 2),

    /**
     * READ operation.
     */
    READ((byte) 3),

    /**
     * WRITE operation.
     */
    WRITE((byte) 4),

    /**
     * CREATE operation.
     */
    CREATE((byte) 5),

    /**
     * DELETE operation.
     */
    DELETE((byte) 6),

    /**
     * ALTER operation.
     */
    ALTER((byte) 7),

    /**
     * DESCRIBE operation.
     */
    DESCRIBE((byte) 8),

    /**
     * CLUSTER_ACTION operation.
     */
    CLUSTER_ACTION((byte) 9),

    /**
     * DESCRIBE_CONFIGS operation.
     */
    DESCRIBE_CONFIGS((byte) 10),

    /**
     * ALTER_CONFIGS operation.
     */
    ALTER_CONFIGS((byte) 11),

    /**
     * IDEMPOTENT_WRITE operation.
     */
    IDEMPOTENT_WRITE((byte) 12),

    /**
     * CREATE_TOKENS operation.
     */
    CREATE_TOKENS((byte) 13),

    /**
     * DESCRIBE_TOKENS operation.
     */
    DESCRIBE_TOKENS((byte) 14);

    // Note: we cannot have more than 30 ACL operations without modifying the format used
    // to describe ACL operations in MetadataResponse.

    private final static HashMap<Byte, AclOperation> CODE_TO_VALUE = new HashMap<>();

    static {
        for (AclOperation operation : AclOperation.values()) {
            CODE_TO_VALUE.put(operation.code, operation);
        }
    }

    /**
     * Parse the given string as an ACL operation.
     *
     * @param str    The string to parse.
     *
     * @return       The AclOperation, or UNKNOWN if the string could not be matched.
     */
    public static AclOperation fromString(String str) throws IllegalArgumentException {
        try {
            return AclOperation.valueOf(str.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    /**
     * Return the AclOperation with the provided code or `AclOperation.UNKNOWN` if one cannot be found.
     */
    public static AclOperation fromCode(byte code) {
        AclOperation operation = CODE_TO_VALUE.get(code);
        if (operation == null) {
            return UNKNOWN;
        }
        return operation;
    }

    private final byte code;

    AclOperation(byte code) {
        this.code = code;
    }

    /**
     * Return the code of this operation.
     */
    public byte code() {
        return code;
    }

    /**
     * Return true if this operation is UNKNOWN.
     */
    public boolean isUnknown() {
        return this == UNKNOWN;
    }
}
