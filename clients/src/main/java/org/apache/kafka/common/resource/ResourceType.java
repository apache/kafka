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

package org.apache.kafka.common.resource;

import java.util.HashMap;
import java.util.Locale;

/**
 * Represents a type of resource which an ACL can be applied to.
 */
public enum ResourceType {
    /**
     * Represents any ResourceType which this client cannot understand,
     * perhaps because this client is too old.
     */
    UNKNOWN((byte) 0),

    /**
     * In a filter, matches any ResourceType.
     */
    ANY((byte) 1),

    /**
     * A Kafka topic.
     */
    TOPIC((byte) 2),

    /**
     * A consumer group.
     */
    GROUP((byte) 3),

    /**
     * The cluster as a whole.
     */
    CLUSTER((byte) 4),

    /**
     * A transactional ID.
     */
    TRANSACTIONAL_ID((byte) 5);

    private final static HashMap<Byte, ResourceType> CODE_TO_VALUE = new HashMap<>();

    static {
        for (ResourceType resourceType : ResourceType.values()) {
            CODE_TO_VALUE.put(resourceType.code, resourceType);
        }
    }

    /**
     * Parse the given string as an ACL resource type.
     *
     * @param str    The string to parse.
     *
     * @return       The ResourceType, or UNKNOWN if the string could not be matched.
     */
    public static ResourceType fromString(String str) throws IllegalArgumentException {
        try {
            return ResourceType.valueOf(str.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    public static ResourceType fromCode(byte code) {
        ResourceType resourceType = CODE_TO_VALUE.get(code);
        if (resourceType == null) {
            return UNKNOWN;
        }
        return resourceType;
    }

    private final byte code;

    ResourceType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public boolean unknown() {
        return this == UNKNOWN;
    }
}
