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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

// TODO cleanup

/**
 *
 */
@InterfaceStability.Evolving
public enum ResourceNameType {
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
    LITERAL((byte) 2),

    /**
     * A consumer group.
     */
    WILDCARD_SUFFIXED((byte) 3);

    private final static Map<Byte, ResourceNameType> CODE_TO_VALUE;

    static {
        final Map<Byte, ResourceNameType> codeToValues = new HashMap<>();
        for (ResourceNameType resourceType : ResourceNameType.values()) {
            codeToValues.put(resourceType.code, resourceType);
        }
        CODE_TO_VALUE = Collections.unmodifiableMap(codeToValues);
    }

    private final byte code;

    ResourceNameType(byte code) {
        this.code = code;
    }

    /**
     * Return the code of this resource.
     */
    public byte code() {
        return code;
    }

    /**
     * Return the ResourceNameType with the provided code or `ResourceNameType.UNKNOWN` if one cannot be found.
     */
    public static ResourceNameType fromCode(byte code) {
        ResourceNameType resourceNameType = CODE_TO_VALUE.get(code);
        if (resourceNameType == null) {
            return UNKNOWN;
        }
        return resourceNameType;
    }

    /**
     * Return whether this resource name type is UNKNOWN.
     */
    public boolean isUnknown() {
        return this == UNKNOWN;
    }

}
