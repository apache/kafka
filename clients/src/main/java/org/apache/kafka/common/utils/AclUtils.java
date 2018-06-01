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

package org.apache.kafka.common.utils;

import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceFilter;
import org.apache.kafka.common.resource.ResourceNameType;
import org.apache.kafka.common.resource.ResourceType;

public final class AclUtils {
    public static final ResourceNameType LITERAL = ResourceNameType.LITERAL;
    public static final String WILDCARD_RESOURCE = "*";

    private AclUtils() {
    }

    /**
     * Checks for a match between a resource path under which ACLs are stored and a resource filter.
     *
     * @param aclPath the resource path under which ACLs are stored.
     * @param filter  a filter to match against.
     * @return {@code true} if the resource matches the filter, {@code false} otherwise.
     */
    public static boolean matchResource(final Resource aclPath, final ResourceFilter filter) {
        throwOnInvalidParams(aclPath);

        if (!filter.resourceType().equals(ResourceType.ANY) && !filter.resourceType().equals(aclPath.resourceType())) {
            return false;
        }

        if (!filter.resourceNameType().equals(ResourceNameType.ANY) && !filter.resourceNameType().equals(aclPath.resourceNameType())) {
            return false;
        }

        if (filter.name() == null) {
            return true;
        }

        if (filter.resourceNameType().equals(aclPath.resourceNameType())) {
            return aclPath.name().equals(filter.name());
        }

        switch (aclPath.resourceNameType()) {
            case LITERAL:
                return aclPath.name().equals(filter.name()) || aclPath.name().equals(WILDCARD_RESOURCE);

            case PREFIXED:
                return filter.name().startsWith(aclPath.name());

            default:
                throw new IllegalArgumentException("Unsupported ResourceNameType: " + aclPath.resourceNameType());
        }
    }

    private static void throwOnInvalidParams(final Resource aclPath) {
        if (aclPath.resourceType().equals(ResourceType.ANY) || aclPath.resourceType().equals(ResourceType.UNKNOWN)) {
            throw new IllegalArgumentException("Only concrete resource types are supported. Got: " + aclPath.resourceType());
        }

        if (aclPath.resourceNameType().equals(ResourceNameType.ANY) || aclPath.resourceNameType().equals(ResourceNameType.UNKNOWN)) {
            throw new IllegalArgumentException("Only concrete resource name types are supported. Got: " + aclPath.resourceNameType());
        }
    }
}
