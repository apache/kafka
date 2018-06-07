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

import java.util.Objects;

/**
 * Represents a pattern that is used by ACLs to match zero or more
 * {@link org.apache.kafka.common.resource.Resource Resources}.
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public class ResourcePattern {
    /**
     * A special literal resource name that corresponds to 'all resources of a certain type'.
     */
    public static final String WILDCARD_RESOURCE = "*";

    private final ResourceType resourceType;
    private final String name;
    private final ResourceNameType nameType;

   /**
     * Create a pattern using the supplied parameters.
     *
     * @param resourceType non-null, specific, resource type
     * @param name non-null resource name, which can be the {@link #WILDCARD_RESOURCE}.
     * @param nameType non-null, specific, resource name type, which controls how the pattern will match resource names.
     */
    public ResourcePattern(ResourceType resourceType, String name, ResourceNameType nameType) {
        this.resourceType = Objects.requireNonNull(resourceType, "resourceType");
        this.name = Objects.requireNonNull(name, "name");
        this.nameType = Objects.requireNonNull(nameType, "nameType");

        if (resourceType == ResourceType.ANY) {
            throw new IllegalArgumentException("resourceType must not be ANY");
        }

        if (nameType == ResourceNameType.ANY) {
            throw new IllegalArgumentException("nameType must not be ANY");
        }
    }

    /**
     * @return the specific resource type this pattern matches
     */
    public ResourceType resourceType() {
        return resourceType;
    }

    /**
     * @return the resource name.
     */
    public String name() {
        return name;
    }

    /**
     * @return the resource name type.
     */
    public ResourceNameType nameType() {
        return nameType;
    }

    /**
     * @return a filter which matches only this pattern.
     */
    public ResourcePatternFilter toFilter() {
        return new ResourcePatternFilter(resourceType, name, nameType);
    }

    @Override
    public String toString() {
        return "ResourcePattern(resourceType=" + resourceType + ", name=" + ((name == null) ? "<any>" : name) + ", nameType=" + nameType + ")";
    }

    /**
     * @return {@code true} if this Resource has any UNKNOWN components.
     */
    public boolean isUnknown() {
        return resourceType.isUnknown() || nameType.isUnknown();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final ResourcePattern resource = (ResourcePattern) o;
        return resourceType == resource.resourceType &&
            Objects.equals(name, resource.name) &&
            nameType == resource.nameType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, name, nameType);
    }
}
