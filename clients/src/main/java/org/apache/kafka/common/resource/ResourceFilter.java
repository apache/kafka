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
import org.apache.kafka.common.utils.AclUtils;

import java.util.Objects;

/**
 * A filter which matches Resource objects.
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public class ResourceFilter {
    private final ResourceType resourceType;
    private final String name;
    private final ResourceNameType resourceNameType;

    /**
     * Matches any resource.
     */
    public static final ResourceFilter ANY = new ResourceFilter(ResourceType.ANY, null, ResourceNameType.ANY);

    /**
     * Create a filter that matches {@link ResourceNameType#LITERAL literal} resources of the
     * supplied {@code resourceType} and {@code name}.
     *
     * @param resourceType non-null resource type
     * @param name resource name or {@code null}.
     *             If {@code null}, the filter will ignore the name of resources.
     * @deprecated Since 2.0. Use {@link #ResourceFilter(ResourceType, String, ResourceNameType)}
     */
    @Deprecated
    public ResourceFilter(ResourceType resourceType, String name) {
        this(resourceType, name, ResourceNameType.LITERAL);
    }

    /**
     * Create a filter that matches resources of the supplied {@code resourceType}, {@code name} and
     * {@code resourceNameType}.
     *
     * @param resourceType non-null resource type.
     * @param name resource name or {@code null}.
     *             If {@code null}, the filter will ignore the name of resources.
     * @param resourceNameType non-null resource name type
     */
    public ResourceFilter(ResourceType resourceType, String name, ResourceNameType resourceNameType) {
        this.resourceType = Objects.requireNonNull(resourceType, "resourceType");
        this.name = name;
        this.resourceNameType = Objects.requireNonNull(resourceNameType, "resourceNameType");
    }

    /**
     * Return the resource type.
     */
    public ResourceType resourceType() {
        return resourceType;
    }

    /**
     * Return the resource name or null.
     */
    public String name() {
        return name;
    }

    /**
     * Return the resource name type.
     */
    public ResourceNameType resourceNameType() {
        return resourceNameType;
    }

    @Override
    public String toString() {
        return "(resourceType=" + resourceType + ", name=" + ((name == null) ? "<any>" : name) + ", resourceNameType=" + resourceNameType + ")";
    }

    /**
     * Return true if this ResourceFilter has any UNKNOWN components.
     */
    public boolean isUnknown() {
        return resourceType.isUnknown() || resourceNameType.isUnknown();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final ResourceFilter that = (ResourceFilter) o;
        return resourceType == that.resourceType &&
            Objects.equals(name, that.name) &&
            resourceNameType == that.resourceNameType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, name, resourceNameType);
    }

    /**
     * Return true if this filter matches the given Resource.
     * @param other the resource path under which ACLs are stored.
     */
    public boolean matches(Resource other) {
        return AclUtils.matchResource(other, this);
    }

    /**
     * Return true if this filter could only match one ACE. In other words, if there are no ANY or UNKNOWN fields.
     */
    public boolean matchesAtMostOne() {
        return findIndefiniteField() == null;
    }

    /**
     * Return a string describing an ANY or UNKNOWN field, or null if there is no such field.
     */
    public String findIndefiniteField() {
        if (resourceType == ResourceType.ANY)
            return "Resource type is ANY.";
        if (resourceType == ResourceType.UNKNOWN)
            return "Resource type is UNKNOWN.";
        if (name == null)
            return "Resource name is NULL.";
        if (resourceNameType == ResourceNameType.ANY)
            return "Resource name type is ANY.";
        if (resourceNameType == ResourceNameType.UNKNOWN)
            return "Resource name type is UNKNOWN.";
        return null;
    }
}
