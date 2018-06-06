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

import static org.apache.kafka.common.resource.Resource.WILDCARD_RESOURCE;

/**
 * A filter which matches Resource objects.
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public class ResourceFilter {
    private final ResourceType resourceType;
    private final String name;
    private final ResourceNameType nameType;

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
     * {@code nameType}.
     * <p>
     * If the filter has each three parameters fully supplied, then it will only match a resource that has exactly
     * the same values, e.g. a filter of {@code new ResourceFilter(ResourceType.GROUP, "one", ResourceTypeName.PREFIXED)}
     * will only match the resource {@code new Resource(ResourceType.GROUP, "one", ResourceTypeName.PREFIXED)}.
     * <p>
     * Any of the three parameters can be set to be ignored by the filter:
     * <ul>
     *     <li><b>{@code resourceType}</b> can be set to {@link ResourceType#ANY},
     *     meaning it will match a resource of any resource type</li>
     *     <li><b>{@code name}</b> can be set to {@code null}, meaning it will match a resource of any name.</li>
     *     <li><b>{@code nameType}</b> can be set to {@link ResourceNameType#ANY},
     *     meaning it will match a resource with any resource name type, including the
     *     {@link Resource#WILDCARD_RESOURCE wildcard resource}</li>
     * </ul>
     *
     * @param resourceType non-null resource type to filter by.
     * @param name resource name to filter by, or {@code null}.
     *             If {@code null}, the filter will ignore the name of resources.
     * @param nameType non-null resource name type to filter by.
     */
    public ResourceFilter(ResourceType resourceType, String name, ResourceNameType nameType) {
        this.resourceType = Objects.requireNonNull(resourceType, "resourceType");
        this.name = name;
        this.nameType = Objects.requireNonNull(nameType, "nameType");
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
    public ResourceNameType nameType() {
        return nameType;
    }

    @Override
    public String toString() {
        return "(resourceType=" + resourceType + ", name=" + ((name == null) ? "<any>" : name) + ", nameType=" + nameType + ")";
    }

    /**
     * Return true if this ResourceFilter has any UNKNOWN components.
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

        final ResourceFilter that = (ResourceFilter) o;
        return resourceType == that.resourceType &&
            Objects.equals(name, that.name) &&
            nameType == that.nameType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, name, nameType);
    }

    /**
     * Return true if this filter matches the given Resource.
     * @param other the resource path under which ACLs are stored.
     */
    public boolean matches(final Resource other) {
        throwOnInvalidParams(other);

        if (!resourceType().equals(ResourceType.ANY) && !resourceType().equals(other.resourceType())) {
            return false;
        }

        if (!nameType().equals(ResourceNameType.ANY) && !nameType().equals(other.nameType())) {
            return false;
        }

        if (name() == null) {
            return true;
        }

        if (nameType().equals(other.nameType())) {
            return other.name().equals(name());
        }

        switch (other.nameType()) {
            case LITERAL:
                return other.name().equals(name()) || other.name().equals(WILDCARD_RESOURCE);

            case PREFIXED:
                return name().startsWith(other.name());

            default:
                throw new IllegalArgumentException("Unsupported ResourceNameType: " + other.nameType());
        }
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
        if (nameType == ResourceNameType.ANY)
            return "Resource name type is ANY.";
        if (nameType == ResourceNameType.UNKNOWN)
            return "Resource name type is UNKNOWN.";
        return null;
    }

    private static void throwOnInvalidParams(final Resource aclPath) {
        if (aclPath.resourceType().equals(ResourceType.ANY) || aclPath.resourceType().equals(ResourceType.UNKNOWN)) {
            throw new IllegalArgumentException("Only concrete resource types are supported. Got: " + aclPath.resourceType());
        }

        if (aclPath.nameType().equals(ResourceNameType.ANY) || aclPath.nameType().equals(ResourceNameType.UNKNOWN)) {
            throw new IllegalArgumentException("Only concrete resource name types are supported. Got: " + aclPath.nameType());
        }
    }
}
