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
 * Represents a cluster resource with a tuple of (type, name, nameType).
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public class Resource {
    /**
     * A special literal resource name that corresponds to 'all resources of a certain type'.
     */
    public static final String WILDCARD_RESOURCE = "*";

    private final ResourceType resourceType;
    private final String name;
    private final ResourceNameType nameType;

    /**
     * The name of the CLUSTER resource.
     */
    public final static String CLUSTER_NAME = "kafka-cluster";

    /**
     * A resource representing the whole cluster.
     */
    public final static Resource CLUSTER = new Resource(ResourceType.CLUSTER, CLUSTER_NAME, ResourceNameType.LITERAL);

    /**
     * Create an instance of this class with the provided parameters.
     *
     * @param resourceType non-null resource type
     * @param name non-null resource name
     * @param nameType non-null resource name type
     */
    public Resource(ResourceType resourceType, String name, ResourceNameType nameType) {
        this.resourceType = Objects.requireNonNull(resourceType, "resourceType");
        this.name = Objects.requireNonNull(name, "name");
        this.nameType = Objects.requireNonNull(nameType, "nameType");
    }

    /**
     * Create an instance of this class with the provided parameters.
     * Resource name type would default to ResourceNameType.LITERAL.
     *
     * @param resourceType non-null resource type
     * @param name non-null resource name
     * @deprecated Since 2.0. Use {@link #Resource(ResourceType, String, ResourceNameType)}
     */
    @Deprecated
    public Resource(ResourceType resourceType, String name) {
        this(resourceType, name, ResourceNameType.LITERAL);
    }

    /**
     * Return the resource type.
     */
    public ResourceType resourceType() {
        return resourceType;
    }

    /**
     * Return the resource name type.
     */
    public ResourceNameType nameType() {
        return nameType;
    }

    /**
     * Return the resource name.
     */
    public String name() {
        return name;
    }

    /**
     * Create a filter which matches only this Resource.
     */
    public ResourceFilter toFilter() {
        return new ResourceFilter(resourceType, name, nameType);
    }

    @Override
    public String toString() {
        return "(resourceType=" + resourceType + ", name=" + ((name == null) ? "<any>" : name) + ", nameType=" + nameType + ")";
    }

    /**
     * Return true if this Resource has any UNKNOWN components.
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

        final Resource resource = (Resource) o;
        return resourceType == resource.resourceType &&
            Objects.equals(name, resource.name) &&
            nameType == resource.nameType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, name, nameType);
    }
}
