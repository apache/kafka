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
 * Represents a cluster resource with a tuple of (type, name).
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public class Resource {
    private final ResourceType resourceType;
    private final String name;

    /**
     * The name of the CLUSTER resource.
     */
    public final static String CLUSTER_NAME = "kafka-cluster";

    /**
     * A resource representing the whole cluster.
     */
    public final static Resource CLUSTER = new Resource(ResourceType.CLUSTER, CLUSTER_NAME);

    /**
     * Create an instance of this class with the provided parameters.
     *
     * @param resourceType non-null resource type
     * @param name non-null resource name
     */
    public Resource(ResourceType resourceType, String name) {
        Objects.requireNonNull(resourceType);
        this.resourceType = resourceType;
        Objects.requireNonNull(name);
        this.name = name;
    }

    /**
     * Return the resource type.
     */
    public ResourceType resourceType() {
        return resourceType;
    }

    /**
     * Return the resource name.
     */
    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "(resourceType=" + resourceType + ", name=" + name + ")";
    }

    /**
     * Return true if this Resource has any UNKNOWN components.
     */
    public boolean isUnknown() {
        return resourceType.isUnknown();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Resource))
            return false;
        Resource other = (Resource) o;
        return resourceType.equals(other.resourceType) && Objects.equals(name, other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, name);
    }
}
