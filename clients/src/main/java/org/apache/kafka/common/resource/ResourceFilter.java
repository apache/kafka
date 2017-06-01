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

import java.util.Objects;

/**
 * A filter which matches Resource objects.
 */
public class ResourceFilter {
    private final ResourceType resourceType;
    private final String name;

    public static final ResourceFilter ANY = new ResourceFilter(ResourceType.ANY, null);

    public ResourceFilter(ResourceType resourceType, String name) {
        Objects.requireNonNull(resourceType);
        this.resourceType = resourceType;
        this.name = name;
    }

    public ResourceType resourceType() {
        return resourceType;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "(resourceType=" + resourceType + ", name=" + ((name == null) ? "<any>" : name) + ")";
    }

    /**
     * Return true if this ResourceFilter has any UNKNOWN components.
     */
    public boolean unknown() {
        return resourceType.unknown();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ResourceFilter))
            return false;
        ResourceFilter other = (ResourceFilter) o;
        return resourceType.equals(other.resourceType) && Objects.equals(name, other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, name);
    }

    public boolean matches(Resource other) {
        if ((name != null) && (!name.equals(other.name())))
            return false;
        if ((resourceType != ResourceType.ANY) && (!resourceType.equals(other.resourceType())))
            return false;
        return true;
    }

    public boolean matchesAtMostOne() {
        return findIndefiniteField() == null;
    }

    public String findIndefiniteField() {
        if (resourceType == ResourceType.ANY)
            return "Resource type is ANY.";
        if (resourceType == ResourceType.UNKNOWN)
            return "Resource type is UNKNOWN.";
        if (name == null)
            return "Resource name is NULL.";
        return null;
    }
}
