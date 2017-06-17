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
import org.apache.kafka.common.resource.ResourceFilter;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Objects;

/**
 * A filter which can match AclBinding objects.
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public class AclBindingFilter {
    private final ResourceFilter resourceFilter;
    private final AccessControlEntryFilter entryFilter;

    /**
     * A filter which matches any ACL binding.
     */
    public static final AclBindingFilter ANY = new AclBindingFilter(
        new ResourceFilter(ResourceType.ANY, null),
        new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY));

    /**
     * Create an instance of this filter with the provided parameters.
     *
     * @param resourceFilter non-null resource filter
     * @param entryFilter non-null access control entry filter
     */
    public AclBindingFilter(ResourceFilter resourceFilter, AccessControlEntryFilter entryFilter) {
        Objects.requireNonNull(resourceFilter);
        this.resourceFilter = resourceFilter;
        Objects.requireNonNull(entryFilter);
        this.entryFilter = entryFilter;
    }

    /**
     * Return true if this filter has any UNKNOWN components.
     */
    public boolean isUnknown() {
        return resourceFilter.isUnknown() || entryFilter.isUnknown();
    }

    /**
     * Return the resource filter.
     */
    public ResourceFilter resourceFilter() {
        return resourceFilter;
    }

    /**
     * Return the access control entry filter.
     */
    public final AccessControlEntryFilter entryFilter() {
        return entryFilter;
    }

    @Override
    public String toString() {
        return "(resourceFilter=" + resourceFilter + ", entryFilter=" + entryFilter + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AclBindingFilter))
            return false;
        AclBindingFilter other = (AclBindingFilter) o;
        return resourceFilter.equals(other.resourceFilter) && entryFilter.equals(other.entryFilter);
    }

    /**
     * Return true if the resource and entry filters can only match one ACE. In other words, if
     * there are no ANY or UNKNOWN fields.
     */
    public boolean matchesAtMostOne() {
        return resourceFilter.matchesAtMostOne() && entryFilter.matchesAtMostOne();
    }

    /**
     * Return a string describing an ANY or UNKNOWN field, or null if there is no such field.
     */
    public String findIndefiniteField() {
        String indefinite = resourceFilter.findIndefiniteField();
        if (indefinite != null)
            return indefinite;
        return entryFilter.findIndefiniteField();
    }

    /**
     * Return true if the resource filter matches the binding's resource and the entry filter matches binding's entry.
     */
    public boolean matches(AclBinding binding) {
        return resourceFilter.matches(binding.resource()) && entryFilter.matches(binding.entry());
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceFilter, entryFilter);
    }
}
