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
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceNameType;
import org.apache.kafka.common.resource.ResourcePattern;

import java.util.Objects;

/**
 * Represents a binding between a resource pattern and an access control entry.
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public class AclBinding {
    private final ResourcePattern pattern;
    private final AccessControlEntry entry;

    /**
     * Create an instance of this class with the provided parameters.
     *
     * @param pattern non-null resource pattern.
     * @param entry non-null entry
     */
    public AclBinding(ResourcePattern pattern, AccessControlEntry entry) {
        this.pattern = Objects.requireNonNull(pattern, "pattern");
        this.entry = Objects.requireNonNull(entry, "entry");
    }

    /**
     * Create an instance of this class with the provided parameters.
     *
     * @param resource non-null resource
     * @param entry non-null entry
     * @deprecated Since 2.0. Use {@link #AclBinding(ResourcePattern, AccessControlEntry)}
     */
    @Deprecated
    public AclBinding(Resource resource, AccessControlEntry entry) {
        this(new ResourcePattern(resource.resourceType(), resource.name(), ResourceNameType.LITERAL), entry);
    }

    /**
     * @return true if this binding has any UNKNOWN components.
     */
    public boolean isUnknown() {
        return pattern.isUnknown() || entry.isUnknown();
    }

    /**
     * @return the resource pattern for this binding.
     */
    public ResourcePattern pattern() {
        return pattern;
    }

    /**
     * @return the access control entry for this binding.
     */
    public final AccessControlEntry entry() {
        return entry;
    }

    /**
     * Create a filter which matches only this AclBinding.
     */
    public AclBindingFilter toFilter() {
        return new AclBindingFilter(pattern.toFilter(), entry.toFilter());
    }

    @Override
    public String toString() {
        return "(pattern=" + pattern + ", entry=" + entry + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AclBinding that = (AclBinding) o;
        return Objects.equals(pattern, that.pattern) &&
            Objects.equals(entry, that.entry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern, entry);
    }
}
