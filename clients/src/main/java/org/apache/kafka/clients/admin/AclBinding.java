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
package org.apache.kafka.clients.admin;

import java.util.Objects;

/**
 * Represents a binding between a resource and an access control entry.
 */
public class AclBinding {
    private final Resource resource;
    private final AccessControlEntry entry;

    public AclBinding(Resource resource, AccessControlEntry entry) {
        Objects.requireNonNull(resource);
        this.resource = resource;
        Objects.requireNonNull(entry);
        this.entry = entry;
    }

    /**
     * Return true if this binding has any UNKNOWN components.
     */
    public boolean unknown() {
        return resource.unknown() || entry.unknown();
    }

    public Resource resource() {
        return resource;
    }

    public final AccessControlEntry entry() {
        return entry;
    }

    /**
     * Create a filter which matches only this AclBinding.
     */
    public AclBindingFilter toFilter() {
        return new AclBindingFilter(resource.toFilter(), entry.toFilter());
    }

    @Override
    public String toString() {
        return "(resource=" + resource + ", entry=" + entry + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AclBinding))
            return false;
        AclBinding other = (AclBinding) o;
        return resource.equals(other.resource) && entry.equals(other.entry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resource, entry);
    }
}
