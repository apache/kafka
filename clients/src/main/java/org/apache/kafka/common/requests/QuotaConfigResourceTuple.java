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

package org.apache.kafka.common.requests;

import java.util.Objects;

/**
 * Stores a child and its parent resource. This is used for representing a (user, client) resource tuple where
 * the user part considered to be the parent resource and the client is the child.
 */
public final class QuotaConfigResourceTuple {

    private final Resource quotaConfigResource;
    private final Resource childQuotaConfigResource;

    public Resource quotaConfigResource() {
        return quotaConfigResource;
    }

    public Resource childQuotaConfigResource() {
        return childQuotaConfigResource;
    }

    public QuotaConfigResourceTuple(Resource quotaConfigResource) {
        Objects.requireNonNull(quotaConfigResource, "Quota-config resource must have a value");

        this.quotaConfigResource = quotaConfigResource;
        this.childQuotaConfigResource = new Resource(ResourceType.UNKNOWN, "");
    }

    public QuotaConfigResourceTuple(Resource quotaConfigResource, Resource childQuotaConfigResource) {
        Objects.requireNonNull(quotaConfigResource, "Quota-config resource must have a value");
        Objects.requireNonNull(childQuotaConfigResource, "The child quota-config resource must have a value");

        this.quotaConfigResource = quotaConfigResource;
        this.childQuotaConfigResource = childQuotaConfigResource;
    }

    @Override
    public int hashCode() {
        return 71 * quotaConfigResource.hashCode() + childQuotaConfigResource.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof QuotaConfigResourceTuple))
            return false;

        QuotaConfigResourceTuple other = (QuotaConfigResourceTuple) obj;

        boolean parentEquals = quotaConfigResource.equals(other.quotaConfigResource);
        boolean childEquals = childQuotaConfigResource.equals(other.childQuotaConfigResource);

        return parentEquals && childEquals;
    }

    @Override
    public String toString() {
        return "UserQuotaConfigResource=(" + (quotaConfigResource == null ? "null" : quotaConfigResource.toString()) +
                "), ClientQuotaConfigResource=(" + (childQuotaConfigResource == null ? "null" : childQuotaConfigResource.toString()) + ")";
    }
}
