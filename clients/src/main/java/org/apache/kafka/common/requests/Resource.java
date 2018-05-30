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

public final class Resource {
    private final ResourceType type;
    private final String name;
    private final ResourceNameType resourceNameType;

    /**
     * @param type resource type
     * @param name resouce name
     * @param resourceNameType resource name type
     */
    public Resource(ResourceType type, String name, ResourceNameType resourceNameType) {
        this.type = type;
        this.name = name;
        this.resourceNameType = resourceNameType;
    }

    /**
     * Resource name type would default to ResourceNameType.LITERAL.
     * @param type resource type
     * @param name resource name type
     */
    public Resource(ResourceType type, String name) {
        this(type, name, ResourceNameType.LITERAL);
    }

    public ResourceType type() {
        return type;
    }

    public String name() {
        return name;
    }

    public ResourceNameType resourceNameType() {
        return resourceNameType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Resource resource = (Resource) o;

        return type == resource.type && name.equals(resource.name) && resourceNameType.equals(resource.resourceNameType);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + resourceNameType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Resource(type=" + type + ", name='" + name + "', resourceNameType=" + resourceNameType + ")";
    }
}
