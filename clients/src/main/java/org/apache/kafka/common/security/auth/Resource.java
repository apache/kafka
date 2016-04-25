/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.auth;

public class Resource {
    public static final String SEPARATOR = ":";
    public static final String WILDCARD_RESOURCE = "*";
    public static final String CLUSTER_RESOURCE_NAME = "kafka-cluster";
    public static final Resource CLUSTER_RESOURCE = new Resource(ResourceType.CLUSTER, CLUSTER_RESOURCE_NAME);

    private ResourceType resourceType;
    private String name;

    public Resource(ResourceType resourceType, String name) {
        if (resourceType == null || name == null) {
            throw new IllegalArgumentException("resourceType and name can not be null");
        }
        this.resourceType = resourceType;
        this.name = name;
    }

    public static Resource fromString(String str) {
        if (str == null || str.isEmpty()) {
            throw new IllegalArgumentException("expected a string in format resourceType:resourceName but got " + str);
        }

        String[] split = str.split(SEPARATOR, 2);

        if (split == null || split.length != 2) {
            throw new IllegalArgumentException("expected a string in format principalType:resourceName but got " + str);
        }

        ResourceType resourceType = ResourceType.forName(split[0]);
        return new Resource(resourceType, split[1]);
    }

    public String getName() {
        return name;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Resource resource = (Resource) o;

        if (resourceType != resource.resourceType) return false;
        return name.equals(resource.name);

    }

    @Override
    public int hashCode() {
        int result = resourceType.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return resourceType.name + SEPARATOR + name;
    }
}



