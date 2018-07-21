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

import static org.apache.kafka.common.resource.ResourcePattern.WILDCARD_RESOURCE;

/**
 * Represents a filter that can match {@link ResourcePattern}.
 * <p>
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public class ResourcePatternFilter {
    /**
     * Matches any resource pattern.
     */
    public static final ResourcePatternFilter ANY = new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY);

    private final ResourceType resourceType;
    private final String name;
    private final PatternType patternType;

    /**
     * Create a filter using the supplied parameters.
     *
     * @param resourceType non-null resource type.
     *                     If {@link ResourceType#ANY}, the filter will ignore the resource type of the pattern.
     *                     If any other resource type, the filter will match only patterns with the same type.
     * @param name         resource name or {@code null}.
     *                     If {@code null}, the filter will ignore the name of resources.
     *                     If {@link ResourcePattern#WILDCARD_RESOURCE}, will match only wildcard patterns.
     * @param patternType  non-null resource pattern type.
     *                     If {@link PatternType#ANY}, the filter will match patterns regardless of pattern type.
     *                     If {@link PatternType#MATCH}, the filter will match patterns that would match the supplied
     *                     {@code name}, including a matching prefixed and wildcards patterns.
     *                     If any other resource pattern type, the filter will match only patterns with the same type.
     */
    public ResourcePatternFilter(ResourceType resourceType, String name, PatternType patternType) {
        this.resourceType = Objects.requireNonNull(resourceType, "resourceType");
        this.name = name;
        this.patternType = Objects.requireNonNull(patternType, "patternType");
    }

    /**
     * @return {@code true} if this filter has any UNKNOWN components.
     */
    public boolean isUnknown() {
        return resourceType.isUnknown() || patternType.isUnknown();
    }

    /**
     * @return the specific resource type this pattern matches
     */
    public ResourceType resourceType() {
        return resourceType;
    }

    /**
     * @return the resource name.
     */
    public String name() {
        return name;
    }

    /**
     * @return the resource pattern type.
     */
    public PatternType patternType() {
        return patternType;
    }

    /**
     * @return {@code true} if this filter matches the given pattern.
     */
    public boolean matches(ResourcePattern pattern) {
        if (!resourceType.equals(ResourceType.ANY) && !resourceType.equals(pattern.resourceType())) {
            return false;
        }

        if (!patternType.equals(PatternType.ANY) && !patternType.equals(PatternType.MATCH) && !patternType.equals(pattern.patternType())) {
            return false;
        }

        if (name == null) {
            return true;
        }

        if (patternType.equals(PatternType.ANY) || patternType.equals(pattern.patternType())) {
            return name.equals(pattern.name());
        }

        switch (pattern.patternType()) {
            case LITERAL:
                return name.equals(pattern.name()) || pattern.name().equals(WILDCARD_RESOURCE);

            case PREFIXED:
                return name.startsWith(pattern.name());

            default:
                throw new IllegalArgumentException("Unsupported PatternType: " + pattern.patternType());
        }
    }

    /**
     * @return {@code true} if this filter could only match one pattern.
     * In other words, if there are no ANY or UNKNOWN fields.
     */
    public boolean matchesAtMostOne() {
        return findIndefiniteField() == null;
    }

    /**
     * @return a string describing any ANY or UNKNOWN field, or null if there is no such field.
     */
    public String findIndefiniteField() {
        if (resourceType == ResourceType.ANY)
            return "Resource type is ANY.";
        if (resourceType == ResourceType.UNKNOWN)
            return "Resource type is UNKNOWN.";
        if (name == null)
            return "Resource name is NULL.";
        if (patternType == PatternType.MATCH)
            return "Resource pattern type is ANY.";
        if (patternType == PatternType.UNKNOWN)
            return "Resource pattern type is UNKNOWN.";
        return null;
    }

    @Override
    public String toString() {
        return "ResourcePattern(resourceType=" + resourceType + ", name=" + ((name == null) ? "<any>" : name) + ", patternType=" + patternType + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final ResourcePatternFilter resource = (ResourcePatternFilter) o;
        return resourceType == resource.resourceType &&
            Objects.equals(name, resource.name) &&
            patternType == resource.patternType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, name, patternType);
    }
}
