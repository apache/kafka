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
package org.apache.kafka.common.utils;

import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class SecurityUtils {

    public static final String WILDCARD_MARKER = "*";

    public static KafkaPrincipal parseKafkaPrincipal(String str) {
        if (str == null || str.isEmpty()) {
            throw new IllegalArgumentException("expected a string in format principalType:principalName but got " + str);
        }

        String[] split = str.split(":", 2);

        if (split.length != 2) {
            throw new IllegalArgumentException("expected a string in format principalType:principalName but got " + str);
        }

        return new KafkaPrincipal(split[0], split[1]);
    }

    public static boolean matchResource(Resource stored, Resource input) {
        return matchResource(stored, input.toFilter());
    }

    public static boolean matchResource(Resource stored, ResourceFilter input) { // TODO for delete
        if (!input.resourceType().equals(ResourceType.ANY) && !input.resourceType().equals(stored.resourceType())) {
            return false;
        }
        switch (stored.resourceNameType()) {
            case LITERAL:
                switch(input.resourceNameType()) {
                    case ANY:
                        return  input.name() == null
                                || input.name().equals(stored.name())
                                || SecurityUtils.matchWildcardSuffixedString(stored.name(), input.name() + WILDCARD_MARKER);
                    case LITERAL:
                        return stored.name().equals(input.name());
                    case WILDCARD_SUFFIXED:
                        return SecurityUtils.matchWildcardSuffixedString(stored.name(), input.name() + WILDCARD_MARKER);
                    default:
                        return false;
                }
            case WILDCARD_SUFFIXED:
                switch (input.resourceNameType()) {
                    case ANY:
                        return input.name() == null
                                || SecurityUtils.matchWildcardSuffixedString(stored.name() + WILDCARD_MARKER, input.name())
                                || SecurityUtils.matchWildcardSuffixedString(stored.name() + WILDCARD_MARKER, input.name() + WILDCARD_MARKER);
                    case LITERAL:
                        return SecurityUtils.matchWildcardSuffixedString(stored.name() + WILDCARD_MARKER, input.name());
                    case WILDCARD_SUFFIXED:
                        return SecurityUtils.matchWildcardSuffixedString(stored.name() + WILDCARD_MARKER, input.name() + WILDCARD_MARKER);
                    default:
                        return false;
                }
            default:
                return false;
        }
    }

    /**
     * Returns true if two strings match, both of which might end with a WILDCARD_MARKER (which matches everything).
     * Examples:
     *   matchWildcardSuffixedString("rob", "rob") => true
     *   matchWildcardSuffixedString("*", "rob") => true
     *   matchWildcardSuffixedString("ro*", "rob") => true
     *   matchWildcardSuffixedString("rob", "bob") => false
     *   matchWildcardSuffixedString("ro*", "bob") => false
     *
     *   matchWildcardSuffixedString("rob", "*") => true
     *   matchWildcardSuffixedString("rob", "ro*") => true
     *   matchWildcardSuffixedString("bob", "ro*") => false
     *
     *   matchWildcardSuffixedString("ro*", "ro*") => true
     *   matchWildcardSuffixedString("rob*", "ro*") => false
     *   matchWildcardSuffixedString("ro*", "rob*") => true
     *
     * @param wildcardSuffixedPattern Value stored in ZK in either resource name or Acl.
     * @param resourceName Value present in the request.
     * @return true if there is a match (including wildcard-suffix matching).
     */
    static boolean matchWildcardSuffixedString(String wildcardSuffixedPattern, String resourceName) { // TODO

        if (wildcardSuffixedPattern.equals(resourceName) || wildcardSuffixedPattern.equals(WILDCARD_MARKER) || resourceName.equals(WILDCARD_MARKER)) {
            // if strings are equal or either of acl or resourceName is a wildcard
            return true;
        }

        if (wildcardSuffixedPattern.endsWith(WILDCARD_MARKER)) {

            String aclPrefix = wildcardSuffixedPattern.substring(0, wildcardSuffixedPattern.length() - WILDCARD_MARKER.length());

            if (resourceName.endsWith(WILDCARD_MARKER)) {
                // when both acl and resourceName ends with wildcard, non-wildcard prefix of resourceName should start with non-wildcard prefix of acl
                String inputPrefix = resourceName.substring(0, resourceName.length() - WILDCARD_MARKER.length());
                return inputPrefix.startsWith(aclPrefix);
            }

            // when acl ends with wildcard but resourceName doesn't, then resourceName should start with non-wildcard prefix of acl
            return resourceName.startsWith(aclPrefix);

        } else {

            if (resourceName.endsWith(WILDCARD_MARKER)) {
                // when resourceName ends with wildcard but acl doesn't, then acl should start with non-wildcard prefix of resourceName
                String inputPrefix = resourceName.substring(0, resourceName.length() - WILDCARD_MARKER.length());
                return wildcardSuffixedPattern.startsWith(inputPrefix);
            }

            // when neither acl nor resourceName ends with wildcard, they have to match exactly.
            return wildcardSuffixedPattern.equals(resourceName);

        }
    }

}
