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
package org.apache.kafka.metadata.authorizer;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

import java.util.function.BiPredicate;

import static org.apache.kafka.common.resource.ResourcePattern.WILDCARD_RESOURCE;

/**
 * An Authorizer that extends the standard Authorizer by reimplementing the
 * {@link #authorizeByResourceType(AuthorizableRequestContext, AclOperation, ResourceType)}
 * method and providing a Radix Tree implementation for the {@link AuthorizerData}.
 * <p>
 *     All implementation details are described in the {@link TrieAuthorizerData} javadoc.
 * </p>
 * @see <a href="https://en.wikipedia.org/wiki/Radix_tree">Radix Tree (Wikipedia)</a>
 */
public class TrieAuthorizer extends StandardAuthorizer {

    /**
     * Constructor.
     */
    public TrieAuthorizer() {
        super(TrieAuthorizerData.createEmpty());
    }

    /**
     * Gets the pattern name matcher for the Trie implementaiton.  This is exposes like this for testing
     * the ResourcePatternFilter.
     * @return
     */
    public static BiPredicate<ResourcePattern, String> getPatternNameMatcher() {
        return (pattern, name) -> {
            switch (pattern.patternType()) {
                case LITERAL:
                    return name.equals(pattern.name()) || pattern.name().equals(WILDCARD_RESOURCE);

                case PREFIXED:
                    return  name.startsWith(pattern.name());

                default:
                    throw new IllegalArgumentException("Unsupported PatternType: " + pattern.patternType());
            }
        };
    }

}
