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
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import java.util.Collections;

/**
 * An Authorizer that extends the standard Authorizer by reimplementing the
 * {@link #authorizeByResourceType(AuthorizableRequestContext, AclOperation, ResourceType)}
 * method and providing a Radix Tree implementation for the {@link AuthorizerData}.
 * <p>
 *     All implementation details are described in the {@link NameTrieAuthorizerData} javadoc.
 * </p>
 * @see <a href="https://en.wikipedia.org/wiki/Radix_tree">Radix Tree (Wikipedia)</a>
 */
public class NameTrieAuthorizer extends StandardAuthorizer {

    /**
     * Constructor.
     */
    public NameTrieAuthorizer() {
        super(NameTrieAuthorizerData.createEmpty());
    }
}
