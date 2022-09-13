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
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Set;
import java.util.function.Function;

import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;


class PrefixRuleFinder implements Function<PrefixNode, Boolean> {
    private final AclOperation operation;
    private final Set<KafkaPrincipal> matchingPrincipals;
    private final String host;
    private MatchingRule bestRule;

    PrefixRuleFinder(
        AclOperation operation,
        Set<KafkaPrincipal> matchingPrincipals,
        String host,
        MatchingRule bestRule
    ) {
        this.operation = operation;
        this.matchingPrincipals = matchingPrincipals;
        this.host = host;
        this.bestRule = bestRule;
    }

    @Override
    public Boolean apply(PrefixNode node) {
        MatchingRule newRule =
                node.resourceAcls().authorize(operation, matchingPrincipals, host);
        if (newRule == null) return true;
        if (newRule.result() == DENIED) {
            bestRule = newRule;
            return false;
        }
        if (bestRule == null) {
            bestRule = newRule;
        }
        return true;
    }

    MatchingRule bestRule() {
        return bestRule;
    }
}