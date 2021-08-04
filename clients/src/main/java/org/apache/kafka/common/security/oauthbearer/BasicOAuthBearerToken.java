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

package org.apache.kafka.common.security.oauthbearer;

import java.util.Collections;
import java.util.Set;
import java.util.StringJoiner;

public class BasicOAuthBearerToken implements OAuthBearerToken {

    private final String value;

    private final Set<String> scope;

    private final Long lifetimeMs;

    private final String principalName;

    private final Long startTimeMs;

    public BasicOAuthBearerToken(final String value, final Set<String> scope, final Long lifetimeMs,
        final String principalName,
        final Long startTimeMs) {
        this.value = value;
        this.scope = Collections.unmodifiableSet(scope);
        this.lifetimeMs = lifetimeMs;
        this.principalName = principalName;
        this.startTimeMs = startTimeMs;
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public Set<String> scope() {
        return scope;
    }

    @Override
    public long lifetimeMs() {
        return lifetimeMs;
    }

    @Override
    public String principalName() {
        return principalName;
    }

    @Override
    public Long startTimeMs() {
        return startTimeMs;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", BasicOAuthBearerToken.class.getSimpleName() + "[", "]")
            .add("value='" + value + "'")
            .add("scope=" + scope)
            .add("lifetimeMs=" + lifetimeMs)
            .add("principalName='" + principalName + "'")
            .add("startTimeMs=" + startTimeMs)
            .toString();
    }

}
