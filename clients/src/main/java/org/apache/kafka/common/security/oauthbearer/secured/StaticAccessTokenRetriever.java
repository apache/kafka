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

package org.apache.kafka.common.security.oauthbearer.secured;

import java.util.Objects;

/**
 * <code>StaticAccessTokenRetriever</code> is an {@link AccessTokenRetriever} that will
 * use the {@link LoginCallbackHandlerConfiguration#ACCESS_TOKEN_CONFIG} configuration
 * option as a JWT access key in the serialized form.
 *
 * @see AccessTokenRetriever
 * @see LoginCallbackHandlerConfiguration#ACCESS_TOKEN_CONFIG
 */

public class StaticAccessTokenRetriever implements AccessTokenRetriever {

    private final String accessToken;

    public StaticAccessTokenRetriever(String accessToken) {
        this.accessToken = Objects.requireNonNull(accessToken);
    }

    /**
     * Retrieves a JWT access token in its serialized three-part form as provided directly
     * in the client configuration.
     *
     * @return Non-<code>null</code> JWT access token string
     */

    @Override
    public String retrieve() {
        return accessToken;
    }

}
