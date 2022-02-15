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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

import java.net.InetAddress;


public class MockAuthorizableRequestContext implements AuthorizableRequestContext {
    public static class Builder {
        private String listenerName = "PLAINTEXT";
        private SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        private KafkaPrincipal principal = KafkaPrincipal.ANONYMOUS;
        private InetAddress clientAddress;
        private ApiKeys requestType = ApiKeys.FETCH;
        private short requestVersion = ApiKeys.FETCH.latestVersion();
        private String clientId = "myClientId";
        private int correlationId = 123;

        public Builder() throws Exception {
            this.clientAddress = InetAddress.getLocalHost();
        }

        public Builder setListenerName(String listenerName) {
            this.listenerName = listenerName;
            return this;
        }

        public Builder setSecurityProtocol(SecurityProtocol securityProtocol) {
            this.securityProtocol = securityProtocol;
            return this;
        }

        public Builder setPrincipal(KafkaPrincipal principal) {
            this.principal = principal;
            return this;
        }

        public Builder setClientAddress(InetAddress clientAddress) {
            this.clientAddress = clientAddress;
            return this;
        }

        public Builder setRequestType(ApiKeys requestType) {
            this.requestType = requestType;
            return this;
        }

        public Builder setRequestVersion(short requestVersion) {
            this.requestVersion = requestVersion;
            return this;
        }

        public Builder setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder setCorrelationId(int correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public MockAuthorizableRequestContext build() {
            return new MockAuthorizableRequestContext(listenerName,
                securityProtocol,
                principal,
                clientAddress,
                requestType,
                requestVersion,
                clientId,
                correlationId);
        }
    }

    private final String listenerName;
    private final SecurityProtocol securityProtocol;
    private final KafkaPrincipal principal;
    private final InetAddress clientAddress;
    private final ApiKeys requestType;
    private final short requestVersion;
    private final String clientId;
    private final int correlationId;

    private MockAuthorizableRequestContext(String listenerName,
            SecurityProtocol securityProtocol,
            KafkaPrincipal principal,
            InetAddress clientAddress,
            ApiKeys requestType,
            short requestVersion,
            String clientId,
            int correlationId) {
        this.listenerName = listenerName;
        this.securityProtocol = securityProtocol;
        this.principal = principal;
        this.clientAddress = clientAddress;
        this.requestType = requestType;
        this.requestVersion = requestVersion;
        this.clientId = clientId;
        this.correlationId = correlationId;
    }

    @Override
    public String listenerName() {
        return listenerName;
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return securityProtocol;
    }

    @Override
    public KafkaPrincipal principal() {
        return principal;
    }

    @Override
    public InetAddress clientAddress() {
        return clientAddress;
    }

    @Override
    public int requestType() {
        return requestType.id;
    }

    @Override
    public int requestVersion() {
        return requestVersion;
    }

    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public int correlationId() {
        return correlationId;
    }
}
