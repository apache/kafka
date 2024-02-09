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
package org.apache.kafka.security.authorizer;

import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.network.Session;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.Authorizer;

import java.net.InetAddress;

public class AuthorizerUtils {
    public static Authorizer createAuthorizer(String className) throws ClassNotFoundException {
        return Utils.newInstance(className, Authorizer.class);
    }

    public static boolean isClusterResource(String name) {
        return name.equals(Resource.CLUSTER_NAME);
    }

    public static AuthorizableRequestContext sessionToRequestContext(Session session) {
        return new AuthorizableRequestContext() {
            @Override
            public String clientId() {
                return "";
            }

            @Override
            public int requestType() {
                return -1;
            }

            @Override
            public String listenerName() {
                return "";
            }

            @Override
            public InetAddress clientAddress() {
                return session.clientAddress;
            }

            @Override
            public KafkaPrincipal principal() {
                return session.principal;
            }

            @Override
            public SecurityProtocol securityProtocol() {
                return null;
            }

            @Override
            public int correlationId() {
                return -1;
            }

            @Override
            public int requestVersion() {
                return -1;
            }
        };
    }
}
