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

package org.apache.kafka.server.authorizer;

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.net.InetAddress;

/**
 * Request context interface that provides data from request header as well as connection
 * and authentication information to plugins.
 */
@InterfaceAudience.Public
public interface AuthorizableRequestContext {

    /**
     * Returns name of listener on which request was received.
     */
    String listenerName();

    /**
     * Returns the security protocol for the listener on which request was received.
     */
    SecurityProtocol securityProtocol();

    /**
     * Returns authenticated principal for the connection on which request was received.
     */
    KafkaPrincipal principal();

    /**
     * Returns client IP address from which request was sent.
     */
    InetAddress clientAddress();

    /**
     * Returns the 16-bit API key ({@code request_api_key}) from the request header.
     * @see <a href="https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/RequestHeader.json#L29-L30">RequestHeader.json</a>
     *
     */
    int requestType();

    /**
     * Returns the 16-bit API version ({@code request_api_version}) from the request header.
     * @see <a href="https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/RequestHeader.json#L31-L32">RequestHeader.json</a>
     *
     */
    int requestVersion();

    /**
     * Returns the client id from the request header.
     */
    String clientId();

    /**
     * Returns the correlation id from the request header.
     */
    int correlationId();
}
