/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.network;

import java.security.Principal;
import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.security.auth.PrincipalBuilder;
import org.apache.kafka.common.KafkaException;

public class DefaultAuthenticator implements Authenticator {

    private TransportLayer transportLayer;
    private PrincipalBuilder principalBuilder;
    private Principal principal;

    public void configure(TransportLayer transportLayer, PrincipalBuilder principalBuilder, Map<String, ?> configs) {
        this.transportLayer = transportLayer;
        this.principalBuilder = principalBuilder;
    }

    /**
     * No-Op for default authenticator
     */
    public void authenticate() throws IOException {}

    /**
     * Constructs Principal using configured principalBuilder.
     * @return Principal
     * @throws KafkaException
     */
    public Principal principal() throws KafkaException {
        if (principal == null)
            principal = principalBuilder.buildPrincipal(transportLayer, this);
        return principal;
    }

    public void close() throws IOException {}

    /**
     * DefaultAuthenticator doesn't implement any additional authentication mechanism.
     * @return true
     */
    public boolean complete() {
        return true;
    }

}
