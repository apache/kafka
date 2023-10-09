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
package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SSLPrincipalBuilder;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;

/**
 * Default implementation of {@link SSLPrincipalBuilder} for SSL authentication.
 */
public class DefaultSSLPrincipalBuilder implements SSLPrincipalBuilder {

    private DefaultKafkaPrincipalBuilder defaultKafkaPrincipalBuilder;

    /**
     * Set the SSLPrincipalMapper for building Kafka principals.
     *
     * @param sslPrincipalMapper The SSLPrincipalMapper instance.
     */
    @Override
    public void buildSSLPrincipalBuilder(SslPrincipalMapper sslPrincipalMapper) {
        this.defaultKafkaPrincipalBuilder = new DefaultKafkaPrincipalBuilder(null, sslPrincipalMapper);
    }

    /**
     * Build the Kafka principal based on the provided AuthenticationContext.
     *
     * @param context The AuthenticationContext containing authentication details.
     * @return The constructed KafkaPrincipal.
     */
    @Override
    public KafkaPrincipal build(AuthenticationContext context) {
        return defaultKafkaPrincipalBuilder.build(context);
    }
}