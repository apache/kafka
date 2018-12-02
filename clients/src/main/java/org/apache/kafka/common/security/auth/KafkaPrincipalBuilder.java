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
package org.apache.kafka.common.security.auth;

/**
 * Pluggable principal builder interface which supports both SSL authentication through
 * {@link SslAuthenticationContext} and SASL through {@link SaslAuthenticationContext}.
 *
 * Note that the {@link org.apache.kafka.common.Configurable} and {@link java.io.Closeable}
 * interfaces are respected if implemented. Additionally, implementations must provide a
 * default no-arg constructor.
 */
public interface KafkaPrincipalBuilder {
    /**
     * Build a kafka principal from the authentication context.
     * @param context The authentication context (either {@link SslAuthenticationContext} or
     *        {@link SaslAuthenticationContext})
     * @return The built principal which may provide additional enrichment through a subclass of
     *        {@link KafkaPrincipalBuilder}.
     */
    KafkaPrincipal build(AuthenticationContext context);
}
