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
package org.apache.kafka.common.errors;

import javax.net.ssl.SSLException;

/**
 * This exception indicates that SASL authentication has failed.
 * On authentication failure, clients abort the operation requested and raise one
 * of the subclasses of this exception:
 * <ul>
 *   </li>{@link SaslAuthenticationException} if SASL handshake fails with invalid credentials
 *   or any other failure specific to the SASL mechanism used for authentication</li>
 *   <li>{@link UnsupportedSaslMechanismException} if the SASL mechanism requested by the client
 *   is not supported on the broker.</li>
 *   <li>{@link IllegalSaslStateException} if an unexpected request is received on during SASL
 *   handshake. This could be due to misconfigured security protocol.</li>
 *   <li>{@link SslAuthenticationException} if SSL handshake failed due to any {@link SSLException}.
 * </ul>
 */
public class AuthenticationException extends ApiException {

    private static final long serialVersionUID = 1L;

    public AuthenticationException(String message) {
        super(message);
    }

    public AuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

}
