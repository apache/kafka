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
 * This exception indicates that SSL handshake has failed. See {@link #getCause()}
 * for the {@link SSLException} that caused this failure.
 * <p>
 * SSL handshake failures in clients may indicate client authentication
 * failure due to untrusted certificates if server is configured to request
 * client certificates. Handshake failures could also indicate misconfigured
 * security including protocol/cipher suite mismatch, server certificate
 * authentication failure or server host name verification failure.
 * </p>
 */
public class SslAuthenticationException extends AuthenticationException {

    private static final long serialVersionUID = 1L;

    public SslAuthenticationException(String message) {
        super(message);
    }

    public SslAuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

}
