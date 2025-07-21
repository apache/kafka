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

/**
 * This exception indicates a persistent authentication failure that is unlikely to succeed on retry.
 * This includes issues like expired certificates, invalid credentials, or configuration problems
 * that require manual intervention.
 * <p>
 * Unlike transient authentication failures that may succeed on retry, persistent failures
 * indicate configuration or credential issues that need to be resolved before the client
 * can successfully authenticate.
 * </p>
 */
public class PersistentAuthenticationException extends AuthenticationException {

    private static final long serialVersionUID = 1L;

    public PersistentAuthenticationException(String message) {
        super(message);
    }

    public PersistentAuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

}