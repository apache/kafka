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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.errors.ApiException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Representation of all SASL/SCRAM credentials associated with a user that can be retrieved, or an exception indicating
 * why credentials could not be retrieved.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API">KIP-554: Add Broker-side SCRAM Config API</a>
 */
public class UserScramCredentialsDescription {
    private final String name;
    private final Optional<ApiException> exception;
    private final List<ScramCredentialInfo> credentialInfos;

    /**
     * Constructor for when SASL/SCRAM credentials associated with a user could not be retrieved
     *
     * @param name the required user name
     * @param exception the required exception indicating why the credentials for the user could not be retrieved
     */
    public UserScramCredentialsDescription(String name, ApiException exception) {
        this(name, Optional.of(Objects.requireNonNull(exception)), Collections.emptyList());
    }

    /**
     * Constructor for when SASL/SCRAM credentials associated with a user are successfully retrieved
     *
     * @param name the required user name
     * @param credentialInfos the required SASL/SCRAM credential representations for the user
     */
    public UserScramCredentialsDescription(String name, List<ScramCredentialInfo> credentialInfos) {
        this(name, Optional.empty(), Objects.requireNonNull(credentialInfos));
    }

    private UserScramCredentialsDescription(String name, Optional<ApiException> exception, List<ScramCredentialInfo> credentialInfos) {
        this.name = Objects.requireNonNull(name);
        this.exception = Objects.requireNonNull(exception);
        this.credentialInfos = Collections.unmodifiableList(new ArrayList<>(credentialInfos));
    }

    /**
     *
     * @return the user name
     */
    public String name() {
        return name;
    }

    /**
     *
     * @return the exception, if any, that prevented the user's SASL/SCRAM credentials from being retrieved
     */
    public Optional<ApiException> exception() {
        return exception;
    }

    /**
     *
     * @return the always non-null/unmodifiable list of SASL/SCRAM credential representations for the user
     * (empty if {@link #exception} defines an exception)
     */
    public List<ScramCredentialInfo> credentialInfos() {
        return credentialInfos;
    }
}
