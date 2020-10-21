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

import java.util.Objects;

/**
 * A request to delete a SASL/SCRAM credential for a user.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API">KIP-554: Add Broker-side SCRAM Config API</a>
 */
public class UserScramCredentialDeletion extends UserScramCredentialAlteration {
    private final ScramMechanism mechanism;

    /**
     * @param user the mandatory user
     * @param mechanism the mandatory mechanism
     */
    public UserScramCredentialDeletion(String user, ScramMechanism mechanism) {
        super(user);
        this.mechanism = Objects.requireNonNull(mechanism);
    }

    /**
     *
     * @return the always non-null mechanism
     */
    public ScramMechanism mechanism() {
        return mechanism;
    }
}
