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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Representation of all SASL/SCRAM credentials associated with a user that can be retrieved.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API">KIP-554: Add Broker-side SCRAM Config API</a>
 */
public class UserScramCredentialsDescription {
    private final String name;
    private final List<ScramCredentialInfo> infos;

    /**
     *
     * @param name the required user name
     * @param infos the required SASL/SCRAM credential representations for the user
     */
    public UserScramCredentialsDescription(String name, List<ScramCredentialInfo> infos) {
        this.name = Objects.requireNonNull(name);
        this.infos = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(infos)));
    }

    /**
     *
     * @return the user name
     */
    public String getName() {
        return name;
    }

    /**
     *
     * @return the unmodifiable list of SASL/SCRAM credential representations for the user
     */
    public List<ScramCredentialInfo> getInfos() {
        return infos;
    }
}
