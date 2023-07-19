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
 * Mechanism and iterations for a SASL/SCRAM credential associated with a user.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API">KIP-554: Add Broker-side SCRAM Config API</a>
 */
public class ScramCredentialInfo {
    private final ScramMechanism mechanism;
    private final int iterations;

    /**
     *
     * @param mechanism the required mechanism
     * @param iterations the number of iterations used when creating the credential
     */
    public ScramCredentialInfo(ScramMechanism mechanism, int iterations) {
        this.mechanism = Objects.requireNonNull(mechanism);
        this.iterations = iterations;
    }

    /**
     *
     * @return the mechanism
     */
    public ScramMechanism mechanism() {
        return mechanism;
    }

    /**
     *
     * @return the number of iterations used when creating the credential
     */
    public int iterations() {
        return iterations;
    }

    @Override
    public String toString() {
        return "ScramCredentialInfo{" +
                "mechanism=" + mechanism +
                ", iterations=" + iterations +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScramCredentialInfo that = (ScramCredentialInfo) o;
        return iterations == that.iterations &&
                mechanism == that.mechanism;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mechanism, iterations);
    }
}
