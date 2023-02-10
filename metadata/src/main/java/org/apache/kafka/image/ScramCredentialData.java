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

package org.apache.kafka.image;

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;

import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Objects;


/**
 * Represents the ACLs in the metadata image.
 *
 * This class is thread-safe.
 */
public final class ScramCredentialData {
    private final byte[] salt;
    private final byte[] saltedPassword;
    private final int iterations;

    static ScramCredentialData fromRecord(
        UserScramCredentialRecord record
    ) {
        return new ScramCredentialData(
                record.salt(),
                record.saltedPassword(),
                record.iterations());
    }

    public ScramCredentialData(
        byte[] salt,
        byte[] saltedPassword,
        int iterations
    ) {
        this.salt = salt;
        this.saltedPassword = saltedPassword;
        this.iterations = iterations;
    }

    public byte[] salt() {
        return salt;
    }

    public byte[] saltedPassword() {
        return saltedPassword;
    }

    public int iterations() {
        return iterations;
    }

    public UserScramCredentialRecord toRecord(
        String userName,
        ScramMechanism mechanism
    ) {
        return new UserScramCredentialRecord().
                setName(userName).
                setMechanism(mechanism.type()).
                setSalt(salt).
                setSaltedPassword(saltedPassword).
                setIterations(iterations);
    }

    public ScramCredential toCredential(
        ScramMechanism mechanism
    ) throws GeneralSecurityException {
        org.apache.kafka.common.security.scram.internals.ScramMechanism internalMechanism =
                org.apache.kafka.common.security.scram.internals.ScramMechanism.forMechanismName(mechanism.mechanismName());
        ScramFormatter formatter = new ScramFormatter(internalMechanism);
        return new ScramCredential(salt,
                formatter.storedKey(formatter.clientKey(saltedPassword)),
                formatter.serverKey(saltedPassword),
                iterations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(salt, saltedPassword, iterations);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!o.getClass().equals(ScramCredentialData.class)) return false;
        ScramCredentialData other = (ScramCredentialData) o;
        return Arrays.equals(salt, other.salt) &&
                Arrays.equals(saltedPassword, other.saltedPassword) &&
                iterations == other.iterations;
    }

    @Override
    public String toString() {
        return "ScramCredentialData" +
            "(salt=" + "[hidden]" +
            ", saltedPassword=" + "[hidden]" +
            ", iterations=" + "[hidden]" +
            ")";
    }
}
