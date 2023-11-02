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

package org.apache.kafka.metadata;

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.security.scram.ScramCredential;

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents the ACLs in the metadata image.
 *
 * This class is thread-safe.
 */
public final class ScramCredentialData {
    private final byte[] salt;
    private final byte[] storedKey;
    private final byte[] serverKey;
    private final int iterations;

    public static ScramCredentialData fromRecord(
        UserScramCredentialRecord record
    ) {
        return new ScramCredentialData(
                record.salt(),
                record.storedKey(),
                record.serverKey(),
                record.iterations());
    }

    public ScramCredentialData(
        byte[] salt,
        byte[] storedKey,
        byte[] serverKey,
        int iterations
    ) {
        this.salt = salt;
        this.storedKey = storedKey;
        this.serverKey = serverKey;
        this.iterations = iterations;
    }

    public byte[] salt() {
        return salt;
    }

    public byte[] storedKey() {
        return storedKey;
    }

    public byte[] serverKey() {
        return serverKey;
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
                setStoredKey(storedKey).
                setServerKey(serverKey).
                setIterations(iterations);
    }

    public ScramCredential toCredential(ScramMechanism mechanism) {
        return new ScramCredential(salt, storedKey, serverKey, iterations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(salt, storedKey, serverKey, iterations);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!o.getClass().equals(ScramCredentialData.class)) return false;
        ScramCredentialData other = (ScramCredentialData) o;
        return Arrays.equals(salt, other.salt) &&
                Arrays.equals(storedKey, other.storedKey) &&
                Arrays.equals(serverKey, other.serverKey) &&
                iterations == other.iterations;
    }

    @Override
    public String toString() {
        return "ScramCredentialData" +
            "(salt=" + "[hidden]" +
            ", storedKey=" + "[hidden]" +
            ", serverKey=" + "[hidden]" +
            ", iterations=" + "[hidden]" +
            ")";
    }
}
