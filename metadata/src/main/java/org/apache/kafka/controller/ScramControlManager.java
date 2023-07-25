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

package org.apache.kafka.controller;

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialDeletion;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialUpsertion;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult;
import org.apache.kafka.common.metadata.RemoveUserScramCredentialRecord;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static org.apache.kafka.common.protocol.Errors.DUPLICATE_RESOURCE;
import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.common.protocol.Errors.RESOURCE_NOT_FOUND;
import static org.apache.kafka.common.protocol.Errors.UNSUPPORTED_VERSION;
import static org.apache.kafka.common.protocol.Errors.UNACCEPTABLE_CREDENTIAL;
import static org.apache.kafka.common.protocol.Errors.UNSUPPORTED_SASL_MECHANISM;


/**
 * Manages SCRAM credentials.
 */
public class ScramControlManager {
    static final int MAX_ITERATIONS = 16384;

    static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        ScramControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            return new ScramControlManager(logContext,
                snapshotRegistry);
        }
    }

    static class ScramCredentialKey {
        private final String username;
        private final ScramMechanism mechanism;

        ScramCredentialKey(String username, ScramMechanism mechanism) {
            this.username = username;
            this.mechanism = mechanism;
        }

        @Override
        public int hashCode() {
            return Objects.hash(username, mechanism);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) return false;
            if (!(o.getClass() == this.getClass())) return false;
            ScramCredentialKey other = (ScramCredentialKey) o;
            return username.equals(other.username) &&
                mechanism.equals(other.mechanism);
        }

        @Override
        public String toString() {
            return "ScramCredentialKey" +
                "(username=" + username +
                ", mechanism=" + mechanism +
                ")";
        }
    }

    static class ScramCredentialValue {
        private final byte[] salt;
        private final byte[] storedKey;
        private final byte[] serverKey;
        private final int iterations;

        ScramCredentialValue(
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

        @Override
        public int hashCode() {
            return Objects.hash(salt, storedKey, serverKey, iterations);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) return false;
            if (!(o.getClass() == this.getClass())) return false;
            ScramCredentialValue other = (ScramCredentialValue) o;
            return Arrays.equals(salt, other.salt) &&
                Arrays.equals(storedKey, other.storedKey) &&
                Arrays.equals(serverKey, other.serverKey) &&
                iterations == other.iterations;
        }

        @Override
        public String toString() {
            return "ScramCredentialValue" +
                "(salt=" + "[hidden]" +
                ", storedKey=" + "[hidden]" +
                ", serverKey=" + "[hidden]" +
                ", iterations=" + "[hidden]" +
                ")";
        }
    }

    private final Logger log;
    private final TimelineHashMap<ScramCredentialKey, ScramCredentialValue> credentials;

    private ScramControlManager(
        LogContext logContext,
        SnapshotRegistry snapshotRegistry
    ) {
        this.log = logContext.logger(ScramControlManager.class);
        this.credentials = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /*
     * Pass in the MetadataVersion so that we can return a response to the caller 
     * if the current metadataVersion is too low.
     */
    public ControllerResult<AlterUserScramCredentialsResponseData> alterCredentials(
        AlterUserScramCredentialsRequestData request,
        MetadataVersion metadataVersion
    ) {
        boolean scramIsSupported = metadataVersion.isScramSupported();
        Map<String, ScramCredentialDeletion> userToDeletion = new HashMap<>();
        Map<String, ScramCredentialUpsertion> userToUpsert = new HashMap<>();
        Map<String, ApiError> userToError = new HashMap<>();

        for (ScramCredentialDeletion deletion : request.deletions()) {
            if (!userToError.containsKey(deletion.name())) {
                if (userToDeletion.remove(deletion.name()) != null) {
                    userToError.put(deletion.name(), new ApiError(DUPLICATE_RESOURCE,
                        "A user credential cannot be altered twice in the same request"));
                } else {
                    if (!scramIsSupported) {
                        userToError.put(deletion.name(), new ApiError(UNSUPPORTED_VERSION,
                            "The current metadata version does not support SCRAM"));
                    } else {
                        ApiError error = validateDeletion(deletion);
                        if (error.isFailure()) {
                            userToError.put(deletion.name(), error);
                        } else {
                            userToDeletion.put(deletion.name(), deletion);
                        }
                    }
                }
            }
        }
        for (ScramCredentialUpsertion upsertion : request.upsertions()) {
            if (!userToError.containsKey(upsertion.name())) {
                if (userToDeletion.remove(upsertion.name()) != null ||
                        userToUpsert.remove(upsertion.name()) != null) {
                    userToError.put(upsertion.name(), new ApiError(DUPLICATE_RESOURCE,
                        "A user credential cannot be altered twice in the same request"));
                } else {
                    if (!scramIsSupported) {
                        userToError.put(upsertion.name(), new ApiError(UNSUPPORTED_VERSION,
                            "The current metadata version does not support SCRAM"));
                    } else {
                        ApiError error = validateUpsertion(upsertion);
                        if (error.isFailure()) {
                            userToError.put(upsertion.name(), error);
                        } else {
                            userToUpsert.put(upsertion.name(), upsertion);
                        }
                    }
                }
            }
        }
        AlterUserScramCredentialsResponseData response = new AlterUserScramCredentialsResponseData();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (ScramCredentialDeletion deletion : userToDeletion.values()) {
            response.results().add(new AlterUserScramCredentialsResult().
                setUser(deletion.name()).
                setErrorCode(NONE.code()).
                setErrorMessage(null));
            records.add(new ApiMessageAndVersion(new RemoveUserScramCredentialRecord().
                setName(deletion.name()).
                setMechanism(deletion.mechanism()), (short) 0));
        }
        for (ScramCredentialUpsertion upsertion : userToUpsert.values()) {
            ApiError error = finishUpsertion(records, upsertion);
            if (!error.isFailure()) {
                response.results().add(new AlterUserScramCredentialsResult().
                    setUser(upsertion.name()).
                    setErrorCode(NONE.code()).
                    setErrorMessage(null));
            } else {
                userToError.put(upsertion.name(), error);
            }
        }
        for (Entry<String, ApiError> entry : userToError.entrySet()) {
            response.results().add(new AlterUserScramCredentialsResult().
                setUser(entry.getKey()).
                setErrorCode(entry.getValue().error().code()).
                setErrorMessage(entry.getValue().message()));
        }
        return ControllerResult.atomicOf(records, response);
    }

    static ApiError finishUpsertion(List<ApiMessageAndVersion> records, ScramCredentialUpsertion upsertion) {
        org.apache.kafka.common.security.scram.internals.ScramMechanism internalMechanism = 
                org.apache.kafka.common.security.scram.internals.ScramMechanism.forMechanismName(
                ScramMechanism.fromType(upsertion.mechanism()).mechanismName());

        try { // Convert from saltedPassword to storedKey and serverKey
            ScramFormatter formatter = new ScramFormatter(internalMechanism);

            records.add(new ApiMessageAndVersion(new UserScramCredentialRecord().
                setName(upsertion.name()).
                setMechanism(upsertion.mechanism()).
                setSalt(upsertion.salt()).

                // Convert from saltedPassword to storedKey and serverKey
                setStoredKey(formatter.storedKey(formatter.clientKey(upsertion.saltedPassword()))).
                setServerKey(formatter.serverKey(upsertion.saltedPassword())).
                setIterations(upsertion.iterations()), (short) 0));

        } catch (Throwable e) {
            return ApiError.fromThrowable(e);
        }
        return ApiError.NONE;
    }

    static ApiError validateUpsertion(ScramCredentialUpsertion upsertion) {
        ScramMechanism mechanism = ScramMechanism.fromType(upsertion.mechanism());
        ApiError error = validateScramUsernameAndMechanism(upsertion.name(), mechanism);
        if (error.isFailure()) return error;
        org.apache.kafka.common.security.scram.internals.ScramMechanism internalMechanism =
            org.apache.kafka.common.security.scram.internals.ScramMechanism.forMechanismName(mechanism.mechanismName());
        if (upsertion.iterations() < internalMechanism.minIterations()) {
            return new ApiError(UNACCEPTABLE_CREDENTIAL, "Too few iterations");
        } else if (upsertion.iterations() > MAX_ITERATIONS) {
            return new ApiError(UNACCEPTABLE_CREDENTIAL, "Too many iterations");
        }
        return ApiError.NONE;
    }

    ApiError validateDeletion(ScramCredentialDeletion deletion) {
        ApiError error = validateScramUsernameAndMechanism(deletion.name(),
            ScramMechanism.fromType(deletion.mechanism()));
        if (error.isFailure()) return error;
        ScramCredentialKey key = new ScramCredentialKey(deletion.name(),
            ScramMechanism.fromType(deletion.mechanism()));
        if (!credentials.containsKey(key)) {
            return new ApiError(RESOURCE_NOT_FOUND,
                "Attempt to delete a user credential that does not exist");
        }
        return ApiError.NONE;
    }

    static ApiError validateScramUsernameAndMechanism(
        String username,
        ScramMechanism mechanism
    ) {
        if (username.isEmpty()) {
            return new ApiError(UNACCEPTABLE_CREDENTIAL, "Username must not be empty");
        }
        if (mechanism == ScramMechanism.UNKNOWN) {
            return new ApiError(UNSUPPORTED_SASL_MECHANISM, "Unknown SCRAM mechanism");
        }
        return ApiError.NONE;
    }

    public void replay(RemoveUserScramCredentialRecord record) {
        ScramCredentialKey key = new ScramCredentialKey(record.name(),
            ScramMechanism.fromType(record.mechanism()));
        if (credentials.remove(key) == null) {
            throw new RuntimeException("Unable to find credential to delete: " + key);
        }
        log.info("Replayed RemoveUserScramCredentialRecord for {} with mechanism {}.",
            key.username, key.mechanism);
    }

    public void replay(UserScramCredentialRecord record) {
        ScramCredentialKey key = new ScramCredentialKey(record.name(),
            ScramMechanism.fromType(record.mechanism()));
        ScramCredentialValue value = new ScramCredentialValue(record.salt(),
            record.storedKey(),
            record.serverKey(),
            record.iterations());
        if (credentials.put(key, value) == null) {
            log.info("Replayed UserScramCredentialRecord creating new entry for {} with " +
                "mechanism {}.", key.username, key.mechanism);
        } else {
            log.info("Replayed UserScramCredentialRecord modifying existing entry for {} " +
                "with mechanism {}.", key.username, key.mechanism);
        }
    }

}
