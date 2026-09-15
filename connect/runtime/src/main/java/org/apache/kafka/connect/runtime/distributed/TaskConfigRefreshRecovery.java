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
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import jakarta.ws.rs.core.Response;

final class TaskConfigRefreshRecovery {

    // Allow the exponential backoff to reach its 60-second cap before requesting a rejoin.
    static final int MAX_RETRIES = 9;

    private final Map<String, Attempt> activeAttempts = new HashMap<>();

    synchronized Attempt begin(String connector, ExtendedAssignment assignment) {
        if (assignment == null) {
            return null;
        }
        Attempt attempt = Attempt.from(assignment);
        if (Objects.equals(activeAttempts.get(connector), attempt)) {
            return null;
        }
        activeAttempts.put(connector, attempt);
        return attempt;
    }

    synchronized void complete(String connector, Attempt attempt) {
        activeAttempts.remove(connector, attempt);
    }

    synchronized void prune(ExtendedAssignment assignment) {
        activeAttempts.entrySet().removeIf(entry -> !entry.getValue().matches(assignment)
                || assignment.tasks().stream().noneMatch(id -> id.connector().equals(entry.getKey())));
    }

    synchronized boolean isActive(String connector, Attempt attempt, ExtendedAssignment assignment) {
        boolean active = Objects.equals(activeAttempts.get(connector), attempt)
                && assignment != null
                && attempt.matches(assignment)
                && assignment.tasks().stream().anyMatch(id -> id.connector().equals(connector));
        if (!active) {
            activeAttempts.remove(connector, attempt);
        }
        return active;
    }

    static boolean requiresRejoin(Throwable error) {
        if (error instanceof RebalanceNeededException
                || error instanceof NotFoundException
                || error instanceof BadRequestException) {
            return true;
        }
        if (error instanceof ConnectRestException restError) {
            return restError.statusCode() == Response.Status.BAD_REQUEST.getStatusCode()
                    || restError.statusCode() == Response.Status.NOT_FOUND.getStatusCode()
                    || restError.statusCode() == Response.Status.CONFLICT.getStatusCode();
        }
        return false;
    }

    record Attempt(long configOffset, String leader, String leaderUrl) {
        static Attempt from(ExtendedAssignment assignment) {
            return new Attempt(assignment.offset(), assignment.leader(), assignment.leaderUrl());
        }

        boolean matches(ExtendedAssignment assignment) {
            return configOffset == assignment.offset()
                    && Objects.equals(leader, assignment.leader())
                    && Objects.equals(leaderUrl, assignment.leaderUrl());
        }
    }
}
