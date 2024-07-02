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
package org.apache.kafka.connect.runtime.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class WorkerStatus {

    private static final WorkerStatus HEALTHY = new WorkerStatus(
            "healthy",
            "Worker has completed startup and is ready to handle requests."
    );

    private final String status;
    private final String message;

    @JsonCreator
    private WorkerStatus(
            @JsonProperty("status") String status,
            @JsonProperty("message") String message
    ) {
        this.status = status;
        this.message = message;
    }

    public static WorkerStatus healthy() {
        return HEALTHY;
    }

    public static WorkerStatus starting(String statusDetails) {
        String message = "Worker is still starting up.";
        if (statusDetails != null)
            message += " " + statusDetails;

        return new WorkerStatus(
                "starting",
                message
        );
    }

    public static WorkerStatus unhealthy(String statusDetails) {
        String message = "Worker was unable to handle this request and may be unable to handle other requests.";
        if (statusDetails != null)
            message += " " + statusDetails;

        return new WorkerStatus(
                "unhealthy",
                message
        );
    }

    @JsonProperty
    public String status() {
        return status;
    }

    @JsonProperty
    public String message() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkerStatus that = (WorkerStatus) o;
        return Objects.equals(status, that.status) && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, message);
    }

    @Override
    public String toString() {
        return "WorkerStatus{" +
                "status='" + status + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
