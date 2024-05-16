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
package org.apache.kafka.streams.processor.internals.assignment;

public enum AssignorError {
    // Note: this error code should be reserved for fatal errors, as the receiving clients are future-proofed
    // to throw an exception upon an unrecognized error code.
    NONE(0, "NONE", "NONE"),
    INCOMPLETE_SOURCE_TOPIC_METADATA(1, "INCOMPLETE_SOURCE_TOPIC_METADATA", "Missing metadata for source topics. Check the group leader logs for details."),
    VERSION_PROBING(2, "VERSION_PROBING", "Could not read internal rebalance metadata due to unknown encoding version."), // not actually used anymore, but we may hit it during a rolling upgrade from earlier versions
    ASSIGNMENT_ERROR(3, "ASSIGNMENT_ERROR", "Internal task assignment error. Check the group leader logs for details."),
    SHUTDOWN_REQUESTED(4, "SHUTDOWN_REQUESTED", "A KafkaStreams instance encountered a fatal error and requested a shutdown for the entire application.");

    private final int code;
    private final String codeName;
    private final String description;

    AssignorError(final int code, final String codeName, final String description) {
        this.code = code;
        this.codeName = codeName;
        this.description = description;
    }

    public int code() {
        return code;
    }

    public String codeName() {
        return codeName;
    }
    public String description() {
        return description;
    }

}
