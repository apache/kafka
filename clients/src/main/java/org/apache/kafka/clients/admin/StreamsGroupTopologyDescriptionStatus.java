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

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The status of the topology description that accompanies a {@link StreamsGroupDescription}.
 * <p>
 * The status indicates whether a topology description was requested and, if so, whether one could
 * be retrieved. It is paired with {@link StreamsGroupDescription#topologyDescription()}: the
 * description is present if and only if the status is {@link #AVAILABLE}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum StreamsGroupTopologyDescriptionStatus {

    /**
     * The topology description was not requested (the caller did not set
     * {@link DescribeStreamsGroupsOptions#includeTopologyDescription(boolean)}).
     */
    NOT_REQUESTED((byte) 0),

    /**
     * No topology description is recorded for this group, for example because no topology description
     * plugin is configured on the broker or the client has not pushed a description yet.
     */
    NOT_STORED((byte) 1),

    /**
     * The broker failed to fetch the topology description. See the broker logs for details.
     */
    ERROR((byte) 2),

    /**
     * The topology description is available and carried in {@link StreamsGroupDescription#topologyDescription()}.
     */
    AVAILABLE((byte) 3);

    private final byte id;

    StreamsGroupTopologyDescriptionStatus(final byte id) {
        this.id = id;
    }

    /**
     * The wire identifier of this status.
     */
    public byte id() {
        return id;
    }

    /**
     * Returns the status corresponding to the given wire identifier.
     *
     * @param id the wire identifier.
     * @return the matching status.
     * @throws IllegalArgumentException if the identifier is unknown.
     */
    public static StreamsGroupTopologyDescriptionStatus forId(final byte id) {
        for (final StreamsGroupTopologyDescriptionStatus status : values()) {
            if (status.id == id) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown topology description status id: " + id);
    }
}
