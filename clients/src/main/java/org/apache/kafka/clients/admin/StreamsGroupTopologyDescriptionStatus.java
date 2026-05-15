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

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Status of a streams group topology description on a {@link StreamsGroupDescription}.
 *
 * <p>Reported alongside {@link StreamsGroupDescription#topologyDescription()}: when
 * the description is present, the status is {@link #AVAILABLE}; when it is absent,
 * the status reports the reason.
 */
@InterfaceStability.Evolving
public enum StreamsGroupTopologyDescriptionStatus {
    /**
     * A topology description is present on this {@code StreamsGroupDescription}.
     */
    AVAILABLE,

    /**
     * The caller did not request a topology description (i.e.
     * {@code includeTopologyDescription(false)}).
     */
    NOT_REQUESTED,

    /**
     * The caller requested a topology description, but no description has been
     * recorded for this group — for example, the broker has no topology description
     * plugin configured, or no client has pushed a description yet for the current
     * topology version.
     */
    NOT_STORED,

    /**
     * The broker failed to retrieve the topology description for this group; check
     * broker logs.
     */
    ERROR;

    /**
     * Maps the wire-level {@code TopologyDescriptionStatus} int8 onto this enum.
     * The wire encoding is: 0=NOT_REQUESTED, 1=NOT_STORED, 2=ERROR, 3=AVAILABLE.
     * The broker is required to set 3 (AVAILABLE) whenever it attaches a topology
     * description, so for a non-null description this method should always observe 3.
     */
    public static StreamsGroupTopologyDescriptionStatus fromWire(boolean topologyPresent, byte wireStatus) {
        switch (wireStatus) {
            case 0: return NOT_REQUESTED;
            case 1: return NOT_STORED;
            case 2: return ERROR;
            case 3: return AVAILABLE;
            default: return ERROR;
        }
    }
}
