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
package org.apache.kafka.coordinator.group.api.streams;

import org.apache.kafka.common.errors.ApiException;

/**
 * Signalled by a {@link StreamsGroupTopologyDescriptionPlugin} to indicate that the
 * push failed for a transient reason and may succeed on a future attempt. The
 * broker arms or extends the per-group back-off and re-solicits the push on a later
 * heartbeat. The caller receives {@code STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED}
 * with this exception's message. Plugins that throw any other (non-permanent)
 * exception are treated identically; this class is provided as the canonical signal.
 */
public class StreamsTopologyDescriptionTransientFailureException extends ApiException {

    private static final long serialVersionUID = 1L;

    public StreamsTopologyDescriptionTransientFailureException(String message) {
        super(message);
    }

    public StreamsTopologyDescriptionTransientFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
