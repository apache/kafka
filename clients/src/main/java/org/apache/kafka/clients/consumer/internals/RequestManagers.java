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
package org.apache.kafka.clients.consumer.internals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * {@code RequestManagers} provides a means to pass around the set of {@link RequestManager} instances in the system.
 * This allows callers to both use the specific {@link RequestManager} instance, or to iterate over the list via
 * the {@link #entries()} method.
 */
public class RequestManagers {

    public final Optional<CoordinatorRequestManager> coordinatorRequestManager;
    public final Optional<CommitRequestManager> commitRequestManager;
    private final Optional<HeartbeatRequestManager> heartbeatRequestManager;
    public final OffsetsRequestManager offsetsRequestManager;
    public final TopicMetadataRequestManager topicMetadataRequestManager;
    private final List<Optional<? extends RequestManager>> entries;

    public RequestManagers(OffsetsRequestManager offsetsRequestManager,
                           TopicMetadataRequestManager topicMetadataRequestManager,
                           Optional<CoordinatorRequestManager> coordinatorRequestManager,
                           Optional<CommitRequestManager> commitRequestManager,
                           Optional<HeartbeatRequestManager> heartbeatRequestManager) {
        this.offsetsRequestManager = requireNonNull(offsetsRequestManager,
                "OffsetsRequestManager cannot be null");
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.commitRequestManager = commitRequestManager;
        this.topicMetadataRequestManager = topicMetadataRequestManager;
        this.heartbeatRequestManager = heartbeatRequestManager;

        List<Optional<? extends RequestManager>> list = new ArrayList<>();
        list.add(coordinatorRequestManager);
        list.add(commitRequestManager);
        list.add(heartbeatRequestManager);
        list.add(Optional.of(offsetsRequestManager));
        list.add(Optional.of(topicMetadataRequestManager));
        entries = Collections.unmodifiableList(list);
    }

    public List<Optional<? extends RequestManager>> entries() {
        return entries;
    }
}
