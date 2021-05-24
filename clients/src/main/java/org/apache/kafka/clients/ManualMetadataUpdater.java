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
package org.apache.kafka.clients;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A simple implementation of `MetadataUpdater` that returns the cluster nodes set via the constructor or via
 * `setNodes`.
 *
 * This is useful in cases where automatic metadata updates are not required. An example is controller/broker
 * communication.
 *
 * This class is not thread-safe!
 */
public class ManualMetadataUpdater implements MetadataUpdater {
    private List<Node> nodes;

    public ManualMetadataUpdater() {
        this(new ArrayList<>(0));
    }

    public ManualMetadataUpdater(List<Node> nodes) {
        this.nodes = nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    @Override
    public List<Node> fetchNodes() {
        return new ArrayList<>(nodes);
    }

    @Override
    public boolean isUpdateDue(long now) {
        return false;
    }

    @Override
    public long maybeUpdate(long now) {
        return Long.MAX_VALUE;
    }

    @Override
    public void handleServerDisconnect(long now, String nodeId, Optional<AuthenticationException> maybeAuthException) {
        // We don't fail the broker on failures. There should be sufficient information from
        // the NetworkClient logs to indicate the reason for the failure.
    }

    @Override
    public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
        // Do nothing
    }

    @Override
    public void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse response) {
        // Do nothing
    }

    @Override
    public void close() {
    }
}
