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

package org.apache.kafka.metadata.util;

import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.concurrent.CompletableFuture;


/**
 * The interface for a cluster metadata source, such as a snapshot file reader or raft observer.
 */
public interface ClusterMetadataSource extends AutoCloseable {
    /**
     * Start producing metadata.
     *
     * @param listener  The listener which should receive the metadata.
     */
    void start(RaftClient.Listener<ApiMessageAndVersion> listener) throws Exception;

    /**
     * Yields a future which is completed when we have caught up with the latest metadata. If we
     * are loading a snapshot file, this would be completed at the end of the file. If we are
     * reading from the metadata quorum, this would be completed when we have read up to the high
     * water mark.
     *
     * @return          A future which will be completed normally when we have caught up, or
     *                  completed exceptionally on error.
     */
    CompletableFuture<Void> caughtUpFuture();

    /**
     * Close the metadata source and block until all resources have been disposed of.
     */
    void close() throws Exception;

    /**
     * Returns a human-readable description of this source.
     */
    String toString();
}
