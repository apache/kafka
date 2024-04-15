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

package org.apache.kafka.server.common;

import org.apache.kafka.common.Uuid;

public interface DirectoryEventHandler {

    /**
     * A no-op implementation of {@link DirectoryEventHandler}.
     */
    DirectoryEventHandler NOOP = new DirectoryEventHandler() {
        @Override public void handleAssignment(TopicIdPartition partition, Uuid directoryId, Runnable callback) {}
        @Override public void handleFailure(Uuid directoryId) {}
    };

    /**
     * Handle the assignment of a topic partition to a directory.
     * @param directoryId  The directory ID
     * @param partition    The topic partition
     * @param callback     Callback to apply when the request is completed.
     */
    void handleAssignment(TopicIdPartition partition, Uuid directoryId, Runnable callback);

    /**
     * Handle the transition of an online log directory to the offline state.
     * @param directoryId  The directory ID
     */
    void handleFailure(Uuid directoryId);
}
