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
package org.apache.kafka.common.errors;

/**
 * This retriable exception indicates that remote storage is not ready to receive the requests yet.
 * This exception will be thrown only when using the
 * {@code org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager} as the implementation
 * for {@code org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataManager}.
 *
 * <ul>
 *     <li>The consumer reads data from a known offset. If there's no initial offset, then it's determined by the
 *     {@link org.apache.kafka.clients.consumer.ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} configuration which can be
 *     set to earliest, latest, or none.</li>
 *     <li>If the auto.offset.reset is set to earliest and the offset is in remote storage, then the consumer FETCH
 *     request can't make progress until the remote log metadata is synced.</li>
 *     <li>In a FETCH request, if there are multiple partitions with some fetching from local and others from remote
 *     storage, only the partitions fetching from remote storage will be blocked. The ones fetching from local
 *     storage can make progress.</li>
 *     <li>If the fetch-offset for a partition is within local storage, then the consumer can fetch the messages.</li>
 *     <li>All calls to LIST_OFFSETS will fail until the remote log metadata gets synced for that partition.</li>
 * </ul>
 * The behavior ensures that the consumer can continue to consume messages from local storage even if the remote
 * storage is not available or not yet synced. However, it also means that the consumer can't consume messages from
 * remote storage until the remote log metadata gets synced.
 */
public class RemoteStorageNotReadyException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public RemoteStorageNotReadyException(String message) {
        super(message);
    }

    public RemoteStorageNotReadyException(String message, Throwable cause) {
        super(message, cause);
    }

    public RemoteStorageNotReadyException(Throwable cause) {
        super(cause);
    }

}
