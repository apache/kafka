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
package kafka.server.share;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogReadResult;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

/**
 * This class is used to store the remote storage fetch information for topic partitions in a share fetch request.
 */
public class PendingRemoteFetches {
    private final List<RemoteFetch> remoteFetches;
    private final LinkedHashMap<TopicIdPartition, LogOffsetMetadata> fetchOffsetMetadataMap;

    PendingRemoteFetches(List<RemoteFetch> remoteFetches, LinkedHashMap<TopicIdPartition, LogOffsetMetadata> fetchOffsetMetadataMap) {
        this.remoteFetches = remoteFetches;
        this.fetchOffsetMetadataMap = fetchOffsetMetadataMap;
    }

    public boolean isDone() {
        for (RemoteFetch remoteFetch : remoteFetches) {
            if (!remoteFetch.remoteFetchResult.isDone())
                return false;
        }
        return true;
    }

    public void invokeCallbackOnCompletion(BiConsumer<Void, Throwable> callback) {
        List<CompletableFuture<RemoteLogReadResult>> remoteFetchResult = new ArrayList<>();
        remoteFetches.forEach(remoteFetch -> remoteFetchResult.add(remoteFetch.remoteFetchResult()));
        CompletableFuture.allOf(remoteFetchResult.toArray(new CompletableFuture<?>[0])).whenComplete(callback);
    }

    public List<RemoteFetch> remoteFetches() {
        return remoteFetches;
    }

    public LinkedHashMap<TopicIdPartition, LogOffsetMetadata> fetchOffsetMetadataMap() {
        return fetchOffsetMetadataMap;
    }

    @Override
    public String toString() {
        return "PendingRemoteFetches(" +
            "remoteFetches=" + remoteFetches +
            ", fetchOffsetMetadataMap=" + fetchOffsetMetadataMap +
            ")";
    }

    public record RemoteFetch(
        TopicIdPartition topicIdPartition,
        LogReadResult logReadResult,
        Future<Void> remoteFetchTask,
        CompletableFuture<RemoteLogReadResult> remoteFetchResult,
        RemoteStorageFetchInfo remoteFetchInfo
    ) {
        @Override
        public String toString() {
            return "RemoteFetch(" +
                "topicIdPartition=" + topicIdPartition +
                ", logReadResult=" + logReadResult +
                ", remoteFetchTask=" + remoteFetchTask +
                ", remoteFetchResult=" + remoteFetchResult +
                ", remoteFetchInfo=" + remoteFetchInfo +
                ")";
        }
    }
}
