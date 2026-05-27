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

package kafka.server;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.quota.ReplicaQuota;
import org.apache.kafka.server.replica.ReplicaLogReader;
import org.apache.kafka.server.storage.log.FetchParams;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import scala.Tuple2;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

/**
 * This class implements {@link ReplicaLogReader} to proxy calls to
 * {@link ReplicaManager#readFromLog(FetchParams, Seq, ReplicaQuota, boolean)}.
 * The primary purpose is to be utilized by modules without dependency on core.
 */
public final class ReplicaLogReaderImpl implements ReplicaLogReader {
    private final ReplicaManager replicaManager;

    public ReplicaLogReaderImpl(ReplicaManager replicaManager) {
        Objects.requireNonNull(replicaManager, "replicaManager");
        this.replicaManager = replicaManager;
    }

    @Override
    public List<ReadResult> readFromLog(
        FetchParams params,
        List<ReadPartitionInfo> readPartitionInfo,
        ReplicaQuota quota,
        boolean readFromPurgatory
    ) {
        if (readPartitionInfo.isEmpty()) {
            return List.of();
        }

        List<Tuple2<TopicIdPartition, FetchRequest.PartitionData>> readInfoTup = new ArrayList<>(readPartitionInfo.size());
        readPartitionInfo.forEach(partitionInfo -> readInfoTup.add(new Tuple2<>(
            partitionInfo.topicIdPartition(), partitionInfo.partitionData())));

        var results = this.replicaManager.readFromLog(
            params,
            CollectionConverters.asScala(readInfoTup),
            quota,
            readFromPurgatory
        );

        List<ReadResult> readResults = new ArrayList<>(results.size());
        results.foreach(tup -> readResults.add(new ReadResult(tup._1, tup._2)));
        return readResults;
    }
}
