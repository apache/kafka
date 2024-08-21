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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShard;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;

/**
 * The share coordinator shard is a replicated state machine that manages the metadata of all
 * share partitions. It holds the hard and the soft state of the share partitions. This class
 * has two kinds of methods:
 * 1) The request handlers which handle the requests and generate a response and records to
 *    mutate the hard state. Those records will be written by the runtime and applied to the
 *    hard state via the replay methods.
 * 2) The replay methods which apply records to the hard state. Those are used in the request
 *    handling as well as during the initial loading of the records from the partitions.
 */
public class ShareCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {
    @Override
    public void onLoaded(MetadataImage newImage) {
        CoordinatorShard.super.onLoaded(newImage);
    }

    @Override
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        CoordinatorShard.super.onNewMetadataImage(newImage, delta);
    }

    @Override
    public void onUnloaded() {
        CoordinatorShard.super.onUnloaded();
    }

    @Override
    public void replay(long offset, long producerId, short producerEpoch, CoordinatorRecord record) throws RuntimeException {
    }

    @Override
    public void replayEndTransactionMarker(long producerId, short producerEpoch, TransactionResult result) throws RuntimeException {
        CoordinatorShard.super.replayEndTransactionMarker(producerId, producerEpoch, result);
    }

    public CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> writeState(RequestContext context, WriteShareGroupStateRequestData request) {
        throw new RuntimeException("Not implemented");
    }

    public ReadShareGroupStateResponseData readState(ReadShareGroupStateRequestData request, Long offset) {
        throw new RuntimeException("Not implemented");
    }
}
