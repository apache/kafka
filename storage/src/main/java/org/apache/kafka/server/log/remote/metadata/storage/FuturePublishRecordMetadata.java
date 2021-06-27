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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class represents the {@link Future} implementation for {@link TopicBasedRemoteLogMetadataManager}
 * <ul>
 *     <li> publishing the {@link org.apache.kafka.server.log.remote.storage.RemoteLogMetadata}, and </li>
 *     <li> consuming these messages from remote log metadata topic for adding them to its internal store, that makes
 *     available for fetching these remote log metadata with APIs like {@link TopicBasedRemoteLogMetadataManager#remoteLogSegmentMetadata(TopicIdPartition, int, long)} </li>
 * </ul>
 * <br>
 * This class instance is returned with APIs like
 * <ul>
 *     <li>{@link TopicBasedRemoteLogMetadataManager#addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata)}</li>
 *     <li>{@link TopicBasedRemoteLogMetadataManager#updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate)}</li>
 *     <li>{@link TopicBasedRemoteLogMetadataManager#putRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata)}</li>
 * </ul>
 */
class FuturePublishRecordMetadata implements Future<Void> {
    private static final Logger log = LoggerFactory.getLogger(FuturePublishRecordMetadata.class.getName());

    private final Future<RecordMetadata> recordMetadataFuture;
    private final ConsumerManager consumerManager;
    private final Time time;

    public FuturePublishRecordMetadata(Future<RecordMetadata> recordMetadataFuture,
                                       ConsumerManager consumerManager,
                                       Time time) {
        this.recordMetadataFuture = recordMetadataFuture;
        this.consumerManager = consumerManager;
        this.time = time;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return recordMetadataFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return recordMetadataFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        if (!recordMetadataFuture.isDone()) {
            // If the recordMetadata is not yet available, return false.
            return false;
        }

        try {
            // If the recordMetadata is already available, check whether consumer was able to caught up until that
            // recordMetadata's offset or not.
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            consumerManager.waitTillConsumptionCatchesUp(recordMetadata, 0);
            return true;
        } catch (TimeoutException e) {
            return false;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        log.debug("Waiting for record metadata to be available after produce: [{}]", recordMetadataFuture);

        // Wait for the published record metadata to be available.
        RecordMetadata recordMetadata = recordMetadataFuture.get();

        log.debug("Record metadata is available after produce: [{}]. Waiting for consumer to catchup until this offset", recordMetadata);
        try {
            // Wait until the consumer catches up with the recordMetadata's offset.
            consumerManager.waitTillConsumptionCatchesUp(recordMetadata);
        } catch (TimeoutException e) {
            throw new ExecutionException(e);
        }
        
        return null;
    }

    @Override
    public Void get(long timeout,
                    TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long startMs = time.milliseconds();
        log.debug("Waiting for record metadata to be available after produce: [{}]", recordMetadataFuture);

        // Wait for the published record metadata to be available.
        RecordMetadata recordMetadata = recordMetadataFuture.get(timeout, unit);
        if (time.milliseconds() - startMs >= timeout) {
            throw new TimeoutException("Timed out in " + timeout + "ms");
        }

        long remainingMs = time.milliseconds() - startMs;
        log.debug("Record metadata is available after produce: [{}]. Waiting for consumer to catchup until this offset", recordMetadata);
        // Wait until the consumer catches up with the recordMetadata's offset.
        consumerManager.waitTillConsumptionCatchesUp(recordMetadata, remainingMs);

        return null;
    }
}
