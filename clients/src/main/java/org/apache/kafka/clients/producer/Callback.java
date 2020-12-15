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
package org.apache.kafka.clients.producer;

/**
 * A callback interface that the user can implement to allow code to execute when the request is complete. This callback
 * will generally execute in the background I/O thread so it should be fast.
 */
public interface Callback {

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. When exception is not null in the callback,
     * metadata will contain the special -1 value for all fields except for topicPartition, which will be valid.
     *
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset). An empty metadata
     *                 with -1 value for all fields except for topicPartition will be returned if an error occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     *                  Possible thrown exceptions include:
     *
     *                  Non-Retriable exceptions (fatal, the message will never be sent):
     *
     *                  InvalidTopicException
     *                  OffsetMetadataTooLargeException
     *                  RecordBatchTooLargeException
     *                  RecordTooLargeException
     *                  UnknownServerException
     *                  UnknownProducerIdException
     *                  InvalidProducerEpochException
     *
     *                  Retriable exceptions (transient, may be covered by increasing #.retries):
     *
     *                  CorruptRecordException
     *                  InvalidMetadataException
     *                  NotEnoughReplicasAfterAppendException
     *                  NotEnoughReplicasException
     *                  OffsetOutOfRangeException
     *                  TimeoutException
     *                  UnknownTopicOrPartitionException
     */
    void onCompletion(RecordMetadata metadata, Exception exception);
}
