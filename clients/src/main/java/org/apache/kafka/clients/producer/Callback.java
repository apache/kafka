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
     * metadata will contain the special -1 value for all fields. If topicPartition cannot be
     * chosen, a -1 value will be assigned.
     *
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset). An empty metadata
     *                 with -1 value for all fields will be returned if an error occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     *                  Possible thrown exceptions include:
     *                  <p>
     *                  Non-Retriable exceptions (fatal, the message will never be sent):
     *                  <ul>
     *                      <li>{@link org.apache.kafka.common.errors.InvalidTopicException InvalidTopicException}
     *                      <li>{@link org.apache.kafka.common.errors.OffsetMetadataTooLarge OffsetMetadataTooLarge}
     *                      <li>{@link org.apache.kafka.common.errors.RecordBatchTooLargeException RecordBatchTooLargeException}
     *                      <li>{@link org.apache.kafka.common.errors.RecordTooLargeException RecordTooLargeException}
     *                      <li>{@link org.apache.kafka.common.errors.UnknownServerException UnknownServerException}
     *                      <li>{@link org.apache.kafka.common.errors.UnknownProducerIdException UnknownProducerIdException}
     *                      <li>{@link org.apache.kafka.common.errors.InvalidProducerEpochException InvalidProducerEpochException}
     *                  </ul>
     *                  Retriable exceptions (transient, may be covered by increasing #.retries):
     *                  <ul>
     *                      <li>{@link org.apache.kafka.common.errors.CorruptRecordException CorruptRecordException}
     *                      <li>{@link org.apache.kafka.common.errors.InvalidMetadataException InvalidMetadataException}
     *                      <li>{@link org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException NotEnoughReplicasAfterAppendException}
     *                      <li>{@link org.apache.kafka.common.errors.NotEnoughReplicasException NotEnoughReplicasException}
     *                      <li>{@link org.apache.kafka.common.errors.OffsetOutOfRangeException OffsetOutOfRangeException}
     *                      <li>{@link org.apache.kafka.common.errors.TimeoutException TimeoutException}
     *                      <li>{@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException UnknownTopicOrPartitionException}
     *                  </ul>
     */
    void onCompletion(RecordMetadata metadata, Exception exception);
}
