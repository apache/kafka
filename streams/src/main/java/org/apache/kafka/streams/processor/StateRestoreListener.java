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

package org.apache.kafka.streams.processor;

/**
 * Class for listening to various states of the restoration process of a StateStore.
 *
 * When using a StateRestoreListener for reporting all state store recovery progress in an application,
 * it is expected that the StateRestoreListener is stateless as a single instance is shared
 * across all {@link org.apache.kafka.streams.processor.internals.StreamThread} instances.
 *
 * Users desiring stateful operations will need to provide synchronization internally in
 * the StateRestorerListener implementation.
 *
 * When used for monitoring a single {@link StateStore} using either {@link AbstractNotifyingRestoreCallback} or
 * {@link AbstractBatchingRestoreCallback} no synchronization is necessary
 * as each StreamThread has it's own StateStore instance.
 *
 * Incremental updates are exposed so users can estimate how much progress has been made.
 */
public interface StateRestoreListener {

    /**
     * Method called at the very beginning of the restoration of a StateStore
     *
     * @param storeName      the name of the store undergoing restoration.
     * @param startingOffset the starting offset of the entire restoration process.
     * @param endingOffset   the ending offset of the entire restoration process.
     */
    void onRestoreStart(String storeName, long startingOffset, long endingOffset);

    /**
     * Method called after a batch of records has been restored.  In this case the size of the batch is whatever the
     * value of the MAX_POLL_RECORDS is set to.
     *
     * This method is called after each restored batch and it is advised to keep processing to a minimum.
     * Any heavy processing will hold up restoring the next batch, hence slowing down the restoration process as a
     * whole.
     *
     * If you need to do any extended processing or connecting to an external service consider doing so asynchronously.
     *
     * @param storeName the name of the store undergoing restoration.
     * @param batchEndOffset the ending offset for the current restored batch.
     * @param numRestored the total number of records restored in this batch.
     */
    void onBatchRestored(String storeName, long batchEndOffset, long numRestored);

    /**
     * Method called when restoration of the StateStore is complete.
     *
     * @param storeName the name of the store just restored.
     * @param totalRestored the total number of records restored
     */
    void onRestoreEnd(String storeName, long totalRestored);

}
