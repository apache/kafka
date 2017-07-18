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
 * Abstract implementation of the  {@link StateRestoreCallback} used for batch restoration operations.
 *
 * Includes default no-op methods of the {@link StateRestoreListener} {@link #onRestoreStart(String, long, long)},
 * {@link #onBatchRestored(String, long, long)}, and {@link #onRestoreEnd(String, long)}.
 */
public abstract class AbstractNotifyingRestoreCallback implements StateRestoreCallback, StateRestoreListener {


    /**
     * Method called at the very beginning of the restoration of a StateStore
     * <p>
     * This method does nothing by default; if desired, subclasses should override it with custom functionality.
     * </p>
     *
     * @param storeName      the name of the store undergoing restoration.
     * @param startingOffset the starting offset of the entire restoration process.
     * @param endingOffset   the ending offset of the entire restoration process.
     */
    @Override
    public void onRestoreStart(String storeName, long startingOffset, long endingOffset) {

    }

    /**
     * Method called after a batch of records has been restored.  In this case the size of the batch is whatever the
     * value of the MAX_POLL_RECORDS is set to.
     *
     * This method is called after each restored batch and it is advised to keep processing to a minimum.
     * Any heavy processing will hold up restoring the next batch, hence slowing down the restoration process as a
     * whole.
     *
     * If you need to do any extended processing or connecting to an external service consider doing so asynchronously.
     * <p>
     * This method does nothing by default; if desired, subclasses should override it with custom functionality.
     * </p>
     *
     * @param storeName the name of the store undergoing restoration.
     * @param batchEndOffset the ending offset for the current restored batch.
     * @param numRestored the total number of records restored in this batch.
     */

    @Override
    public void onBatchRestored(String storeName, long batchEndOffset, long numRestored) {

    }

    /**
     * Method called when restoration of the StateStore is complete.
     * <p>
     * This method does nothing by default; if desired, subclasses should override it with custom functionality.
     * </p>
     *
     * @param storeName the name of the store just restored.
     * @param totalRestored the total number of records restored
     */
    @Override
    public void onRestoreEnd(String storeName, long totalRestored) {

    }
}
