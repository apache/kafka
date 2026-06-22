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
package org.apache.kafka.coordinator.transaction;

import org.apache.kafka.common.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;

public class TxnMarkerQueue {

    private static final Logger LOG = LoggerFactory.getLogger(TxnMarkerQueue.class);

    // keep track of the requests per txn topic partition so we can easily clear the queue
    // during partition emigration
    private final ConcurrentHashMap<Integer, BlockingQueue<PendingCompleteTxnAndMarkerEntry>> markersPerTxnTopicPartition =
        new ConcurrentHashMap<>();

    private volatile Node destination;

    public TxnMarkerQueue(Node destination) {
        this.destination = destination;
    }

    public Node destination() {
        return destination;
    }

    public void setDestination(Node destination) {
        this.destination = destination;
    }

    public Optional<BlockingQueue<PendingCompleteTxnAndMarkerEntry>> removeMarkersForTxnTopicPartition(int partition) {
        return Optional.ofNullable(markersPerTxnTopicPartition.remove(partition));
    }

    public void addMarkers(int txnTopicPartition, PendingCompleteTxnAndMarkerEntry pendingCompleteTxnAndMarker) {
        BlockingQueue<PendingCompleteTxnAndMarkerEntry> queue = markersPerTxnTopicPartition.computeIfAbsent(txnTopicPartition, partition -> {
            LOG.info("Creating new marker queue for txn partition {} to destination broker {}", txnTopicPartition, destination.id());
            return new LinkedBlockingQueue<>();
        });
        queue.add(pendingCompleteTxnAndMarker);

        if (markersPerTxnTopicPartition.get(txnTopicPartition) != queue) {
            // This could happen if the queue got removed concurrently.
            // Note that it could create an unexpected state when the queue is removed from
            // removeMarkersForTxnTopicPartition, we could have:
            //
            // 1. [addMarkers] Retrieve queue.
            // 2. [removeMarkersForTxnTopicPartition] Remove queue.
            // 3. [removeMarkersForTxnTopicPartition] Iterate over queue, but not removeMarkersForTxn because queue is empty.
            // 4. [addMarkers] Add markers to the queue.
            //
            // Now we've effectively removed the markers while transactionsWithPendingMarkers has an entry.
            //
            // While this could lead to an orphan entry in transactionsWithPendingMarkers, sending new markers
            // will fix the state, so it shouldn't impact the state machine operation.
            LOG.warn("Added {} to dead queue for txn partition {} to destination broker {}",
                pendingCompleteTxnAndMarker, txnTopicPartition, destination.id());
        }
    }

    public void forEachTxnTopicPartition(BiConsumer<Integer, BlockingQueue<PendingCompleteTxnAndMarkerEntry>> f) {
        markersPerTxnTopicPartition.forEach(f);
    }

    public int totalNumMarkers() {
        int total = 0;
        for (BlockingQueue<PendingCompleteTxnAndMarkerEntry> queue : markersPerTxnTopicPartition.values()) {
            total += queue.size();
        }
        return total;
    }

    // visible for testing
    public int totalNumMarkers(int txnTopicPartition) {
        BlockingQueue<PendingCompleteTxnAndMarkerEntry> queue = markersPerTxnTopicPartition.get(txnTopicPartition);
        return queue == null ? 0 : queue.size();
    }
}
