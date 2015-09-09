/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.RestoreFunc;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.state.OffsetCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ProcessorStateManager {

    private static final Logger log = LoggerFactory.getLogger(ProcessorStateManager.class);
    private static final String CHECKPOINT_FILE_NAME = ".checkpoint";

    private final int id;
    private final File baseDir;
    private final Map<String, StateStore> stores;
    private final Consumer<byte[], byte[]> restoreConsumer;
    private final Map<TopicPartition, Long> restoredOffsets;
    private final Map<TopicPartition, Long> checkpointedOffsets;

    public ProcessorStateManager(int id, File baseDir, Consumer<byte[], byte[]> restoreConsumer) throws IOException {
        this.id = id;
        this.baseDir = baseDir;
        this.stores = new HashMap<>();
        this.restoreConsumer = restoreConsumer;
        this.restoredOffsets = new HashMap<>();
        this.checkpointedOffsets = new HashMap<>();

        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(this.baseDir, CHECKPOINT_FILE_NAME));
        this.checkpointedOffsets.putAll(checkpoint.read());

        // delete the checkpoint file after finish loading its stored offsets
        checkpoint.delete();
    }

    public File baseDir() {
        return this.baseDir;
    }

    public void register(StateStore store, RestoreFunc restoreFunc) {
        if (store.name().equals(CHECKPOINT_FILE_NAME))
            throw new IllegalArgumentException("Illegal store name: " + CHECKPOINT_FILE_NAME);

        if (this.stores.containsKey(store.name()))
            throw new IllegalArgumentException("Store " + store.name() + " has already been registered.");

        // ---- register the store ---- //

        // check that the underlying change log topic exist or not
        if (restoreConsumer.listTopics().containsKey(store.name())) {
            boolean partitionNotFound = true;
            for (PartitionInfo partitionInfo : restoreConsumer.partitionsFor(store.name())) {
                if (partitionInfo.partition() == id) {
                    partitionNotFound = false;
                    break;
                }
            }

            if (partitionNotFound)
                throw new IllegalStateException("Store " + store.name() + "'s change log does not contain the partition for group " + id);

        } else {
            throw new IllegalStateException("Change log topic for store " + store.name() + " does not exist yet");
        }

        this.stores.put(store.name(), store);

        // ---- try to restore the state from change-log ---- //

        // subscribe to the store's partition
        TopicPartition storePartition = new TopicPartition(store.name(), id);
        if (!restoreConsumer.subscriptions().isEmpty()) {
            throw new IllegalStateException("Restore consumer should have not subscribed to any partitions beforehand");
        }
        restoreConsumer.subscribe(storePartition);

        // calculate the end offset of the partition
        // TODO: this is a bit hacky to first seek then position to get the end offset
        restoreConsumer.seekToEnd(storePartition);
        long endOffset = restoreConsumer.position(storePartition);

        // load the previously flushed state and restore from the checkpointed offset of the change log
        // if it exists in the offset file; restore the state from the beginning of the change log otherwise
        if (checkpointedOffsets.containsKey(storePartition)) {
            restoreConsumer.seek(storePartition, checkpointedOffsets.get(storePartition));
        } else {
            restoreConsumer.seekToBeginning(storePartition);
        }

        // restore its state from changelog records; while restoring the log end offset
        // should not change since it is only written by this thread.
        while (true) {
            for (ConsumerRecord<byte[], byte[]> record : restoreConsumer.poll(100)) {
                restoreFunc.apply(record.key(), record.value());
            }

            if (restoreConsumer.position(storePartition) == endOffset) {
                break;
            } else if (restoreConsumer.position(storePartition) > endOffset) {
                throw new IllegalStateException("Log end offset should not change while restoring");
            }
        }

        // record the restored offset for its change log partition
        long newOffset = restoreConsumer.position(storePartition);
        restoredOffsets.put(storePartition, newOffset);

        // un-subscribe the change log partition
        restoreConsumer.unsubscribe(storePartition);
    }

    public StateStore getStore(String name) {
        return stores.get(name);
    }

    public void cleanup() throws IOException {
        // clean up any unknown files in the state directory
        for (File file : this.baseDir.listFiles()) {
            if (!this.stores.containsKey(file.getName())) {
                log.info("Deleting state directory {}", file.getAbsolutePath());
                file.delete();
            }
        }
    }

    public void flush() {
        if (!this.stores.isEmpty()) {
            log.debug("Flushing stores.");
            for (StateStore engine : this.stores.values())
                engine.flush();
        }
    }

    public void close(Map<TopicPartition, Long> ackedOffsets) throws IOException {
        if (!stores.isEmpty()) {
            log.debug("Closing stores.");
            for (Map.Entry<String, StateStore> entry : stores.entrySet()) {
                log.debug("Closing storage engine {}", entry.getKey());
                entry.getValue().flush();
                entry.getValue().close();
            }

            Map<TopicPartition, Long> checkpointOffsets = new HashMap<TopicPartition, Long>(restoredOffsets);
            for (String storeName : stores.keySet()) {
                TopicPartition part = new TopicPartition(storeName, id);

                // only checkpoint the offset to the offsets file if it is persistent;
                if (stores.get(storeName).persistent()) {
                    if (ackedOffsets.containsKey(part))
                        // store the last ack'd offset + 1 (the log position after restoration)
                        checkpointOffsets.put(part, ackedOffsets.get(part) + 1);
                } else {
                    checkpointOffsets.remove(part);
                }
            }

            // write the checkpoint file before closing, to indicate clean shutdown
            OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(this.baseDir, CHECKPOINT_FILE_NAME));
            checkpoint.write(checkpointOffsets);
        }

        // close the restore consumer
        restoreConsumer.close();
    }

}
