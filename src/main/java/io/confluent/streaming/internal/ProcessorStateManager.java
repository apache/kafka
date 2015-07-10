/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.streaming.internal;

import io.confluent.streaming.RecordCollector;
import io.confluent.streaming.StorageEngine;
import io.confluent.streaming.util.OffsetCheckpoint;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
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
    private final Map<String, StorageEngine> stores;
    private final Map<TopicPartition, Long> checkpointedOffsets;
    private final Map<TopicPartition, Long> restoredOffsets;

    public ProcessorStateManager(int id, File baseDir) {
        this.id = id;
        this.baseDir = baseDir;
        this.stores = new HashMap<String, StorageEngine>();
        this.checkpointedOffsets = new HashMap<TopicPartition, Long>();
        this.restoredOffsets = new HashMap<TopicPartition, Long>();
    }

    public File baseDir() {
        return this.baseDir;
    }

    public void init() throws IOException {
        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(this.baseDir, CHECKPOINT_FILE_NAME));
        this.checkpointedOffsets.putAll(checkpoint.read());
        checkpoint.delete();
    }

    public void registerAndRestore(RecordCollector<byte[], byte[]> collector, Consumer<byte[], byte[]> consumer, StorageEngine engine) {
        if (engine.name().equals(CHECKPOINT_FILE_NAME))
            throw new IllegalArgumentException("Illegal store name: " + CHECKPOINT_FILE_NAME);

        if(this.stores.containsKey(engine.name()))
            throw new IllegalArgumentException("Store " + engine.name() + " has already been registered.");

        // register store
        this.stores.put(engine.name(), engine);

        TopicPartition storePartition = new TopicPartition(engine.name(), id);
        consumer.subscribe(storePartition);

        // calculate the end offset of the partition
        consumer.seekToEnd(storePartition);
        long partitionEndOffset = consumer.position(storePartition);

        // what was the last-written offset when we shutdown last?
        long checkpointedOffset = 0;
        if(checkpointedOffsets.containsKey(storePartition))
            checkpointedOffset = checkpointedOffsets.get(storePartition);

        // restore
        consumer.subscribe(storePartition);
        consumer.seekToBeginning(storePartition);
        engine.registerAndRestore(collector, consumer, storePartition, checkpointedOffset, partitionEndOffset);
        consumer.unsubscribe(storePartition);
        restoredOffsets.put(storePartition, partitionEndOffset);
    }

    public void cleanup() throws IOException {
        // clean up any unknown files in the state directory
        for(File file: this.baseDir.listFiles()) {
            if(!this.stores.containsKey(file.getName())) {
                log.info("Deleting state directory {}", file.getAbsolutePath());
                file.delete();
            }
        }
    }

    public void flush() {
        if(!this.stores.isEmpty()) {
            log.debug("Flushing stores.");
            for (StorageEngine engine : this.stores.values())
                engine.flush();
        }
    }

    public void close(Map<TopicPartition, Long> ackedOffsets) throws IOException {
        if(!stores.isEmpty()) {
            log.debug("Closing stores.");
            for (Map.Entry<String, StorageEngine> entry : stores.entrySet()) {
                log.debug("Closing storage engine {}", entry.getKey());
                entry.getValue().flush();
                entry.getValue().close();
            }

            Map<TopicPartition, Long> checkpointOffsets = new HashMap<TopicPartition, Long>(restoredOffsets);
            for(String storeName: stores.keySet()) {
                TopicPartition part = new TopicPartition(storeName, id);
                if(ackedOffsets.containsKey(part))
                    // store the last ack'd offset + 1 (the log position after restoration)
                    checkpointOffsets.put(part, ackedOffsets.get(part) + 1);
            }

            // record that shutdown was clean
            OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(this.baseDir, CHECKPOINT_FILE_NAME));
            checkpoint.write(checkpointOffsets);
        }
    }

}
