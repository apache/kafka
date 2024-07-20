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
package org.apache.kafka.server.log;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.ProducerAppendInfo;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.kafka.coordinator.transaction.TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_DEFAULT;
import static org.apache.kafka.storage.internals.log.ProducerStateManager.LATE_TRANSACTION_BUFFER_MS;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProducerStateManagerTest {
    
    private File logDir;
    private ProducerStateManager stateManager;
    private final TopicPartition partition = new TopicPartition("test", 0);
    private final long producerId = 1;
    private final int maxTransactionTimeoutMs = 5 * 60 * 1000; 
    private final ProducerStateManagerConfig producerStateManagerConfig = 
            new ProducerStateManagerConfig(PRODUCER_ID_EXPIRATION_MS_DEFAULT, true);
    private final long lateTransactionTimeoutMs = maxTransactionTimeoutMs + LATE_TRANSACTION_BUFFER_MS;
    private final MockTime time = new MockTime();
    
    @BeforeEach
    public void setUp() throws IOException {
        logDir = TestUtils.tempDirectory();
        stateManager = new ProducerStateManager(partition, logDir, maxTransactionTimeoutMs,
                producerStateManagerConfig, time);
    }

    @AfterEach
    public void tearDown() throws IOException {
        Utils.delete(logDir);
    }

    @Test
    public void testBasicIdMapping() {
        short epoch = 0;
        // First entry for id 0 added
        append(stateManager, producerId, epoch, 0, 0L, 0L);
        // Second entry for id 0 added
        append(stateManager, producerId, epoch, 1, 0L, 1L);

        // Duplicates are checked separately and should result in OutOfOrderSequence if appended
        assertThrows(OutOfOrderSequenceException.class, 
                () -> append(stateManager, producerId, epoch, 1, 0L, 1L));
        // Invalid sequence number (greater than next expected sequence number)
        assertThrows(OutOfOrderSequenceException.class, 
                () -> append(stateManager, producerId, epoch, 5, 0L, 2L));

        // Change epoch
        append(stateManager, producerId, (short) (epoch + 1), 0, 0L, 3L);
        // Incorrect epoch
        assertThrows(InvalidProducerEpochException.class, () -> append(stateManager, producerId, epoch, 0, 0L, 4L));
    }

    private void append(ProducerStateManager stateManager, long producerId, short producerEpoch, int seq, long offset, long l) {
        ProducerAppendInfo producerAppendInfo = stateManager.prepareUpdate(producerId, AppendOrigin.CLIENT);
        producerAppendInfo.appendDataBatch(producerEpoch, seq, seq, time.milliseconds(), 
                new LogOffsetMetadata(offset), offset, false);
        stateManager.update(producerAppendInfo);
        stateManager.updateMapEndOffset(offset + 1);
    }
}
