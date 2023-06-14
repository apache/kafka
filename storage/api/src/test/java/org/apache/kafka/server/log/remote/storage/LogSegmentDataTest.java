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
package org.apache.kafka.server.log.remote.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class LogSegmentDataTest {

    @Test
    public void testOptionalTransactionIndex(@TempDir File tempDir) {
        LogSegmentData logSegmentDataWithTransactionIndex = new LogSegmentData(
                new File(tempDir, "log-segment").toPath(),
                new File(tempDir, "offset-index").toPath(),
                new File(tempDir, "time-index").toPath(),
                Optional.of(new File(tempDir, "transaction-index").toPath()),
                new File(tempDir, "producer-snapshot").toPath(),
                ByteBuffer.allocate(1)
        );
        Assertions.assertTrue(logSegmentDataWithTransactionIndex.transactionIndex().isPresent());

        LogSegmentData logSegmentDataWithNoTransactionIndex = new LogSegmentData(
                new File(tempDir, "log-segment").toPath(),
                new File(tempDir, "offset-index").toPath(),
                new File(tempDir, "time-index").toPath(),
                Optional.empty(),
                new File(tempDir, "producer-snapshot").toPath(),
                ByteBuffer.allocate(1)
        );
        assertFalse(logSegmentDataWithNoTransactionIndex.transactionIndex().isPresent());
    }
}