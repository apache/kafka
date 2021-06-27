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
package org.apache.kafka.common.header.internals;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecordHeaderConcurrentTest {

    private static final int ITERATIONS = 100_000;

    private final byte[] keyBytes = "key".getBytes(StandardCharsets.UTF_8);
    private final byte[] valueBytes = "value".getBytes(StandardCharsets.UTF_8);
    private final ByteBuffer keyBuffer = ByteBuffer.wrap(keyBytes);
    private final ByteBuffer valueBuffer = ByteBuffer.wrap(valueBytes);

    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @AfterAll
    public static void shutdownExecutor() {
        executorService.shutdown();
    }

    @Test
    public void testConcurrentKeyInit() throws ExecutionException, InterruptedException {
        for (int i = 0; i < ITERATIONS; i++) {
            RecordHeader header = new RecordHeader(keyBuffer, valueBuffer);
            Future<String> future = executorService.submit(header::key);
            assertEquals("key", header.key());
            assertEquals("key", future.get());
        }
    }

    @Test
    public void testConcurrentValueInit() throws ExecutionException, InterruptedException {
        for (int i = 0; i < ITERATIONS; i++) {
            RecordHeader header = new RecordHeader(keyBuffer, valueBuffer);
            Future<byte[]> future = executorService.submit(header::value);
            assertArrayEquals(valueBytes, header.value());
            assertArrayEquals(valueBytes, future.get());
        }
    }

}
