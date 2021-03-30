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
package org.apache.kafka.connect.runtime.errors;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProcessingContextTest {

    @Test
    public void testReportWithSingleReporter() {
        testReport(1);
    }

    @Test
    public void testReportWithMultipleReporters() {
        testReport(2);
    }

    private void testReport(int numberOfReports) {
        ProcessingContext context = new ProcessingContext();
        List<CompletableFuture<RecordMetadata>> fs = IntStream.range(0, numberOfReports).mapToObj(i -> new CompletableFuture<RecordMetadata>()).collect(Collectors.toList());
        context.reporters(IntStream.range(0, numberOfReports).mapToObj(i -> (ErrorReporter) c -> fs.get(i)).collect(Collectors.toList()));
        Future<Void> result = context.report();
        fs.forEach(f -> {
            assertFalse(result.isDone());
            f.complete(new RecordMetadata(new TopicPartition("t", 0), 0, 0, 0, 0L, 0, 0));
        });
        assertTrue(result.isDone());
    }
}
