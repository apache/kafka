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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.InternalSinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class WorkerErrantRecordReporterTest {

    private WorkerErrantRecordReporter reporter;

    @Mock private Converter converter;
    @Mock private HeaderConverter headerConverter;
    @Mock private InternalSinkRecord record;
    @Mock private ErrorHandlingMetrics errorHandlingMetrics;
    @Mock private ErrorReporter errorReporter;

    @Test
    public void testGetFutures() {
        initializeReporter(true);
        Collection<TopicPartition> topicPartitions = new ArrayList<>();
        assertTrue(reporter.futures.isEmpty());
        for (int i = 0; i < 4; i++) {
            TopicPartition topicPartition = new TopicPartition("topic", i);
            topicPartitions.add(topicPartition);
            reporter.futures.put(topicPartition, Collections.singletonList(CompletableFuture.completedFuture(null)));
        }
        assertFalse(reporter.futures.isEmpty());
        reporter.awaitFutures(topicPartitions);
        assertTrue(reporter.futures.isEmpty());
    }

    @Test
    public void testReportErrorsTolerated() {
        testReport(true);
    }

    @Test
    public void testReportNoToleratedErrors() {
        testReport(false);
    }

    private void testReport(boolean errorsTolerated) {
        initializeReporter(errorsTolerated);
        when(errorReporter.report(any())).thenReturn(CompletableFuture.completedFuture(null));
        @SuppressWarnings("unchecked") ConsumerRecord<byte[], byte[]> consumerRecord = mock(ConsumerRecord.class);
        when(record.originalRecord()).thenReturn(consumerRecord);

        if (errorsTolerated) {
            reporter.report(record, new Throwable());
        } else {
            assertThrows(ConnectException.class, () -> reporter.report(record, new Throwable()));
        }

        verify(errorReporter).report(any());
    }

    private void initializeReporter(boolean errorsTolerated) {
        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(
                5000,
                ConnectorConfig.ERRORS_RETRY_MAX_DELAY_DEFAULT,
                errorsTolerated ? ToleranceType.ALL : ToleranceType.NONE,
                Time.SYSTEM,
                errorHandlingMetrics
        );
        retryWithToleranceOperator.reporters(Collections.singletonList(errorReporter));
        reporter = new WorkerErrantRecordReporter(
                retryWithToleranceOperator,
                converter,
                converter,
                headerConverter
        );
    }
}
