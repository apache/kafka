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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FutureRecordMetadataTest {

    private final MockTime time = new MockTime();

    @Test
    public void testFutureGetWithSeconds() throws ExecutionException, InterruptedException, TimeoutException {
        ProduceRequestResult produceRequestResult = mockProduceRequestResult();
        FutureRecordMetadata future = futureRecordMetadata(produceRequestResult);

        ProduceRequestResult chainedProduceRequestResult = mockProduceRequestResult();
        future.chain(futureRecordMetadata(chainedProduceRequestResult));

        future.get(1L, TimeUnit.SECONDS);

        verify(produceRequestResult).await(1L, TimeUnit.SECONDS);
        verify(chainedProduceRequestResult).await(1000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testFutureGetWithMilliSeconds() throws ExecutionException, InterruptedException, TimeoutException {
        ProduceRequestResult produceRequestResult = mockProduceRequestResult();
        FutureRecordMetadata future = futureRecordMetadata(produceRequestResult);

        ProduceRequestResult chainedProduceRequestResult = mockProduceRequestResult();
        future.chain(futureRecordMetadata(chainedProduceRequestResult));

        future.get(1000L, TimeUnit.MILLISECONDS);

        verify(produceRequestResult).await(1000L, TimeUnit.MILLISECONDS);
        verify(chainedProduceRequestResult).await(1000L, TimeUnit.MILLISECONDS);
    }

    private FutureRecordMetadata futureRecordMetadata(ProduceRequestResult produceRequestResult) {
        return new FutureRecordMetadata(
                produceRequestResult,
                0,
                RecordBatch.NO_TIMESTAMP,
                0,
                0,
                time
        );
    }

    private ProduceRequestResult mockProduceRequestResult() throws InterruptedException {
        ProduceRequestResult mockProduceRequestResult = mock(ProduceRequestResult.class);
        when(mockProduceRequestResult.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
        return mockProduceRequestResult;
    }
}
