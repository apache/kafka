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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CoordinatorBackgroundThreadPoolExecutorTest {

    @Test
    public void testMetrics() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntimeMetrics metrics = mock(CoordinatorRuntimeMetrics.class);
        Time mockTime = new MockTime();
        CoordinatorBackgroundThreadPoolExecutor threadPoolExecutor = new CoordinatorBackgroundThreadPoolExecutor(
            "threaad-pool-",
            2,
            mockTime,
            metrics
        );

        try {
            CountDownLatch task1Started = new CountDownLatch(1);
            CountDownLatch task2Started = new CountDownLatch(1);
            CountDownLatch task3Started = new CountDownLatch(1);
            CountDownLatch task1Unblocked = new CountDownLatch(1);
            CountDownLatch task2Unblocked = new CountDownLatch(1);
            CountDownLatch task3Unblocked = new CountDownLatch(1);

            Future<?> task1 = threadPoolExecutor.submit(() -> {
                task1Started.countDown();
                try {
                    task1Unblocked.await();
                } catch (InterruptedException e) { }
            });
            Future<?> task2 = threadPoolExecutor.submit(() -> {
                task2Started.countDown();
                try {
                    task2Unblocked.await();
                } catch (InterruptedException e) { }
            });
            Future<?> task3 = threadPoolExecutor.submit(() -> {
                task3Started.countDown();
                try {
                    task3Unblocked.await();
                } catch (InterruptedException e) { }
            });

            // Task 1 and task 2 start.
            task1Started.await();
            task2Started.await();

            // Task 1 takes 100 ms.
            mockTime.sleep(100);
            task1Unblocked.countDown();
            task1.get(5, TimeUnit.SECONDS);

            // Task 3 starts.
            task3Started.await();

            // Task 2 takes 500 ms.
            mockTime.sleep(400);
            task2Unblocked.countDown();
            task2.get(5, TimeUnit.SECONDS);

            // Task 3 takes 500 ms.
            mockTime.sleep(100);
            task3Unblocked.countDown();
            task3.get(5, TimeUnit.SECONDS);

            verify(metrics, times(2)).recordBackgroundQueueTime(0);
            verify(metrics, times(1)).recordBackgroundQueueTime(100);
            verify(metrics, times(1)).recordBackgroundProcessingTime(100);
            verify(metrics, times(2)).recordBackgroundProcessingTime(500);
            verify(metrics, times(1)).recordBackgroundThreadBusyTime(50.0);
            verify(metrics, times(2)).recordBackgroundThreadBusyTime(250.0);
        } finally {
            threadPoolExecutor.shutdown();
            threadPoolExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
