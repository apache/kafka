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
package org.apache.kafka.connect.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class OffsetStorageReaderTest {

    @Mock
    private Converter taskKeyConverter;

    @Mock
    private Converter taskValueConverter;

    @Mock
    private OffsetBackingStore offsetBackingStore;

    @Test
    @Timeout(60)
    public void testClosingOffsetReaderWhenOffsetStoreHangs() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        OffsetStorageReaderImpl offsetStorageReaderImpl = new OffsetStorageReaderImpl(
                offsetBackingStore, "namespace", taskKeyConverter, taskValueConverter);

        doAnswer(invocation -> {
            // Sleep for a long time to simulate a hanging offset store
            Thread.sleep(9999 * 1000);
            throw new RuntimeException("Should never get here");
        }).when(offsetBackingStore).get(any());

        // Connector task thread hanging
        executor.submit(() -> {
            // Does call offsetBackingStore.get() and hangs
            offsetStorageReaderImpl.offsets(Collections.emptyList());
        });
        Thread.sleep(3000);

        verify(offsetBackingStore, times(1)).get(any());

        // The herder thread should not block when trying to close `offsetStorageReaderImpl`
        // and complete before test timeout
        offsetStorageReaderImpl.close();
    }

    @Test
    @Timeout(60)
    public void testClosingOffsetReaderWhenOffsetStoreHangsAndHasIncompleteFutures() throws Exception {
        // Test similar to `testClosingOffsetReaderWhenOffsetStoreHangs` above, but in this case
        // `OffsetStorageReaderImpl.offsetReadFutures` contains a future when `offsetStorageReaderImpl.close()` is called.

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CompletableFuture<?> hangingFuture = mock(CompletableFuture.class);

        OffsetStorageReaderImpl offsetStorageReaderImpl = new OffsetStorageReaderImpl(
                offsetBackingStore, "namespace", taskKeyConverter, taskValueConverter);

        // Mock hanging future
        doAnswer(invocation -> {
            Thread.sleep(9999 * 1000);
            throw new RuntimeException("Should never get here");
        }).when(hangingFuture).get();

        // Mock `offsetBackingStore.get()`
        doAnswer(new Answer<Object>() {
                int callCount = 0;

                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    if (callCount == 0) {
                        callCount += 1;
                        // First connector task
                        return hangingFuture;
                    } else {
                        // Second connector task
                        Thread.sleep(9999 * 1000);
                        throw new RuntimeException("Should never get here");
                    }
                }
            }
        ).when(offsetBackingStore).get(any());


        // Connector task thread calls `offsets()` --> hangs on `hangingFuture.get()`
        // --> the future is added to `offsetStorageReaderImpl.offsetReadFutures` and never removed
        executor.submit(() -> {
            offsetStorageReaderImpl.offsets(Collections.emptyList());
        });
        Thread.sleep(3000);

        verify(offsetBackingStore, times(1)).get(any());
        verify(hangingFuture, times(1)).get();

        // Another connector task thread calls `offsets()` --> hangs on offsetBackingStore.get()
        // --> the future is never added to `offsetStorageReaderImpl.offsetReadFutures`
        executor.submit(() -> {
            offsetStorageReaderImpl.offsets(Collections.emptyList());
        });
        Thread.sleep(3000);

        verify(offsetBackingStore, times(2)).get(any());

        // The herder thread should not block when trying to close `offsetStorageReaderImpl` and should complete
        // before the test timeout
        offsetStorageReaderImpl.close();

        // The hanging future should be cancelled by `close()`
        verify(hangingFuture, times(1)).cancel(true);
    }
}
