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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClassLoaderAwareRemoteLogMetadataManagerTest {

    @Test
    public void testWithClassLoader() throws RemoteStorageException {
        DummyClassLoader dummyClassLoader = new DummyClassLoader();
        RemoteLogMetadataManager delegate = mock(RemoteLogMetadataManager.class);
        ClassLoaderAwareRemoteLogMetadataManager rlmm = new ClassLoaderAwareRemoteLogMetadataManager(delegate, dummyClassLoader);
        when(delegate.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenAnswer(metadata -> {
            assertEquals(dummyClassLoader, Thread.currentThread().getContextClassLoader());
            return CompletableFuture.completedFuture(null);
        });
        assertNotEquals(dummyClassLoader, Thread.currentThread().getContextClassLoader());
        rlmm.addRemoteLogSegmentMetadata(mock(RemoteLogSegmentMetadata.class));
        assertNotEquals(dummyClassLoader, Thread.currentThread().getContextClassLoader());
    }

    private static class DummyClassLoader extends ClassLoader {
    }
}