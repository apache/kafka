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

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ClassLoaderAwareRemoteStorageManagerTest {
    @Test
    public void testWithClassLoader() {
        final DummyClassLoader dummyClassLoader = new DummyClassLoader();
        final RemoteStorageManager delegate = mock(RemoteStorageManager.class);
        final ClassLoaderAwareRemoteStorageManager rsm = new ClassLoaderAwareRemoteStorageManager(delegate, dummyClassLoader);
        doAnswer(invocation -> {
            assertEquals(dummyClassLoader, Thread.currentThread().getContextClassLoader());
            return null;
        }).when(delegate).configure(any());

        assertNotEquals(dummyClassLoader, Thread.currentThread().getContextClassLoader());
        rsm.configure(Collections.emptyMap());
        assertNotEquals(dummyClassLoader, Thread.currentThread().getContextClassLoader());
    }

    private static class DummyClassLoader extends ClassLoader { }
}
