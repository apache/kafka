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

import org.apache.kafka.server.log.internals.StorageAction;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * A wrapper class of {@link RemoteStorageManager} that sets the context class loader when calling the respective
 * methods.
 */
public class ClassLoaderAwareRemoteStorageManager implements RemoteStorageManager {

    private final RemoteStorageManager delegate;
    private final ClassLoader rsmClassLoader;

    public ClassLoaderAwareRemoteStorageManager(RemoteStorageManager rsm, ClassLoader rsmClassLoader) {
        this.delegate = rsm;
        this.rsmClassLoader = rsmClassLoader;
    }

    public RemoteStorageManager delegate() {
        return delegate;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        withClassLoader(() -> {
            delegate.configure(configs);
            return null;
        });
    }

    @Override
    public void close() throws IOException {
        withClassLoader(() -> {
            delegate.close();
            return null;
        });
    }

    private <T, E extends Exception> T withClassLoader(StorageAction<T, E> action) throws E {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(rsmClassLoader);
        try {
            return action.execute();
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    public void copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                   LogSegmentData logSegmentData) throws RemoteStorageException {
        withClassLoader(() -> {
            delegate.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData);
            return null;
        });
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int startPosition) throws RemoteStorageException {
        return withClassLoader(() -> delegate.fetchLogSegment(remoteLogSegmentMetadata, startPosition));
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int startPosition, int endPosition) throws RemoteStorageException {
        return withClassLoader(() -> delegate.fetchLogSegment(remoteLogSegmentMetadata, startPosition, endPosition));
    }

    @Override
    public InputStream fetchIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata, IndexType indexType) throws RemoteStorageException {
        return withClassLoader(() -> delegate.fetchIndex(remoteLogSegmentMetadata, indexType));
    }

    @Override
    public void deleteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        withClassLoader(() -> {
            delegate.deleteLogSegmentData(remoteLogSegmentMetadata);
            return null;
        });
    }

}
