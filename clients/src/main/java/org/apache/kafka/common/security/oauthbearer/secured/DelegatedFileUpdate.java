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

package org.apache.kafka.common.security.oauthbearer.secured;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.kafka.common.security.oauthbearer.secured.FileWatchService.FileWatchCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DelegatedFileUpdate<T> implements Initable, Closeable, FileWatchCallback {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final Path path;

    protected final boolean isPathDirectory;

    private final ReentrantReadWriteLock lock;

    private final FileWatchService fileWatchService;

    private T delegate;

    private boolean isInitialized = false;

    private boolean isClosed = false;

    protected DelegatedFileUpdate(Path path) {
        this.path = path.toAbsolutePath();
        this.isPathDirectory = this.path.toFile().isDirectory();
        this.fileWatchService = new FileWatchService(this.isPathDirectory ? this.path : this.path.getParent(), this);
        this.lock = new ReentrantReadWriteLock();
    }

    @Override
    public void init() throws IOException {
        try {
            log.debug("init started");

            lock.writeLock().lock();

            if (isInitialized) {
                log.warn("{} already initialized", getClass().getSimpleName());
            } else {
                refresh();
                fileWatchService.init();
            }
        } finally {
            isInitialized = true;

            lock.writeLock().unlock();

            log.debug("init completed");
        }
    }

    @Override
    public void close() throws IOException {
        try {
            log.debug("close started");

            lock.writeLock().lock();

            if (isClosed) {
                log.warn("{} already closed", getClass().getSimpleName());
            } else {
                try {
                    fileWatchService.close();
                } catch (Exception e) {
                    log.warn(String.format("%s encountered error during close", path), e);
                }
            }
        } finally {
            isClosed = true;

            lock.writeLock().unlock();

            log.debug("close completed");
        }
    }

    @Override
    public void fileCreated(Path path) throws IOException {
        if (isPathDirectory || (this.path.equals(path.toAbsolutePath()))) {
            log.debug("File created event triggered for {}", path);
            refresh();
        }
    }

    @Override
    public void fileDeleted(Path path) throws IOException {
        if (isPathDirectory) {
            log.debug("File deleted event triggered for {}", path);
            refresh();
        } else {
            log.warn("File deleted event triggered for {} ignored", path);
        }
    }

    @Override
    public void fileModified(Path path) throws IOException {
        if (isPathDirectory || (this.path.equals(path.toAbsolutePath()))) {
            log.debug("File modified event triggered for {}", path);
            refresh();
        }
    }

    protected abstract T createDelegate() throws IOException;

    protected T retrieveDelegate() {
        try {
            log.debug("Delegate retrieval started");

            lock.readLock().lock();

            if (isClosed)
                throw new IllegalStateException(String.format("Cannot use %s after close() method called", getClass().getSimpleName()));

            if (!isInitialized)
                throw new IllegalStateException(String.format("To use %s, first call init() method", getClass().getSimpleName()));

            return delegate;
        } finally {
            lock.readLock().unlock();

            log.debug("Delegate retrieval completed");
        }
    }

    protected void refreshDelegate(T delegate) {
        // Since we're updating our delegate, make sure to surround with the write lock!
        try {
            log.debug("Delegate refresh started");

            lock.writeLock().lock();

            this.delegate = delegate;
        } finally {
            lock.writeLock().unlock();

            log.info("Delegate refresh completed");
        }
    }

    protected final void refresh() throws IOException {
        refreshDelegate(createDelegate());
    }

}
