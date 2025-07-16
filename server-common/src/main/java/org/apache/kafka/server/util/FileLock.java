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
package org.apache.kafka.server.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;

/**
 * A file lock a la flock/funlock
 *
 * The given path will be created and opened if it doesn't exist.
 */
public class FileLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileLock.class);

    private final File file;
    private final FileChannel channel;
    private java.nio.channels.FileLock flock;

    public FileLock(File file) throws IOException {
        this.file = file;
        this.channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
    }

    public File file() {
        return file;
    }

    /**
     * Lock the file or throw an exception if the lock is already held
     */
    public synchronized void lock() throws IOException {
        LOGGER.trace("Acquiring lock on {}", file.getAbsolutePath());
        flock = channel.lock();
    }

    /**
     * Try to lock the file and return true if the locking succeeds
     */
    public synchronized boolean tryLock() throws IOException {
        LOGGER.trace("Acquiring lock on {}", file.getAbsolutePath());
        try {
            // weirdly this method will return null if the lock is held by another
            // process, but will throw an exception if the lock is held by this process
            // so we have to handle both cases
            flock = channel.tryLock();
            return flock != null;
        } catch (OverlappingFileLockException e) {
            return false;
        }
    }

    /**
     * Unlock the lock if it is held
     */
    public synchronized void unlock() throws IOException {
        LOGGER.trace("Releasing lock on {}", file.getAbsolutePath());
        if (flock != null) {
            flock.release();
        }
    }

    /**
     * Destroy this lock, closing the associated FileChannel
     */
    public synchronized void destroy() throws IOException {
        unlock();
        if (file.exists() && file.delete()) {
            LOGGER.trace("Deleted {}", file.getAbsolutePath());
        }
        channel.close();
    }

    /**
     * Unlock the file and close the associated FileChannel
     */
    public synchronized void unlockAndClose() throws IOException {
        unlock();
        channel.close();
    }
}
